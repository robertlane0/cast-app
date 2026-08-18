// SPDX-License-Identifier: MIT OR Apache-2.0
//! In-process PipeWire client for Wayland screen capture
//! (`05-screen-capture.md` §3.4): consumes the xdg-desktop-portal stream fd
//! and copies raw frames into the capture→encoder queue.
//!
//! Stock FFmpeg has no PipeWire input device (the `pipewiregrab` demuxer
//! patches were never merged), so the portal's stream socket is driven
//! here with the official freedesktop `pipewire` crate: a capture stream is
//! created on the portal's PipeWire instance, format negotiation yields the
//! compositor's packed 4-byte pixel format (RGBx/BGRx/RGBA/BGRA), and each
//! process callback copies the mapped buffer into the same drop-oldest frame
//! queue the X11 capture thread uses. The encoder stdin then carries plain
//! rawvideo, so the `ffmpeg` child is byte-identical in shape to the X11
//! path except for the negotiated `-pix_fmt` (rgb0/bgr0/rgba/bgra).
//!
//! The loop is polled manually (`Loop::iterate` with a bounded timeout)
//! instead of `MainLoop::run`, so the thread owns every PipeWire object
//! (none of them are `Send`) and can be stopped from the controller via the
//! shared stop flag.

use std::io;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use pipewire as pw;
use pw::spa;
use spa::param::video::{VideoFormat, VideoInfoRaw};
use spa::pod::Pod;

use crate::util::backpressure::BoundedDropOldest;

/// Poll interval of the capture loop (ms): bounds teardown latency while
/// staying idle-friendly.
pub const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Maximum stream dimension accepted in negotiation (defensive cap; portal
/// streams are monitor-sized).
const MAX_STREAM_DIMENSION: u32 = 16_384;

/// The negotiated pixel format mapped to the ffmpeg rawvideo `-pix_fmt`
/// name. All variants are 4 bytes per pixel, so the encoder's `-s WxH`
/// math and the frame byte counts are identical to the X11 RGBA path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PixFmt {
    Rgb0,
    Bgr0,
    Rgba,
    Bgra,
}

impl PixFmt {
    /// The ffmpeg `-pix_fmt` argument for this format.
    pub fn ffmpeg_name(self) -> &'static str {
        match self {
            PixFmt::Rgb0 => "rgb0",
            PixFmt::Bgr0 => "bgr0",
            PixFmt::Rgba => "rgba",
            PixFmt::Bgra => "bgra",
        }
    }

    /// Map a negotiated SPA video format; `None` for formats the encoder
    /// path cannot consume (planar/compressed formats are never advertised
    /// by this client).
    fn from_video_format(format: VideoFormat) -> Option<Self> {
        match format {
            VideoFormat::RGBx => Some(PixFmt::Rgb0),
            VideoFormat::BGRx => Some(PixFmt::Bgr0),
            VideoFormat::RGBA => Some(PixFmt::Rgba),
            VideoFormat::BGRA => Some(PixFmt::Bgra),
            _ => None,
        }
    }
}

/// The negotiated stream format, delivered to the controller so it can
/// spawn the encoder with the right `-s` and `-pix_fmt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PwFormat {
    pub width: u32,
    pub height: u32,
    pub pix_fmt: PixFmt,
}

impl PwFormat {
    /// Bytes per frame for the negotiated format (4 bytes per pixel).
    pub fn frame_bytes(&self) -> usize {
        (self.width * self.height * 4) as usize
    }
}

/// The capture-thread spawner, extracted so bridge tests can inject a fake
/// (no PipeWire): it must spawn a thread that copies frames from the portal
/// fd into `frames`, report the negotiated format through `status` exactly
/// once (and later failures as `Err`), and stop on `stop`.
pub type PipewireSpawner = dyn Fn(
        OwnedFd,
        u32,
        Arc<BoundedDropOldest<Vec<u8>>>,
        std::sync::mpsc::Sender<Result<PwFormat, String>>,
        Arc<AtomicBool>,
    ) -> io::Result<JoinHandle<()>>
    + Send
    + Sync;

/// Spawn the PipeWire capture thread for a portal stream fd.
///
/// `node_id` is the PipeWire node id the portal granted for this stream
/// (`Start()`'s `streams` entry); the capture stream must target it
/// explicitly; the portal's restricted PipeWire remote otherwise leaves
/// `pw_stream_connect` to autoconnect to whatever default video node it can
/// see, which is not guaranteed to be the granted node and fails
/// negotiation immediately after the stream reports itself connected.
///
/// The thread copies frames into `frames` (drop-oldest backpressure) and
/// reports the negotiated format exactly once through `status`; a failure at
/// any point after that is delivered as `Err` on the same channel so the
/// controller can tear the pipeline down.
pub fn spawn_pipewire_capture(
    fd: OwnedFd,
    node_id: u32,
    frames: Arc<BoundedDropOldest<Vec<u8>>>,
    status: std::sync::mpsc::Sender<Result<PwFormat, String>>,
    stop: Arc<AtomicBool>,
) -> io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("pipewire-capture".to_string())
        .spawn(move || {
            if let Err(error) = run_pipewire_capture(fd, node_id, frames, status, stop) {
                tracing::error!(%error, "pipewire capture failed");
            }
        })
}

/// The capture loop body (thread entry): owns every PipeWire object, so no
/// handle ever crosses a thread boundary. Runs until the stop flag or a
/// stream failure; the thread's only exit is this function returning.
fn run_pipewire_capture(
    fd: OwnedFd,
    node_id: u32,
    frames: Arc<BoundedDropOldest<Vec<u8>>>,
    status: std::sync::mpsc::Sender<Result<PwFormat, String>>,
    stop: Arc<AtomicBool>,
) -> Result<(), String> {
    pw::init();
    let failed = Arc::new(AtomicBool::new(false));
    let stream_failed = Arc::clone(&failed);

    let loop_ = pw::loop_::LoopRc::new(None).map_err(|error| error.to_string())?;
    let context = pw::context::ContextRc::new(&loop_, None).map_err(|error| error.to_string())?;
    let core = context
        .connect_fd(fd, None)
        .map_err(|error| error.to_string())?;

    let data = CaptureData {
        format: VideoInfoRaw::default(),
        format_sent: false,
        status: status.clone(),
        frames: Arc::clone(&frames),
    };

    let stream = pw::stream::StreamBox::new(
        &core,
        "cast-app-capture",
        pw::properties::properties! {
            *pw::keys::MEDIA_TYPE => "Video",
            *pw::keys::MEDIA_CATEGORY => "Capture",
            *pw::keys::MEDIA_ROLE => "Screen",
        },
    )
    .map_err(|error| error.to_string())?;

    let _listener = stream
        .add_local_listener_with_user_data(data)
        .state_changed(move |_, _, old, new| {
            tracing::debug!("pipewire stream state: {old:?} -> {new:?}");
            if matches!(new, pw::stream::StreamState::Error(_)) {
                stream_failed.store(true, Ordering::Relaxed);
            }
        })
        .param_changed(|_, data, id, param| {
            let Some(param) = param else {
                return;
            };
            if id != pw::spa::param::ParamType::Format.as_raw() {
                return;
            }
            let Ok((media_type, media_subtype)) = pw::spa::param::format_utils::parse_format(param)
            else {
                return;
            };
            if media_type != pw::spa::param::format::MediaType::Video
                || media_subtype != pw::spa::param::format::MediaSubtype::Raw
            {
                return;
            }
            let parse = data.format.parse(param);
            if parse.is_err() {
                tracing::warn!("failed to parse the negotiated video format");
                return;
            }
            tracing::debug!(
                format = ?data.format.format(),
                size = ?data.format.size(),
                framerate = ?data.format.framerate(),
                "pipewire stream format negotiated"
            );
            if !data.format_sent {
                data.format_sent = true;
                let format = data.format.format();
                let size = data.format.size();
                let result = (|| {
                    if size.width == 0
                        || size.height == 0
                        || size.width > MAX_STREAM_DIMENSION
                        || size.height > MAX_STREAM_DIMENSION
                    {
                        return Err(format!("unreasonable negotiated size {size:?}"));
                    }
                    let pix_fmt = PixFmt::from_video_format(format)
                        .ok_or_else(|| format!("unsupported negotiated pixel format {format:?}"))?;
                    Ok(PwFormat {
                        width: size.width,
                        height: size.height,
                        pix_fmt,
                    })
                })();
                let _ = data.status.send(result);
            }
        })
        .process(|stream, data| {
            let Some(mut buffer) = stream.dequeue_buffer() else {
                return;
            };
            let datas = buffer.datas_mut();
            let Some(buf) = datas.first_mut() else {
                return;
            };
            let (offset, size) = {
                let chunk = buf.chunk();
                (chunk.offset() as usize, chunk.size() as usize)
            };
            let Some(payload) = buf.data() else {
                return;
            };
            let Some(format) = data.format_sent.then(|| data.format.size()) else {
                return;
            };
            let expected = (format.width as usize)
                .checked_mul(format.height as usize)
                .and_then(|pixels| pixels.checked_mul(4));
            let Some(expected) = expected else {
                return;
            };
            let start = offset.min(payload.len());
            let end = start.saturating_add(size).min(payload.len());
            if end.saturating_sub(start) != expected {
                tracing::debug!(
                    start,
                    size,
                    expected,
                    "frame payload size mismatch; skipping"
                );
                return;
            }
            data.frames.push(payload[start..end].to_vec());
        })
        .register()
        .map_err(|error| error.to_string())?;

    let pod_bytes = capture_format_pod()?;
    let pod = Pod::from_bytes(&pod_bytes)
        .ok_or_else(|| "failed to build the capture format pod".to_string())?;
    let mut params: [&Pod; 1] = [pod];
    stream
        .connect(
            spa::utils::Direction::Input,
            Some(node_id),
            pw::stream::StreamFlags::AUTOCONNECT | pw::stream::StreamFlags::MAP_BUFFERS,
            &mut params,
        )
        .map_err(|error| error.to_string())?;

    tracing::info!("pipewire capture connected; streaming");
    while !stop.load(Ordering::Relaxed) && !failed.load(Ordering::Relaxed) {
        // `iterate` enters/leaves the loop internally (0.9.x exposes no
        // safe enter/leave pair and none is needed here).
        loop_.iterate(POLL_INTERVAL);
    }

    let aborted = stop.load(Ordering::Relaxed);
    if !aborted {
        // A stream failure: the controller must hear about it even after
        // the format was negotiated, so the pipeline stops instead of
        // streaming stale frames.
        let _ = status.send(Err("the pipewire stream failed".to_string()));
        return Err("pipewire stream failed".to_string());
    }
    tracing::info!("pipewire capture stopped");
    Ok(())
}

/// User data shared with the stream callbacks.
struct CaptureData {
    format: VideoInfoRaw,
    format_sent: bool,
    status: std::sync::mpsc::Sender<Result<PwFormat, String>>,
    frames: Arc<BoundedDropOldest<Vec<u8>>>,
}

/// The capture stream's format negotiation pod: packed 4-byte RGB formats
/// only (the encoder path is rawvideo), size up to `MAX_STREAM_DIMENSION`,
/// framerate 0–1000 with a 30 fps default.
fn capture_format_pod() -> Result<Vec<u8>, String> {
    let object = pw::spa::pod::object!(
        pw::spa::utils::SpaTypes::ObjectParamFormat,
        pw::spa::param::ParamType::EnumFormat,
        pw::spa::pod::property!(
            pw::spa::param::format::FormatProperties::MediaType,
            Id,
            pw::spa::param::format::MediaType::Video
        ),
        pw::spa::pod::property!(
            pw::spa::param::format::FormatProperties::MediaSubtype,
            Id,
            pw::spa::param::format::MediaSubtype::Raw
        ),
        pw::spa::pod::property!(
            pw::spa::param::format::FormatProperties::VideoFormat,
            Choice,
            Enum,
            Id,
            pw::spa::param::video::VideoFormat::BGRx,
            pw::spa::param::video::VideoFormat::BGRx,
            pw::spa::param::video::VideoFormat::RGBx,
            pw::spa::param::video::VideoFormat::BGRA,
            pw::spa::param::video::VideoFormat::RGBA,
        ),
        pw::spa::pod::property!(
            pw::spa::param::format::FormatProperties::VideoSize,
            Choice,
            Range,
            Rectangle,
            pw::spa::utils::Rectangle {
                width: 1,
                height: 1
            },
            pw::spa::utils::Rectangle {
                width: 1,
                height: 1
            },
            pw::spa::utils::Rectangle {
                width: MAX_STREAM_DIMENSION,
                height: MAX_STREAM_DIMENSION,
            }
        ),
        pw::spa::pod::property!(
            pw::spa::param::format::FormatProperties::VideoFramerate,
            Choice,
            Range,
            Fraction,
            pw::spa::utils::Fraction { num: 30, denom: 1 },
            pw::spa::utils::Fraction { num: 0, denom: 1 },
            pw::spa::utils::Fraction {
                num: 1000,
                denom: 1
            }
        ),
    );
    let values = pw::spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &pw::spa::pod::Value::Object(object),
    )
    .map_err(|error| error.to_string())?
    .0
    .into_inner();
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pix_fmt_names_match_ffmpeg() {
        assert_eq!(PixFmt::Rgb0.ffmpeg_name(), "rgb0");
        assert_eq!(PixFmt::Bgr0.ffmpeg_name(), "bgr0");
        assert_eq!(PixFmt::Rgba.ffmpeg_name(), "rgba");
        assert_eq!(PixFmt::Bgra.ffmpeg_name(), "bgra");
    }

    #[test]
    fn video_format_mapping_covers_all_advertised_formats() {
        for format in [
            VideoFormat::RGBx,
            VideoFormat::BGRx,
            VideoFormat::RGBA,
            VideoFormat::BGRA,
        ] {
            assert!(PixFmt::from_video_format(format).is_some(), "{format:?}");
        }
        assert!(PixFmt::from_video_format(VideoFormat::I420).is_none());
        assert!(PixFmt::from_video_format(VideoFormat::YUY2).is_none());
    }

    #[test]
    fn frame_bytes_is_four_bytes_per_pixel() {
        let format = PwFormat {
            width: 640,
            height: 480,
            pix_fmt: PixFmt::Bgr0,
        };
        assert_eq!(format.frame_bytes(), 640 * 480 * 4);
    }

    #[test]
    fn negotiation_pod_is_serializable() {
        let pod = capture_format_pod().expect("pod serialization");
        assert!(!pod.is_empty());
    }
}
