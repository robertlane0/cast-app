// SPDX-License-Identifier: MIT OR Apache-2.0
//! Capture→ffmpeg stdin and ffmpeg stdout→HTTP channel bridges with
//! drop-oldest backpressure (`05-screen-capture.md` §5–§6).
//!
//! Threads:
//! - **capture** (spawned by `capture::start_capture` via [`capture_link`])
//!   pushes raw RGBA frames into the cap-2 frame queue;
//! - **controller** ([`controller`]) owns the `Ffmpeg` child: writes frames
//!   to its stdin, restarts it on resolution change, runs the EOF → wait →
//!   kill teardown, and reports unexpected exits;
//! - **stdout reader** ([`stdout_reader`], one per encoder generation)
//!   segments the encoded output at ISO-BMFF fragment boundaries
//!   (`screen::segments`) and pushes whole segments into the cap-8 output
//!   queue;
//! - **forwarder** ([`forwarder`]) moves output-queue segments into the
//!   media server's live stream channel, and tears the session down when the
//!   consumer closes.
//!
//! Backpressure is media-aware: the drop unit is one complete encoded
//! segment (a `moof`+`mdat` fragment, or a whole read of non-box-structured
//! output), never an arbitrary byte slice. A slow consumer therefore
//! receives a stream that skips whole fragments — a transient glitch — and
//! never truncated MP4 boxes, which would break decoding until the stream
//! is restarted. The init segment (`ftyp`+`moov`) is protected from
//! eviction everywhere, and an encoder restart clears the old generation's
//! buffered bytes so a fresh, valid init always leads the new stream.
//!
//! The implementation is split across sub-modules along those thread
//! boundaries, following the `cast::connection` precedent: [`capture_link`]
//! (capture → encoder), [`controller`] (encoder lifecycle/restart),
//! [`stdout_reader`] (segmentation) and [`forwarder`] (output → HTTP). This
//! module exposes the [`ScreenBridge`] facade.

mod capture_link;
mod controller;
mod forwarder;
mod stdout_reader;

pub use capture_link::{FRAME_QUEUE_CAPACITY, FrameFeeder};
pub use stdout_reader::OUTPUT_QUEUE_CAPACITY;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::media::server::MediaServer;
use crate::screen::segments::EncodedSegment;
use crate::state::BackendEvent;
use crate::util::backpressure::BoundedDropOldest;
use crate::util::shutdown::Shutdown;

/// Idle poll interval of the controller and forwarder threads.
const IDLE_POLL: Duration = Duration::from_millis(2);

/// How the pipeline gets its raw frames (`05-screen-capture.md` §3, §3.4).
pub enum PipelineInput {
    /// xcap monitor capture (X11/macOS/Windows): the bridge spawns the
    /// capture thread and sizes the first encoder from the monitor's current
    /// resolution.
    Frames {
        initial_resolution: (u32, u32),
        /// Spawn the xcap capture thread (real captures) or feed frames
        /// externally via [`ScreenBridge::push_frame`] (test harness; the
        /// thread is pointless there and must not hit the Wayland guard).
        capture_thread: bool,
    },
    /// Wayland portal capture (Linux only): the controller runs the portal
    /// dance on its own thread, spawns the PipeWire capture thread on the
    /// portal's stream fd, and sizes the first encoder from the negotiated
    /// format. `portal`/`capture` are injectable fakes for tests; `None`
    /// means the real implementations.
    #[cfg(target_os = "linux")]
    Portal {
        portal: Option<Box<dyn crate::screen::portal::ScreenCast>>,
        capture: Option<Arc<crate::screen::pipewire::PipewireSpawner>>,
    },
}

/// Handle to a running screen pipeline.
pub struct ScreenBridge {
    monitor_name: String,
    frames: Arc<BoundedDropOldest<Vec<u8>>>,
    output: Arc<BoundedDropOldest<EncodedSegment>>,
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    resolution_request: Arc<Mutex<Option<(u32, u32)>>>,
    reader_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    last_error: Arc<Mutex<Option<String>>>,
    dropped_segments: Arc<AtomicUsize>,
    threads: Vec<Option<JoinHandle<()>>>,
}

impl ScreenBridge {
    /// Spawn the full screen pipeline for `monitor_name`
    /// (`05-screen-capture.md` §5): capture thread, controller, encoder,
    /// stdout reader and forwarder, attached to `server`.
    ///
    /// On a Wayland session this routes to the portal pipeline
    /// ([`ScreenBridge::start_portal`], `05-screen-capture.md` §3.4), which
    /// serves the virtual [`WAYLAND_SCREEN_ENTRY`] monitor. Fails with an
    /// explanatory error if `ffmpeg` is missing (the Display source is
    /// disabled in that case) or the monitor cannot be resolved.
    pub fn start(
        monitor_name: String,
        server: MediaServer,
        events: mpsc::UnboundedSender<BackendEvent>,
        shutdown: Shutdown,
    ) -> Result<ScreenBridge, String> {
        if !crate::screen::ffmpeg_discover::ffmpeg_available() {
            return Err(
                "ffmpeg was not found on PATH; the Display source is unavailable".to_string(),
            );
        }
        #[cfg(target_os = "linux")]
        if crate::screen::capture::is_wayland_session() {
            return Self::start_portal(monitor_name, server, events, shutdown, None, None, None);
        }
        let initial_resolution = crate::screen::capture::monitor_resolution(&monitor_name)?;
        Self::start_pipeline(
            monitor_name,
            server,
            events,
            shutdown,
            PipelineInput::Frames {
                initial_resolution,
                capture_thread: true,
            },
            None,
        )
    }

    /// Spawn the Wayland portal pipeline for the virtual [`WAYLAND_SCREEN_ENTRY`]
    /// monitor (`05-screen-capture.md` §3.4): the controller thread runs the
    /// portal dance, spawns the PipeWire capture thread on the portal's
    /// stream fd, and sizes the encoder from the negotiated format.
    ///
    /// `portal`/`capture` inject fakes for tests; `None` uses the real
    /// portal client and PipeWire spawner. `custom_encoder` substitutes a
    /// fake program + args for `ffmpeg` (tests).
    #[cfg(target_os = "linux")]
    pub fn start_portal(
        monitor_name: String,
        server: MediaServer,
        events: mpsc::UnboundedSender<BackendEvent>,
        shutdown: Shutdown,
        portal: Option<Box<dyn crate::screen::portal::ScreenCast>>,
        capture: Option<Arc<crate::screen::pipewire::PipewireSpawner>>,
        custom_encoder: Option<(std::path::PathBuf, Vec<String>)>,
    ) -> Result<ScreenBridge, String> {
        if custom_encoder.is_none() && !crate::screen::ffmpeg_discover::ffmpeg_available() {
            return Err(
                "ffmpeg was not found on PATH; the Display source is unavailable".to_string(),
            );
        }
        Self::start_pipeline(
            monitor_name,
            server,
            events,
            shutdown,
            PipelineInput::Portal { portal, capture },
            custom_encoder,
        )
    }

    /// Spawn the encoder+HTTP half of the pipeline without a capture thread
    /// (test harness: frames arrive via [`ScreenBridge::push_frame`], and
    /// `custom_encoder` substitutes a fake program for `ffmpeg`).
    pub fn start_with_encoder(
        server: MediaServer,
        events: mpsc::UnboundedSender<BackendEvent>,
        shutdown: Shutdown,
        initial_resolution: (u32, u32),
        custom_encoder: Option<(std::path::PathBuf, Vec<String>)>,
    ) -> Result<ScreenBridge, String> {
        Self::start_pipeline(
            "test-monitor".to_string(),
            server,
            events,
            shutdown,
            PipelineInput::Frames {
                initial_resolution,
                capture_thread: false,
            },
            custom_encoder,
        )
    }

    fn start_pipeline(
        monitor_name: String,
        server: MediaServer,
        events: mpsc::UnboundedSender<BackendEvent>,
        shutdown: Shutdown,
        input: PipelineInput,
        custom_encoder: Option<(std::path::PathBuf, Vec<String>)>,
    ) -> Result<ScreenBridge, String> {
        let frames = Arc::new(BoundedDropOldest::new(FRAME_QUEUE_CAPACITY));
        let output = Arc::new(BoundedDropOldest::new(OUTPUT_QUEUE_CAPACITY));
        let stop = Arc::new(AtomicBool::new(false));
        let failed = Arc::new(AtomicBool::new(false));
        let last_error = Arc::new(Mutex::new(None));
        let resolution_request = Arc::new(Mutex::new(None));
        let reader_handles = Arc::new(Mutex::new(Vec::new()));
        let dropped_segments = Arc::new(AtomicUsize::new(0));

        // Capture thread (dedicated std::thread; spec §3) — only for the
        // xcap path; the portal pipeline's frames arrive from the PipeWire
        // capture thread spawned by the controller.
        let capture_thread = match &input {
            PipelineInput::Frames {
                capture_thread: true,
                ..
            } => Some(capture_link::spawn_capture_thread(
                capture_link::CaptureContext {
                    monitor_name: monitor_name.clone(),
                    frames: Arc::clone(&frames),
                    stop: Arc::clone(&stop),
                    resolution_request: Arc::clone(&resolution_request),
                    failed: Arc::clone(&failed),
                    last_error: Arc::clone(&last_error),
                    events: events.clone(),
                    shutdown: shutdown.clone(),
                },
            )?),
            PipelineInput::Frames {
                capture_thread: false,
                ..
            } => None,
            #[cfg(target_os = "linux")]
            PipelineInput::Portal { .. } => None,
        };

        // Controller thread: owns the encoder lifecycle (and, on Wayland,
        // the portal dance + PipeWire capture thread).
        let controller_frames = Arc::clone(&frames);
        let controller_output = Arc::clone(&output);
        let controller_stop = Arc::clone(&stop);
        let controller_failed = Arc::clone(&failed);
        let controller_resolution = Arc::clone(&resolution_request);
        let controller_readers = Arc::clone(&reader_handles);
        let controller_error = Arc::clone(&last_error);
        let controller = std::thread::Builder::new()
            .name("screen-controller".to_string())
            .spawn(move || {
                controller::controller_loop(
                    controller::ControllerContext {
                        frames: controller_frames,
                        output: controller_output,
                        stop: controller_stop,
                        failed: controller_failed,
                        resolution_request: controller_resolution,
                        reader_handles: controller_readers,
                        last_error: controller_error,
                        events,
                        shutdown,
                    },
                    input,
                    custom_encoder,
                );
            })
            .map_err(|error| error.to_string())?;

        // Encoder output → media server (spec §6).
        let (server_tx, server_rx) = mpsc::channel(forwarder::SERVER_CHANNEL_CAPACITY);
        server.attach_screen_stream(server_rx);
        let forwarder_output = Arc::clone(&output);
        let forwarder_stop = Arc::clone(&stop);
        let forwarder_dropped = Arc::clone(&dropped_segments);
        let forwarder = std::thread::Builder::new()
            .name("screen-forwarder".to_string())
            .spawn(move || {
                forwarder::forwarder_loop(
                    forwarder_output,
                    server_tx,
                    forwarder_stop,
                    forwarder_dropped,
                );
            })
            .map_err(|error| error.to_string())?;

        Ok(ScreenBridge {
            monitor_name,
            frames,
            output,
            stop,
            failed,
            resolution_request,
            reader_handles,
            last_error,
            dropped_segments,
            threads: vec![capture_thread, Some(controller), Some(forwarder)],
        })
    }

    /// The monitor this pipeline is streaming.
    pub fn monitor_name(&self) -> &str {
        &self.monitor_name
    }

    /// Feed one raw RGBA frame into the pipeline (used by the test harness
    /// without a capture thread; drop-oldest applies when the queue is full).
    pub fn push_frame(&self, bytes: Vec<u8>) {
        self.frames.push(bytes);
    }

    /// A cloneable frame-feed handle for background feeders (tests).
    pub fn feeder(&self) -> FrameFeeder {
        FrameFeeder::new(Arc::clone(&self.frames))
    }

    /// Ask the controller to restart the encoder with a new `-s WxH`
    /// (spec §3.2). The capture thread also drives this automatically on
    /// resolution changes; this setter is for tests and external callers.
    pub fn request_resolution(&self, width: u32, height: u32) {
        *lock(&self.resolution_request) = Some((width, height));
    }

    /// Request graceful teardown: threads stop and the encoder is closed
    /// with EOF → wait → kill. Idempotent.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Whether the pipeline is still running (not stopped, not failed).
    pub fn is_running(&self) -> bool {
        !self.stop.load(Ordering::Relaxed) && !self.failed.load(Ordering::Relaxed)
    }

    /// The last reported error, if the pipeline failed.
    pub fn last_error(&self) -> Option<String> {
        lock(&self.last_error).clone()
    }

    /// Buffered frames awaiting the encoder (diagnostics/tests).
    pub fn frames_len(&self) -> usize {
        self.frames.len()
    }

    /// Buffered encoded segments awaiting the HTTP consumer
    /// (diagnostics/tests).
    pub fn output_len(&self) -> usize {
        self.output.len()
    }

    /// How many encoded segments were dropped because a consumer fell
    /// behind (diagnostics/tests). Drops are whole segments, never byte
    /// slices.
    pub fn dropped_segments(&self) -> usize {
        self.dropped_segments.load(Ordering::Relaxed)
    }

    /// Stop the pipeline and join every thread (blocking; used at shutdown
    /// and by tests). Safe to call once; teardown is idempotent.
    pub fn join(mut self) {
        self.stop();
        for thread in self.threads.drain(..).flatten() {
            let _ = thread.join();
        }
        // Readers are spawned by the controller; by now the encoder is dead
        // so every reader has hit EOF. Drain what is left.
        let readers = lock(&self.reader_handles).drain(..).collect::<Vec<_>>();
        for reader in readers {
            let _ = reader.join();
        }
    }
}

/// Build a single-shot error reporter: surface `BackendEvent::StreamError`
/// once, then stop the pipeline.
fn report_once(
    events: mpsc::UnboundedSender<BackendEvent>,
    failed: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    last_error: Arc<Mutex<Option<String>>>,
) -> impl Fn(&str) + Send + Sync + 'static {
    move |message: &str| {
        if !failed.swap(true, Ordering::Relaxed) {
            *lock(&last_error) = Some(message.to_string());
            tracing::error!(%message, "screen pipeline failed");
            let _ = events.send(BackendEvent::StreamError(message.to_string()));
        }
        stop.store(true, Ordering::Relaxed);
    }
}

fn lock<T>(slot: &Mutex<T>) -> MutexGuard<'_, T> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}
