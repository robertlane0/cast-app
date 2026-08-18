// SPDX-License-Identifier: MIT OR Apache-2.0
//! Capture→ffmpeg stdin and ffmpeg stdout→HTTP channel bridges with
//! drop-oldest backpressure (`05-screen-capture.md` §5–§6).
//!
//! Threads:
//! - **capture** (spawned by `capture::start_capture`) pushes raw RGBA
//!   frames into the cap-2 frame queue;
//! - **controller** owns the `Ffmpeg` child: writes frames to its stdin,
//!   restarts it on resolution change, runs the EOF → wait → kill teardown,
//!   and reports unexpected exits;
//! - **stdout reader** (one per encoder generation) segments the encoded
//!   output at ISO-BMFF fragment boundaries (`screen::segments`) and pushes
//!   whole segments into the cap-8 output queue;
//! - **forwarder** moves output-queue segments into the media server's live
//!   stream channel, and tears the session down when the consumer closes.
//!
//! Backpressure is media-aware: the drop unit is one complete encoded
//! segment (a `moof`+`mdat` fragment, or a whole read of non-box-structured
//! output), never an arbitrary byte slice. A slow consumer therefore
//! receives a stream that skips whole fragments — a transient glitch — and
//! never truncated MP4 boxes, which would break decoding until the stream
//! is restarted. The init segment (`ftyp`+`moov`) is protected from
//! eviction everywhere, and an encoder restart clears the old generation's
//! buffered bytes so a fresh, valid init always leads the new stream.

use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::media::server::MediaServer;
use crate::screen::ffmpeg::{Ffmpeg, GRACE_PERIOD};
use crate::screen::segments::{EncodedSegment, Mp4Segmenter};
use crate::state::BackendEvent;
use crate::util::backpressure::BoundedDropOldest;
use crate::util::shutdown::Shutdown;

/// Capture → encoder frame queue capacity (AGENTS.md §7; drop-oldest).
pub const FRAME_QUEUE_CAPACITY: usize = 2;

/// Encoder → HTTP output queue capacity, measured in encoded segments
/// (AGENTS.md §7; drop-oldest evicts whole media fragments, never byte
/// slices). With the configured 1 s keyframe interval a segment is ~1 s of
/// video.
pub const OUTPUT_QUEUE_CAPACITY: usize = 8;

/// Media-server live-stream channel capacity (forwarder transport), in
/// encoded segments.
const SERVER_CHANNEL_CAPACITY: usize = 8;

/// Idle poll interval of the controller and forwarder threads.
const IDLE_POLL: Duration = Duration::from_millis(2);

/// Chunk size used by the stdout reader (64 KiB, same as the media server).
const STDOUT_CHUNK: usize = 64 * 1024;

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
            } => {
                let mut capture = crate::screen::capture::start_capture(
                    monitor_name.clone(),
                    Arc::clone(&frames),
                    Arc::clone(&stop),
                    Arc::clone(&resolution_request),
                    report_once(
                        events.clone(),
                        Arc::clone(&failed),
                        Arc::clone(&stop),
                        Arc::clone(&last_error),
                    ),
                    shutdown.clone(),
                )
                .map_err(|error| error.to_string())?;
                let handle = capture.join_handle();
                drop(capture);
                Some(handle)
            }
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
                controller_loop(
                    ControllerContext {
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
        let (server_tx, server_rx) = mpsc::channel(SERVER_CHANNEL_CAPACITY);
        server.attach_screen_stream(server_rx);
        let forwarder_output = Arc::clone(&output);
        let forwarder_stop = Arc::clone(&stop);
        let forwarder_dropped = Arc::clone(&dropped_segments);
        let forwarder = std::thread::Builder::new()
            .name("screen-forwarder".to_string())
            .spawn(move || {
                forwarder_loop(
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
        FrameFeeder {
            frames: Arc::clone(&self.frames),
        }
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

/// A cloneable handle that pushes frames into a running bridge (used by
/// tests and the runtime to feed the pipeline without a capture thread).
#[derive(Clone)]
pub struct FrameFeeder {
    frames: Arc<BoundedDropOldest<Vec<u8>>>,
}

impl FrameFeeder {
    /// Queue one raw RGBA frame (drop-oldest when the queue is full).
    pub fn push_frame(&self, bytes: Vec<u8>) {
        self.frames.push(bytes);
    }
}

/// Shared state for one pipeline run, passed to the controller thread.
struct ControllerContext {
    frames: Arc<BoundedDropOldest<Vec<u8>>>,
    output: Arc<BoundedDropOldest<EncodedSegment>>,
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    resolution_request: Arc<Mutex<Option<(u32, u32)>>>,
    reader_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    last_error: Arc<Mutex<Option<String>>>,
    events: mpsc::UnboundedSender<BackendEvent>,
    shutdown: Shutdown,
}

/// The encoder the controller currently owns: the X11 capture path always
/// encodes RGBA; the portal path re-negotiates its pixel format when the
/// compositor changes it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EncoderTarget {
    X11((u32, u32)),
    #[cfg(target_os = "linux")]
    Portal(crate::screen::pipewire::PwFormat),
}

impl EncoderTarget {
    /// Spawn the encoder for this target: the spec §4 args (X11) or the
    /// negotiated portal format, or a custom fake program in tests.
    fn spawn(
        &self,
        custom_encoder: &Option<(std::path::PathBuf, Vec<String>)>,
    ) -> io::Result<Ffmpeg> {
        match custom_encoder {
            Some((program, args)) => {
                let args: Vec<&str> = args.iter().map(String::as_str).collect();
                Ffmpeg::spawn_custom(program, &args)
            }
            None => match self {
                EncoderTarget::X11(size) => Ffmpeg::spawn(size.0, size.1),
                #[cfg(target_os = "linux")]
                EncoderTarget::Portal(format) => Ffmpeg::spawn_pipewire(
                    format.pix_fmt.ffmpeg_name(),
                    format.width,
                    format.height,
                ),
            },
        }
    }

    /// Human-readable form for restart logs.
    fn describe(&self) -> String {
        match self {
            EncoderTarget::X11(size) => format!("{}x{} (RGBA)", size.0, size.1),
            #[cfg(target_os = "linux")]
            EncoderTarget::Portal(format) => format!(
                "{}x{} ({})",
                format.width,
                format.height,
                format.pix_fmt.ffmpeg_name()
            ),
        }
    }
}

/// Portal-pipeline state owned by the controller: the portal client, the
/// PipeWire capture thread it spawned, and the negotiated format.
#[cfg(target_os = "linux")]
struct PortalState {
    /// Negotiated stream format (also the encoder's `-s`/`-pix_fmt`).
    format: crate::screen::pipewire::PwFormat,
    /// Live status channel from the PipeWire capture thread: format updates
    /// and failure reports.
    status_rx: std::sync::mpsc::Receiver<Result<crate::screen::pipewire::PwFormat, String>>,
    /// Stop flag for the PipeWire capture thread.
    pw_stop: Arc<AtomicBool>,
    /// The capture thread, joined during teardown.
    pw_thread: Option<JoinHandle<()>>,
    /// The portal client, closed during teardown.
    portal: Option<Box<dyn crate::screen::portal::ScreenCast>>,
    /// The open portal session handle.
    session: String,
}

/// The controller: owns the encoder child for the current resolution, feeds
/// it frames, restarts it on resolution/format changes, and runs the spec
/// §4.2 teardown (EOF → wait ≤5 s → kill) on stop or failure.
///
/// `custom_encoder` substitutes a fake program + args for `ffmpeg` (tests).
fn controller_loop(
    ctx: ControllerContext,
    input: PipelineInput,
    custom_encoder: Option<(std::path::PathBuf, Vec<String>)>,
) {
    let ControllerContext {
        frames,
        output,
        stop,
        failed,
        resolution_request,
        reader_handles,
        last_error,
        events,
        shutdown,
    } = ctx;
    let fail = report_once(events, failed, Arc::clone(&stop), last_error);
    let teardown_requested = || stop.load(Ordering::Relaxed) || shutdown.is_shutting_down();

    // Portal pipeline: run the dance and spawn the capture thread before
    // the first encoder (its format sizes the encoder's `-s WxH`).
    #[cfg(target_os = "linux")]
    let mut portal_state: Option<PortalState> = None;
    let mut current = match input {
        PipelineInput::Frames {
            initial_resolution, ..
        } => EncoderTarget::X11(initial_resolution),
        #[cfg(target_os = "linux")]
        PipelineInput::Portal { portal, capture } => {
            let state = match prepare_portal(
                portal,
                capture,
                Arc::clone(&frames),
                stop.clone(),
                shutdown.clone(),
            ) {
                Ok(state) => state,
                Err(error) => {
                    fail(&format!("portal capture failed: {error}"));
                    return;
                }
            };
            let target = EncoderTarget::Portal(state.format);
            portal_state = Some(state);
            target
        }
    };

    let mut encoder = match current.spawn(&custom_encoder) {
        Ok(encoder) => encoder,
        Err(error) => {
            fail(&format!("failed to start ffmpeg: {error}"));
            return;
        }
    };
    let stdout = encoder.take_stdout();
    if let Some(stdout) = stdout {
        spawn_stdout_reader(stdout, Arc::clone(&output), Arc::clone(&reader_handles));
    }
    let mut stdin = encoder.take_stdin();

    while !teardown_requested() {
        // A restart candidate: the capture thread requests one via the
        // resolution slot (xcap path) or the portal status channel.
        let mut next: Option<EncoderTarget> = None;
        #[cfg(target_os = "linux")]
        if let Some(state) = &portal_state {
            match state.status_rx.try_recv() {
                Ok(Ok(format)) => next = Some(EncoderTarget::Portal(format)),
                Ok(Err(error)) => {
                    if !teardown_requested() {
                        fail(&format!("pipewire capture failed: {error}"));
                    }
                    break;
                }
                Err(std::sync::mpsc::TryRecvError::Empty)
                | Err(std::sync::mpsc::TryRecvError::Disconnected) => {}
            }
        }
        let requested = *lock(&resolution_request);
        if let Some(size) = requested {
            next = Some(EncoderTarget::X11(size));
        }
        if let Some(target) = next {
            if target != current {
                tracing::info!(
                    target = %target.describe(),
                    "capture format changed; restarting encoder"
                );
                drop(stdin.take());
                let _ = encoder.wait_graceful(GRACE_PERIOD);
                // The old generation's stdout reader has hit EOF now that
                // its encoder is dead; join it so no stale bytes can land
                // after the output queue is cleared, then drop the old
                // generation entirely: the new encoder's init segment must
                // be the only init the consumer sees after a restart.
                let old_readers = lock(&reader_handles).drain(..).collect::<Vec<_>>();
                for reader in old_readers {
                    let _ = reader.join();
                }
                output.clear();
                match target.spawn(&custom_encoder) {
                    Ok(mut new_encoder) => {
                        if let Some(new_stdout) = new_encoder.take_stdout() {
                            spawn_stdout_reader(
                                new_stdout,
                                Arc::clone(&output),
                                Arc::clone(&reader_handles),
                            );
                        }
                        stdin = new_encoder.take_stdin();
                        encoder = new_encoder;
                        current = target;
                    }
                    Err(error) => {
                        fail(&format!(
                            "failed to restart ffmpeg at {}: {error}",
                            target.describe()
                        ));
                        break;
                    }
                }
            }
            *lock(&resolution_request) = None;
            continue;
        }

        // Unexpected encoder exit (spec §4.2: non-zero status stops the
        // pipeline and surfaces an error). Silent during teardown.
        match encoder.try_exit() {
            Ok(Some(status)) => {
                if teardown_requested() {
                    break;
                }
                let tail = encoder.stderr_tail().join("\n");
                if status.success() {
                    fail(&format!(
                        "ffmpeg stopped unexpectedly (exit 0); stderr:\n{tail}"
                    ));
                } else {
                    fail(&format!(
                        "ffmpeg exited unexpectedly with {status}; stderr:\n{tail}"
                    ));
                }
                break;
            }
            Ok(None) => {}
            Err(error) => {
                if !teardown_requested() {
                    fail(&format!("polling ffmpeg failed: {error}"));
                }
                break;
            }
        }

        // Feed frames to the encoder's stdin.
        if let Some(frame) = frames.try_pop() {
            match stdin.as_mut().map(|pipe| pipe.write_all(&frame)) {
                None => {
                    if !teardown_requested() {
                        fail("ffmpeg stdin closed unexpectedly");
                    }
                    break;
                }
                Some(Ok(())) => {}
                Some(Err(error)) => {
                    if !teardown_requested() {
                        fail(&format!("writing to ffmpeg stdin failed: {error}"));
                    }
                    break;
                }
            }
        }

        std::thread::sleep(IDLE_POLL);
    }

    // Spec §4.2 teardown: EOF (drop stdin) → wait ≤5 s → kill. The portal
    // pipeline stops its capture thread and closes the portal session first
    // (the dialog/stream may keep running otherwise; the session is owned by
    // this controller).
    #[cfg(target_os = "linux")]
    if let Some(state) = portal_state.take() {
        teardown_portal(state);
    }
    drop(stdin);
    let _ = encoder.wait_graceful(GRACE_PERIOD);
    tracing::info!("encoder torn down");
}

/// Run the Wayland portal dance and spawn the PipeWire capture thread,
/// returning the negotiated format and the pieces the teardown needs. Any
/// error cleans up what was already acquired (capture thread joined, session
/// closed) before returning.
#[cfg(target_os = "linux")]
fn prepare_portal(
    portal: Option<Box<dyn crate::screen::portal::ScreenCast>>,
    capture: Option<Arc<crate::screen::pipewire::PipewireSpawner>>,
    frames: Arc<BoundedDropOldest<Vec<u8>>>,
    stop: Arc<AtomicBool>,
    shutdown: Shutdown,
) -> Result<PortalState, String> {
    use crate::screen::pipewire::{PipewireSpawner, PwFormat};
    use crate::screen::portal::{AbortSignal, ScreenCast, ZbusScreenCast};

    let portal: Box<dyn ScreenCast> = match portal {
        Some(portal) => portal,
        None => Box::new(ZbusScreenCast::connect_blocking().map_err(|error| error.to_string())?),
    };
    let session = portal
        .create_session("cast_app_capture")
        .map_err(|error| error.to_string())?;
    portal
        .select_sources(&session)
        .map_err(|error| error.to_string())?;
    let abort = AbortSignal::new(Arc::clone(&stop), shutdown.clone());
    let _stream = portal
        .start(&session, &abort)
        .map_err(|error| error.to_string())?;
    let fd = portal
        .open_pipewire_remote(&session)
        .map_err(|error| error.to_string())?;

    let (status_tx, status_rx) = std::sync::mpsc::channel();
    let pw_stop = Arc::new(AtomicBool::new(false));
    let spawner: Arc<PipewireSpawner> = match capture {
        Some(spawner) => spawner,
        None => Arc::new(crate::screen::pipewire::spawn_pipewire_capture),
    };
    let pw_thread = spawner(fd, Arc::clone(&frames), status_tx, Arc::clone(&pw_stop))
        .map_err(|error| error.to_string())?;

    let format: PwFormat = match wait_for_format(&status_rx, &stop, &shutdown) {
        Ok(format) => format,
        Err(error) => {
            pw_stop.store(true, Ordering::Relaxed);
            let _ = pw_thread.join();
            let _ = portal.close(&session);
            return Err(error);
        }
    };
    tracing::info!(
        width = format.width,
        height = format.height,
        pix_fmt = format.pix_fmt.ffmpeg_name(),
        "portal capture negotiated"
    );
    Ok(PortalState {
        format,
        status_rx,
        pw_stop,
        pw_thread: Some(pw_thread),
        portal: Some(portal),
        session,
    })
}

/// Wait for the PipeWire capture thread to negotiate its format, polling the
/// status channel against the teardown signals (a pending share dialog or a
/// stalled stream must never block shutdown forever).
#[cfg(target_os = "linux")]
fn wait_for_format(
    status_rx: &std::sync::mpsc::Receiver<Result<crate::screen::pipewire::PwFormat, String>>,
    stop: &Arc<AtomicBool>,
    shutdown: &Shutdown,
) -> Result<crate::screen::pipewire::PwFormat, String> {
    loop {
        match status_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(format)) => return Ok(format),
            Ok(Err(error)) => return Err(format!("pipewire capture failed: {error}")),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if stop.load(Ordering::Relaxed) || shutdown.is_shutting_down() {
                    return Err("capture aborted while waiting for the pipewire format".to_string());
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(
                    "the pipewire capture thread ended before negotiating a format".to_string(),
                );
            }
        }
    }
}

/// Stop the PipeWire capture thread, join it, and close the portal session.
#[cfg(target_os = "linux")]
fn teardown_portal(state: PortalState) {
    let PortalState {
        format,
        status_rx,
        pw_stop,
        pw_thread,
        portal,
        session,
    } = state;
    drop((format, status_rx));
    pw_stop.store(true, Ordering::Relaxed);
    if let Some(thread) = pw_thread {
        let _ = thread.join();
    }
    if let Some(portal) = portal {
        if let Err(error) = portal.close(&session) {
            tracing::warn!(%error, "failed to close the portal session");
        }
    }
}

/// Read encoded bytes from the encoder's stdout, cut them at decoder-safe
/// ISO-BMFF boundaries, and push the resulting segments into the cap-8
/// output queue until EOF. One thread per encoder generation.
fn spawn_stdout_reader(
    stdout: std::process::ChildStdout,
    output: Arc<BoundedDropOldest<EncodedSegment>>,
    reader_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
) {
    let reader = std::thread::Builder::new()
        .name("ffmpeg-stdout".to_string())
        .spawn(move || {
            let mut stdout = stdout;
            let mut chunk = vec![0u8; STDOUT_CHUNK];
            let mut segmenter = Mp4Segmenter::new();
            loop {
                match stdout.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(read) => {
                        for segment in segmenter.feed(&chunk[..read]) {
                            push_segment(&output, segment);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        tracing::warn!(%error, "encoder stdout read failed");
                        break;
                    }
                }
            }
            for segment in segmenter.finish() {
                push_segment(&output, segment);
            }
            tracing::debug!("encoder stdout reader finished");
        });
    if let Ok(reader) = reader {
        // Reap finished readers from earlier encoder generations before
        // accumulating the new handle (ISS-006).
        let mut handles = lock(&reader_handles);
        handles.retain(|handle| !handle.is_finished());
        handles.push(reader);
    } else {
        tracing::warn!("failed to spawn the encoder stdout reader");
    }
}

/// Push one encoded segment into the output queue. When the queue is full,
/// the oldest *fragment* is evicted — the init segment is protected, since
/// every following fragment depends on it. If the queue holds only
/// protected segments (pathological), the newest segment is dropped instead:
/// a whole-segment skip, never corrupt bytes.
fn push_segment(output: &BoundedDropOldest<EncodedSegment>, segment: EncodedSegment) {
    let evictable = |buffered: &EncodedSegment| matches!(buffered, EncodedSegment::Fragment(_));
    if let Some(rejected) = output.push_or(segment, evictable) {
        tracing::warn!(
            bytes = rejected.len(),
            "dropping encoded segment; queue holds only the protected init"
        );
    }
}

/// Move output-queue segments into the media server's live-stream channel.
/// When the consumer closes (client disconnect or source switch), the whole
/// session is torn down (spec §4.2).
///
/// Drop policy is media-aware: a fragment that does not fit is dropped as a
/// whole (a skipped play interval, never a truncated box); the init segment
/// is never dropped — it is retried until the consumer makes room, so a
/// restart always leads with a fresh, valid initialization.
fn forwarder_loop(
    output: Arc<BoundedDropOldest<EncodedSegment>>,
    server_tx: mpsc::Sender<Vec<u8>>,
    stop: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
) {
    while !stop.load(Ordering::Relaxed) {
        if let Some(segment) = output.try_pop() {
            let is_init = matches!(segment, EncodedSegment::Init(_));
            let mut bytes = segment.into_bytes();
            loop {
                match server_tx.try_send(bytes) {
                    Ok(()) => break,
                    Err(mpsc::error::TrySendError::Full(returned)) => {
                        bytes = returned;
                        if is_init {
                            // The init must reach the consumer: wait for the
                            // slow client to drain instead of dropping it
                            // (a fragment drop is a skip, an init drop is a
                            // corrupt stream).
                            tracing::debug!(
                                "waiting for the consumer to make room for the init segment"
                            );
                            std::thread::sleep(IDLE_POLL);
                        } else {
                            // The server buffer is full (slow client): drop
                            // the whole fragment and accept a transient
                            // glitch (spec §6).
                            dropped.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                bytes = bytes.len(),
                                "dropping encoded fragment; consumer is slow"
                            );
                            break;
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        tracing::info!("screen stream consumer closed; tearing down the encoder");
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
        std::thread::sleep(IDLE_POLL);
    }
    tracing::debug!("forwarder stopped");
}

fn lock<T>(slot: &Mutex<T>) -> MutexGuard<'_, T> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}
