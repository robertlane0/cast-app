// SPDX-License-Identifier: MIT OR Apache-2.0
//! Controller thread (`05-screen-capture.md` §4–§6): owns the `ffmpeg`
//! child for the current resolution/format, feeds raw frames to its stdin,
//! restarts it on resolution/format changes, and runs the spec §4.2 teardown
//! (EOF → wait ≤5 s → kill). On Wayland it also runs the portal dance and
//! owns the PipeWire capture thread (`05-screen-capture.md` §3.4).
//!
//! Encoder restarts are generation bookkeeping ([`EncoderState::restart`]):
//! the old generation is EOF'd and reaped, its stdout readers are joined so
//! no stale bytes can land after the output queue is cleared, and the next
//! generation is spawned — a fresh, valid init segment must be the only init
//! the consumer sees after a restart.

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ChildStdin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use tokio::sync::mpsc;

use crate::screen::ffmpeg::{Ffmpeg, GRACE_PERIOD};
use crate::screen::segments::EncodedSegment;
use crate::state::BackendEvent;
use crate::util::backpressure::BoundedDropOldest;
use crate::util::shutdown::Shutdown;

use super::stdout_reader::spawn_stdout_reader;
use super::{IDLE_POLL, PipelineInput, lock, report_once};

#[cfg(target_os = "linux")]
use std::time::Duration;

/// Shared state for one pipeline run, passed to the controller thread.
pub(super) struct ControllerContext {
    pub(super) frames: Arc<BoundedDropOldest<Vec<u8>>>,
    pub(super) output: Arc<BoundedDropOldest<EncodedSegment>>,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) failed: Arc<AtomicBool>,
    pub(super) resolution_request: Arc<Mutex<Option<(u32, u32)>>>,
    pub(super) reader_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    pub(super) last_error: Arc<Mutex<Option<String>>>,
    pub(super) events: mpsc::UnboundedSender<BackendEvent>,
    pub(super) shutdown: Shutdown,
}

/// The encoder the controller currently owns: the X11 capture path always
/// encodes RGBA; the portal path re-negotiates its pixel format when the
/// compositor changes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EncoderTarget {
    X11((u32, u32)),
    #[cfg(target_os = "linux")]
    Portal(crate::screen::pipewire::PwFormat),
}

impl EncoderTarget {
    /// Spawn the encoder for this target: the spec §4 args (X11) or the
    /// negotiated portal format, or a custom fake program in tests.
    fn spawn(&self, custom_encoder: &Option<(PathBuf, Vec<String>)>) -> io::Result<Ffmpeg> {
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

/// One encoder generation: the running child, its stdin pipe, and the
/// target it was spawned for. The restart bookkeeping lives here so it can
/// be unit-tested directly (the full-pipeline tests only observe it
/// indirectly through `encoder_restart_emits_a_fresh_valid_init_segment`).
struct EncoderState {
    /// The target the current generation was spawned for.
    current: EncoderTarget,
    /// The running child.
    encoder: Ffmpeg,
    /// The child's stdin, closed (EOF) to finalize a generation.
    stdin: Option<ChildStdin>,
}

impl EncoderState {
    /// Spawn the generation for `current`: wire its stdout reader into
    /// `reader_handles` and take its stdin.
    fn spawn(
        current: EncoderTarget,
        custom_encoder: &Option<(PathBuf, Vec<String>)>,
        output: &Arc<BoundedDropOldest<EncodedSegment>>,
        reader_handles: &Arc<Mutex<Vec<JoinHandle<()>>>>,
    ) -> io::Result<Self> {
        let mut encoder = current.spawn(custom_encoder)?;
        if let Some(stdout) = encoder.take_stdout() {
            spawn_stdout_reader(stdout, Arc::clone(output), Arc::clone(reader_handles));
        }
        let stdin = encoder.take_stdin();
        Ok(Self {
            current,
            encoder,
            stdin,
        })
    }

    /// Restart bookkeeping for a capture format change: EOF the old child
    /// (drop stdin), wait for its graceful exit, join the old generation's
    /// stdout readers so no stale bytes can land after the output queue is
    /// cleared, clear the queue, then spawn the next generation. The new
    /// init segment must be the only init the consumer sees after a restart.
    fn restart(
        &mut self,
        target: EncoderTarget,
        custom_encoder: &Option<(PathBuf, Vec<String>)>,
        output: &Arc<BoundedDropOldest<EncodedSegment>>,
        reader_handles: &Arc<Mutex<Vec<JoinHandle<()>>>>,
    ) -> Result<(), String> {
        drop(self.stdin.take());
        let _ = self.encoder.wait_graceful(GRACE_PERIOD);
        let old_readers = lock(reader_handles).drain(..).collect::<Vec<_>>();
        for reader in old_readers {
            let _ = reader.join();
        }
        output.clear();
        match Self::spawn(target, custom_encoder, output, reader_handles) {
            Ok(state) => {
                *self = state;
                Ok(())
            }
            Err(error) => Err(format!(
                "failed to restart ffmpeg at {}: {error}",
                target.describe()
            )),
        }
    }

    /// Feed one raw frame to the encoder's stdin; a closed pipe (the child
    /// exited, or its stdin was never taken) is an error, never a hang.
    fn feed(&mut self, frame: &[u8]) -> io::Result<()> {
        let pipe = self.stdin.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "ffmpeg stdin closed unexpectedly",
            )
        })?;
        pipe.write_all(frame)
    }

    /// Spec §4.2 teardown: EOF (drop stdin) → wait ≤5 s → kill.
    fn teardown(&mut self) {
        drop(self.stdin.take());
        let _ = self.encoder.wait_graceful(GRACE_PERIOD);
        tracing::info!("encoder torn down");
    }
}

/// The controller: owns the encoder child for the current resolution, feeds
/// it frames, restarts it on resolution/format changes, and runs the spec
/// §4.2 teardown (EOF → wait ≤5 s → kill) on stop or failure.
///
/// `custom_encoder` substitutes a fake program + args for `ffmpeg` (tests).
pub(super) fn controller_loop(
    ctx: ControllerContext,
    input: PipelineInput,
    custom_encoder: Option<(PathBuf, Vec<String>)>,
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
    let current = match input {
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

    let mut encoder_state =
        match EncoderState::spawn(current, &custom_encoder, &output, &reader_handles) {
            Ok(state) => state,
            Err(error) => {
                fail(&format!("failed to start ffmpeg: {error}"));
                return;
            }
        };

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
            if target != encoder_state.current {
                tracing::info!(
                    target = %target.describe(),
                    "capture format changed; restarting encoder"
                );
                if let Err(error) =
                    encoder_state.restart(target, &custom_encoder, &output, &reader_handles)
                {
                    fail(&error);
                    break;
                }
            }
            *lock(&resolution_request) = None;
            continue;
        }

        // Unexpected encoder exit (spec §4.2: non-zero status stops the
        // pipeline and surfaces an error). Silent during teardown.
        match encoder_state.encoder.try_exit() {
            Ok(Some(status)) => {
                if teardown_requested() {
                    break;
                }
                let tail = encoder_state.encoder.stderr_tail().join("\n");
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
            if let Err(error) = encoder_state.feed(&frame) {
                if !teardown_requested() {
                    fail(&format!("writing to ffmpeg stdin failed: {error}"));
                }
                break;
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
    encoder_state.teardown();
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
    let stream = portal
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
    // The capture stream must target the exact node id the portal granted
    // (`stream.id`); the portal's restricted PipeWire remote otherwise
    // leaves autoconnect to guess, which fails negotiation right after the
    // stream reports itself connected.
    let pw_thread = spawner(
        fd,
        stream.id,
        Arc::clone(&frames),
        status_tx,
        Arc::clone(&pw_stop),
    )
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::Duration;

    fn sh() -> PathBuf {
        PathBuf::from("sh")
    }

    fn scratch(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cast-app-controller-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    /// Fake encoder script: appends a "started" line per launch (an encoder
    /// generation marker), then consumes stdin until EOF and exits 0.
    fn fake_encoder(log: &Path) -> String {
        format!(
            "echo started >> '{}'; cat >/dev/null; exit 0",
            log.display()
        )
    }

    fn custom_encoder(log: &Path) -> Option<(PathBuf, Vec<String>)> {
        Some((sh(), vec!["-c".into(), fake_encoder(log)]))
    }

    /// Minimal controller context for one `EncoderState`: an empty output
    /// queue (capacity 8) and reader-handle list.
    type StateContext = (
        Arc<BoundedDropOldest<EncodedSegment>>,
        Arc<Mutex<Vec<JoinHandle<()>>>>,
    );

    fn context() -> StateContext {
        (
            Arc::new(BoundedDropOldest::new(8)),
            Arc::new(Mutex::new(Vec::new())),
        )
    }

    /// A restart must spawn a genuinely new encoder process, join the old
    /// generation's readers, clear the stale generation's buffered bytes,
    /// and record the new target — the generation bookkeeping the
    /// full-pipeline tests only observe indirectly
    /// (`encoder_restart_emits_a_fresh_valid_init_segment`).
    #[test]
    fn restart_bookkeeping_spawns_a_new_generation() {
        let log = scratch("restart");
        let _ = std::fs::remove_file(&log);
        let custom = custom_encoder(&log);
        let (output, reader_handles) = context();

        let mut state = EncoderState::spawn(
            EncoderTarget::X11((8, 8)),
            &custom,
            &output,
            &reader_handles,
        )
        .expect("generation 0 must spawn");
        let generation0_pid = state.encoder.pid().expect("a fake encoder process");
        // Stale bytes the old generation's reader left buffered; a restart
        // must clear them so the new init leads the stream.
        output.push(EncodedSegment::Init(vec![0xAA; 16]));
        output.push(EncodedSegment::Fragment(vec![0xAA; 16]));

        state
            .restart(
                EncoderTarget::X11((16, 16)),
                &custom,
                &output,
                &reader_handles,
            )
            .expect("the restart must succeed");

        assert_eq!(state.current, EncoderTarget::X11((16, 16)));
        assert_ne!(
            state.encoder.pid(),
            Some(generation0_pid),
            "a new encoder process must be spawned"
        );
        assert!(
            output.is_empty(),
            "the stale generation's bytes must be cleared"
        );
        assert_eq!(
            reader_handles.lock().unwrap().len(),
            1,
            "exactly one live stdout reader (the new generation's)"
        );

        state.teardown();
        let _ = std::fs::remove_file(&log);
    }

    /// A restart whose spawn fails reports the failure and leaves the old
    /// generation already reaped (the controller then surfaces the error
    /// and stops).
    #[test]
    fn restart_reports_a_spawn_failure() {
        let log = scratch("restart-fail");
        let _ = std::fs::remove_file(&log);
        let good = custom_encoder(&log);
        let (output, reader_handles) = context();

        let mut state =
            EncoderState::spawn(EncoderTarget::X11((8, 8)), &good, &output, &reader_handles)
                .expect("generation 0 must spawn");
        let bad: Option<(PathBuf, Vec<String>)> =
            Some((PathBuf::from("/nonexistent/encoder-binary"), Vec::new()));

        let error = state
            .restart(EncoderTarget::X11((16, 16)), &bad, &output, &reader_handles)
            .expect_err("the restart must fail");

        assert!(
            error.contains("failed to restart ffmpeg"),
            "unexpected error: {error}"
        );
        state.teardown();
        let _ = std::fs::remove_file(&log);
    }

    /// Frames written into a live encoder's stdin flow through (the fake
    /// consumes them and observes the EOF on teardown).
    #[test]
    fn feed_writes_into_the_live_encoder_stdin() {
        let marker = scratch("feed");
        let _ = std::fs::remove_file(&marker);
        let custom = Some((
            sh(),
            vec![
                "-c".into(),
                format!("cat >/dev/null; touch '{}'", marker.display()),
            ],
        ));
        let (output, reader_handles) = context();

        let mut state = EncoderState::spawn(
            EncoderTarget::X11((4, 4)),
            &custom,
            &output,
            &reader_handles,
        )
        .expect("the encoder must spawn");
        state
            .feed(&[0x11; 1024])
            .expect("a live encoder accepts frames");
        state.teardown();

        assert!(
            marker.exists(),
            "the encoder must observe the fed bytes and stdin EOF"
        );
        let _ = std::fs::remove_file(&marker);
    }

    /// A dead encoder's closed stdin is reported as an error, never a hang:
    /// the controller must not block on a pipe whose reader is gone.
    #[test]
    fn feed_reports_a_closed_stdin_after_the_encoder_dies() {
        // An encoder that exits immediately, never reading its stdin.
        let custom = Some((sh(), vec!["-c".into(), "exit 0".into()]));
        let (output, reader_handles) = context();

        let mut state = EncoderState::spawn(
            EncoderTarget::X11((4, 4)),
            &custom,
            &output,
            &reader_handles,
        )
        .expect("the encoder must spawn");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while state.encoder.try_exit().ok().flatten().is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "the fake encoder never exited"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        let error = state
            .feed(&[0x22; 8])
            .expect_err("a dead encoder's stdin must be closed");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        state.teardown();
    }
}
