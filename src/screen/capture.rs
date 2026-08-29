// SPDX-License-Identifier: MIT OR Apache-2.0
//! xcap monitor enumeration and the 30 fps capture loop on a dedicated
//! `std::thread` (`05-screen-capture.md` §3).
//!
//! Note: xcap 0.9.6 has no `Monitor::from_name`; the spec's "map the GUI
//! display name to the xcap monitor by its `name()`" is implemented as a
//! `Monitor::all()` scan matching `name()`.

use std::ffi::OsStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::util::backpressure::BoundedDropOldest;
use crate::util::shutdown::Shutdown;

/// Capture pacing (`05-screen-capture.md` §3: fixed 30 fps).
pub const CAPTURE_RATE: u32 = 30;

/// Consecutive capture failures after which the pipeline stops and surfaces
/// an error to the GUI (spec §3.3).
pub const MAX_CONSECUTIVE_FAILURES: u32 = 5;

/// Re-acquire the monitor handle every `MONITOR_REACQUIRE_INTERVAL` frames
/// (1 s at 30 fps) so display unplug/replug is picked up.
const MONITOR_REACQUIRE_INTERVAL: u32 = 30;

/// One captured frame. Byte order is RGBA for the pinned xcap version (see
/// `bgra_rgba::XCAP_FRAMES_ARE_RGBA`), matching the `-pix_fmt rgba` encoder
/// input (`05-screen-capture.md` §3.1, §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub width: u32,
    pub height: u32,
    /// `width * height * 4` bytes, RGBA.
    pub bytes: Vec<u8>,
}

/// The single "monitor" entry exposed on Wayland sessions (portal-driven
/// capture; `05-screen-capture.md` §3.4). The portal yields one stream for
/// the whole monitor selection, so there is nothing to enumerate.
pub const WAYLAND_SCREEN_ENTRY: &str = "Screen";

/// Names of all monitors, sorted and deduplicated (for
/// `BackendEvent::DisplaysUpdated`).
///
/// On a Wayland-only session this returns the virtual [`WAYLAND_SCREEN_ENTRY`]
/// entry when the portal path is usable (ffmpeg present + portal reachable),
/// so the Display source stays available; otherwise it returns an error and
/// the Display source must be disabled (spec §3; `01-architecture.md` §8).
pub fn monitor_names() -> Result<Vec<String>, String> {
    if is_wayland_session() {
        #[cfg(target_os = "linux")]
        {
            if crate::screen::ffmpeg_discover::ffmpeg_available()
                && crate::screen::portal::portal_available()
            {
                return Ok(vec![WAYLAND_SCREEN_ENTRY.to_string()]);
            }
            return Err(
                "Wayland screen capture needs ffmpeg on PATH and the xdg-desktop-portal \
                 service; install ffmpeg and run a portal-backed session"
                    .to_string(),
            );
        }
        #[cfg(not(target_os = "linux"))]
        return Err(
            "screen capture is unavailable on Wayland sessions; run under X11/XWayland".to_string(),
        );
    }
    let mut names: Vec<String> = Vec::new();
    for monitor in xcap::Monitor::all().map_err(|error| error.to_string())? {
        names.push(monitor.name().map_err(|error| error.to_string())?);
    }
    names.sort();
    names.dedup();
    Ok(names)
}

/// Whether the current session is Wayland (`XDG_SESSION_TYPE=wayland` or a
/// `WAYLAND_DISPLAY` socket is set). Pure env probe, gated to Linux where
/// xcap's capture path requires X11.
pub fn is_wayland_session() -> bool {
    wayland_for(
        std::env::var_os("XDG_SESSION_TYPE").as_deref(),
        std::env::var_os("WAYLAND_DISPLAY").as_deref(),
    )
}

/// Pure session-type test (`01-architecture.md` §8).
fn wayland_for(session_type: Option<&OsStr>, wayland_display: Option<&OsStr>) -> bool {
    if !cfg!(target_os = "linux") {
        return false;
    }
    let session_is_wayland = session_type
        .map(|value| value.to_string_lossy() == "wayland")
        .unwrap_or(false);
    session_is_wayland || wayland_display.is_some()
}

/// A source of raw frames. Extracted from the capture loop so tests can
/// drive failure counting, resolution changes and stop handling without a
/// real display.
///
/// Not `Send`-bounded: on Windows `xcap::Monitor` wraps an `HMONITOR` raw
/// pointer and is `!Send`, so the source is constructed and used entirely on
/// the capture thread (`start_capture` moves only the monitor *name* across).
pub trait FrameSource {
    /// Capture one frame, or explain the transient/permanent failure.
    fn capture_frame(&mut self) -> Result<Frame, String>;
    /// Re-acquire the underlying display handle (display hotplug).
    fn reacquire(&mut self) -> Result<(), String>;
}

/// xcap-backed frame source for the selected monitor.
struct X11MonitorSource {
    name: String,
    monitor: Option<xcap::Monitor>,
}

impl X11MonitorSource {
    fn new(name: String) -> Result<Self, String> {
        let monitor = Some(monitor_by_name(&name)?);
        Ok(Self { name, monitor })
    }
}

impl FrameSource for X11MonitorSource {
    fn capture_frame(&mut self) -> Result<Frame, String> {
        let monitor = self.monitor.as_ref().ok_or("monitor handle lost")?;
        let image = monitor.capture_image().map_err(|error| error.to_string())?;
        Ok(Frame {
            width: image.width(),
            height: image.height(),
            bytes: image.into_raw(),
        })
    }

    fn reacquire(&mut self) -> Result<(), String> {
        self.monitor = Some(monitor_by_name(&self.name)?);
        Ok(())
    }
}

/// Locate an xcap monitor by its `name()` (`05-screen-capture.md` §3).
fn monitor_by_name(name: &str) -> Result<xcap::Monitor, String> {
    for monitor in xcap::Monitor::all().map_err(|error| error.to_string())? {
        if monitor.name().map_err(|error| error.to_string())? == name {
            return Ok(monitor);
        }
    }
    Err(format!("no monitor named {name:?}"))
}

/// Handle to a running capture thread.
pub struct CaptureHandle {
    /// Monitor resolution observed at start, for the first ffmpeg `-s WxH`.
    pub initial_resolution: (u32, u32),
    thread: Option<JoinHandle<()>>,
}

impl CaptureHandle {
    /// Join the capture thread (blocking; used at shutdown and in tests).
    pub fn join(&mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    /// Take the underlying join handle (handed to the bridge's thread pool).
    pub fn join_handle(&mut self) -> JoinHandle<()> {
        self.thread
            .take()
            .expect("the capture join handle is taken exactly once")
    }
}

/// Resolve `name` to the monitor's current pixel dimensions
/// (`05-screen-capture.md` §3.2). Used by the bridge to size the first
/// ffmpeg `-s WxH` before the capture thread starts.
pub fn monitor_resolution(name: &str) -> Result<(u32, u32), String> {
    let monitor = monitor_by_name(name)?;
    monitor
        .width()
        .ok()
        .zip(monitor.height().ok())
        .ok_or_else(|| format!("failed to read the resolution of monitor {name:?}"))
}

/// Spawn the dedicated capture thread for `monitor_name`
/// (`05-screen-capture.md` §3).
///
/// The thread pushes raw RGBA frames into `frames` (drop-oldest backpressure)
/// at 30 fps, signals resolution changes through `resolution_request`, stops
/// on `stop`, and reports a permanent failure through `on_error` after
/// [`MAX_CONSECUTIVE_FAILURES`] consecutive capture errors (spec §3.3) or
/// consecutive monitor re-acquire failures (counted independently).
pub fn start_capture(
    monitor_name: String,
    frames: Arc<BoundedDropOldest<Vec<u8>>>,
    stop: Arc<AtomicBool>,
    resolution_request: Arc<Mutex<Option<(u32, u32)>>>,
    on_error: impl Fn(&str) + Send + Sync + 'static,
    shutdown: Shutdown,
) -> Result<CaptureHandle, String> {
    if is_wayland_session() {
        return Err(
            "screen capture is unavailable on Wayland sessions; run under X11/XWayland".to_string(),
        );
    }
    let initial_resolution = monitor_resolution(&monitor_name)?;
    let on_error = Arc::new(on_error);
    let thread = std::thread::Builder::new()
        .name("screen-capture".to_string())
        .spawn(move || {
            // xcap's `Monitor` is `!Send` on Windows (HMONITOR); build the
            // source on this thread so only the name string crosses threads.
            let source = match X11MonitorSource::new(monitor_name) {
                Ok(source) => source,
                Err(error) => {
                    on_error(&format!("failed to open monitor: {error}"));
                    return;
                }
            };
            run_capture(source, frames, stop, resolution_request, on_error, shutdown);
        })
        .map_err(|error| error.to_string())?;
    Ok(CaptureHandle {
        initial_resolution,
        thread: Some(thread),
    })
}

/// The capture loop: pace to [`CAPTURE_RATE`], convert to RGBA, signal
/// resolution changes, count failures. Public so integration tests can drive
/// it with a [`ScriptedSource`] (same pattern as `cast::connection::test_support`).
///
/// Capture and monitor re-acquire failures are counted independently: a
/// successful capture no longer masks persistent re-acquire failures (ISS-007).
pub fn run_capture<S: FrameSource>(
    mut source: S,
    frames: Arc<BoundedDropOldest<Vec<u8>>>,
    stop: Arc<AtomicBool>,
    resolution_request: Arc<Mutex<Option<(u32, u32)>>>,
    on_error: Arc<dyn Fn(&str) + Send + Sync>,
    shutdown: Shutdown,
) {
    let interval = Duration::from_secs_f64(1.0 / CAPTURE_RATE as f64);
    let mut capture_failures: u32 = 0;
    let mut reacquire_failures: u32 = 0;
    let mut expected: Option<(u32, u32)> = None;
    let mut frame_count: u32 = 0;

    while !stop.load(Ordering::Relaxed) && !shutdown.is_shutting_down() {
        let started = Instant::now();
        match source.capture_frame() {
            Ok(frame) => {
                capture_failures = 0;
                let size = (frame.width, frame.height);
                if expected != Some(size) {
                    expected = Some(size);
                    *lock(&resolution_request) = Some(size);
                }
                let mut bytes = frame.bytes;
                if !crate::screen::bgra_rgba::XCAP_FRAMES_ARE_RGBA {
                    crate::screen::bgra_rgba::bgra_to_rgba(&mut bytes);
                }
                frames.push(bytes);
            }
            Err(error) => {
                capture_failures += 1;
                tracing::warn!(capture_failures, %error, "screen capture failed");
                if capture_failures >= MAX_CONSECUTIVE_FAILURES {
                    on_error(&format!(
                        "screen capture failed {capture_failures} times in a row: {error}"
                    ));
                    break;
                }
            }
        }
        frame_count += 1;
        if frame_count.is_multiple_of(MONITOR_REACQUIRE_INTERVAL) {
            if let Err(error) = source.reacquire() {
                reacquire_failures += 1;
                tracing::warn!(reacquire_failures, %error, "monitor re-acquire failed");
                if reacquire_failures >= MAX_CONSECUTIVE_FAILURES {
                    on_error(&format!("monitor lost: {error}"));
                    break;
                }
            } else {
                reacquire_failures = 0;
            }
        }
        if let Some(remaining) = interval.checked_sub(started.elapsed()) {
            std::thread::sleep(remaining);
        }
    }
    tracing::debug!("capture thread stopped");
}

fn lock(slot: &Mutex<Option<(u32, u32)>>) -> MutexGuard<'_, Option<(u32, u32)>> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A fake source for tests: scripted frames, failures, resolutions.
/// Always compiled (same pattern as `cast::connection::test_support`).
pub struct ScriptedSource {
    script: Vec<Result<Frame, String>>,
    reacquire_script: Vec<Result<(), String>>,
    cursor: usize,
    reacquire_cursor: usize,
}

impl ScriptedSource {
    pub fn new(script: Vec<Result<Frame, String>>) -> Self {
        Self {
            script,
            reacquire_script: vec![Ok(())],
            cursor: 0,
            reacquire_cursor: 0,
        }
    }

    pub fn with_reacquire(mut self, result: Result<(), String>) -> Self {
        self.reacquire_script = vec![result];
        self
    }

    /// Cycle through `results` on every re-acquire call (wraps around).
    pub fn with_reacquire_script(mut self, results: Vec<Result<(), String>>) -> Self {
        if !results.is_empty() {
            self.reacquire_script = results;
        }
        self
    }
}

impl FrameSource for ScriptedSource {
    fn capture_frame(&mut self) -> Result<Frame, String> {
        if self.cursor >= self.script.len() {
            // Script exhausted: keep failing so the loop's failure counter
            // trips and the thread ends deterministically.
            return Err("script exhausted".to_string());
        }
        let item = self.script[self.cursor].clone();
        self.cursor += 1;
        item
    }

    fn reacquire(&mut self) -> Result<(), String> {
        let result =
            self.reacquire_script[self.reacquire_cursor % self.reacquire_script.len()].clone();
        self.reacquire_cursor += 1;
        result
    }
}

/// A uniformly blank frame of `width × height` RGBA for tests.
pub fn empty_frame(width: u32, height: u32) -> Frame {
    Frame {
        width,
        height,
        bytes: vec![0u8; (width * height * 4) as usize],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Error callback used by the loop tests (records messages).
    type ErrorSink = Arc<dyn Fn(&str) + Send + Sync>;

    type Ctx = (
        Arc<BoundedDropOldest<Vec<u8>>>,
        Arc<AtomicBool>,
        Arc<Mutex<Option<(u32, u32)>>>,
        Arc<Mutex<Vec<String>>>,
    );

    fn context() -> Ctx {
        (
            Arc::new(BoundedDropOldest::new(4)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Vec::new())),
        )
    }

    fn record_errors(slot: Arc<Mutex<Vec<String>>>) -> ErrorSink {
        Arc::new(move |message: &str| {
            slot.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(message.to_string());
        })
    }

    #[test]
    fn wayland_session_detection_is_pure() {
        // The env probes only apply on Linux; on every other platform the
        // session can never be Wayland (`wayland_for` short-circuits).
        if !cfg!(target_os = "linux") {
            assert!(!wayland_for(Some(OsStr::new("wayland")), None));
            assert!(!wayland_for(None, Some(OsStr::new("wayland-0"))));
            return;
        }
        assert!(!wayland_for(None, None));
        assert!(wayland_for(Some(OsStr::new("wayland")), None));
        assert!(wayland_for(
            Some(OsStr::new("wayland")),
            Some(OsStr::new("wayland-0"))
        ));
        assert!(wayland_for(None, Some(OsStr::new("wayland-0"))));
        assert!(!wayland_for(Some(OsStr::new("x11")), None));
        // WAYLAND_DISPLAY set ⇒ Wayland compositor session (possibly with
        // XWayland): the Display source is disabled regardless.
        assert!(wayland_for(
            Some(OsStr::new("x11")),
            Some(OsStr::new("wayland-0"))
        ));
        assert!(!wayland_for(Some(OsStr::new("Wayland")), None));
    }

    #[test]
    fn pushes_frames_and_reports_initial_resolution() {
        let (frames, stop, resolution, errors) = context();
        let source = ScriptedSource::new(vec![
            Ok(empty_frame(10, 10)),
            Ok(empty_frame(10, 10)),
            Ok(empty_frame(10, 10)),
        ]);
        run_capture(
            source,
            Arc::clone(&frames),
            stop,
            Arc::clone(&resolution),
            record_errors(errors),
            Shutdown::new(),
        );
        assert_eq!(frames.len(), 3);
        assert_eq!(*resolution.lock().unwrap(), Some((10, 10)));
    }

    #[test]
    fn resolution_change_is_signaled_once_per_size() {
        let (frames, stop, resolution, errors) = context();
        let source = ScriptedSource::new(vec![
            Ok(empty_frame(10, 10)),
            Ok(empty_frame(20, 30)),
            Ok(empty_frame(20, 30)),
            Ok(empty_frame(10, 10)),
        ]);
        run_capture(
            source,
            Arc::clone(&frames),
            stop,
            Arc::clone(&resolution),
            record_errors(errors),
            Shutdown::new(),
        );
        // Last write wins; the change is only signaled when the size changes.
        assert_eq!(*resolution.lock().unwrap(), Some((10, 10)));
        assert_eq!(frames.len(), 4);
    }

    #[test]
    fn stop_flag_ends_the_loop() {
        let (frames, stop, resolution, errors) = context();
        stop.store(true, Ordering::Relaxed);
        let source = ScriptedSource::new(vec![Ok(empty_frame(4, 4)); 10]);
        run_capture(
            source,
            Arc::clone(&frames),
            stop,
            Arc::clone(&resolution),
            record_errors(errors),
            Shutdown::new(),
        );
        assert_eq!(frames.len(), 0, "stop must prevent any capture");
    }

    #[test]
    fn frames_are_dropped_oldest_first_under_overflow() {
        let (frames, stop, resolution, errors) = context();
        let source = ScriptedSource::new(vec![
            Ok(empty_frame(1, 1)),
            Ok(empty_frame(1, 1)),
            Ok(empty_frame(1, 1)),
            Ok(empty_frame(1, 1)),
            Ok(empty_frame(1, 1)),
            Ok(empty_frame(1, 1)),
        ]);
        run_capture(
            source,
            Arc::clone(&frames),
            stop,
            Arc::clone(&resolution),
            record_errors(errors),
            Shutdown::new(),
        );
        assert_eq!(frames.len(), 4, "queue caps at its capacity");
    }

    #[test]
    fn failure_counter_stops_the_loop_and_reports() {
        let (frames, stop, resolution, errors) = context();
        let source = ScriptedSource::new(vec![
            Err("boom 1".into()),
            Err("boom 2".into()),
            Err("boom 3".into()),
            Err("boom 4".into()),
            Err("boom 5".into()),
            Ok(empty_frame(2, 2)),
        ]);
        run_capture(
            source,
            Arc::clone(&frames),
            stop,
            Arc::clone(&resolution),
            record_errors(Arc::clone(&errors)),
            Shutdown::new(),
        );
        let reported = errors.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(reported.len(), 1);
        assert!(reported[0].contains("failed 5 times"));
        assert_eq!(frames.len(), 0);
    }

    #[test]
    fn transient_errors_do_not_stop_the_loop() {
        let (frames, stop, resolution, errors) = context();
        let source = ScriptedSource::new(vec![
            Err("glitch".into()),
            Ok(empty_frame(2, 2)),
            Err("glitch".into()),
            Ok(empty_frame(2, 2)),
        ]);
        run_capture(
            source,
            Arc::clone(&frames),
            stop,
            Arc::clone(&resolution),
            record_errors(errors),
            Shutdown::new(),
        );
        assert_eq!(frames.len(), 2);
    }

    #[test]
    fn reacquire_failures_are_not_masked_by_successful_captures() {
        // Larger queue than `context()` so the frame count stays observable.
        let frames = Arc::new(BoundedDropOldest::new(500));
        let stop = Arc::new(AtomicBool::new(false));
        let resolution = Arc::new(Mutex::new(None));
        let errors = Arc::new(Mutex::new(Vec::new()));
        // Every capture succeeds; only the periodic re-acquire fails.
        let source = ScriptedSource::new(vec![Ok(empty_frame(2, 2)); 200])
            .with_reacquire(Err("monitor gone".into()));
        run_capture(
            source,
            Arc::clone(&frames),
            Arc::clone(&stop),
            Arc::clone(&resolution),
            record_errors(Arc::clone(&errors)),
            Shutdown::new(),
        );
        // One re-acquire failure per MONITOR_REACQUIRE_INTERVAL frames: five
        // consecutive failures stop the loop even though capture always won.
        let reported = errors.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(reported.len(), 1);
        assert!(reported[0].contains("monitor lost"));
        assert_eq!(
            frames.len(),
            MAX_CONSECUTIVE_FAILURES as usize * MONITOR_REACQUIRE_INTERVAL as usize
        );
    }

    #[test]
    fn reacquire_failures_are_reset_on_reacquire_success() {
        // Larger queue than `context()` so the frame count stays observable.
        let frames = Arc::new(BoundedDropOldest::new(500));
        let stop = Arc::new(AtomicBool::new(false));
        let resolution = Arc::new(Mutex::new(None));
        let errors = Arc::new(Mutex::new(Vec::new()));
        // Failing re-acquires interleaved with a success never reach five in
        // a row, so the loop must keep running.
        let source =
            ScriptedSource::new(vec![Ok(empty_frame(2, 2)); 200]).with_reacquire_script(vec![
                Err("flaky".into()),
                Err("flaky".into()),
                Err("flaky".into()),
                Ok(()),
            ]);
        run_capture(
            source,
            Arc::clone(&frames),
            Arc::clone(&stop),
            Arc::clone(&resolution),
            record_errors(Arc::clone(&errors)),
            Shutdown::new(),
        );
        // Script exhausted: the final capture failure trips the counter.
        let reported = errors.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(reported.len(), 1);
        assert!(reported[0].contains("failed 5 times"));
        assert_eq!(frames.len(), 200);
    }
}
