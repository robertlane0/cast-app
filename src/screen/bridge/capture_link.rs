// SPDX-License-Identifier: MIT OR Apache-2.0
//! Capture → encoder link (`05-screen-capture.md` §3, §5): the dedicated
//! capture thread (xcap path) and the cap-2 frame queue it pushes into, plus
//! the cloneable [`FrameFeeder`] handle for background feeders.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use tokio::sync::mpsc;

use crate::state::BackendEvent;
use crate::util::backpressure::BoundedDropOldest;
use crate::util::shutdown::Shutdown;

use super::report_once;

/// Capture → encoder frame queue capacity (AGENTS.md §7; drop-oldest).
pub const FRAME_QUEUE_CAPACITY: usize = 2;

/// The shared pipeline state the capture thread needs: the frame queue,
/// stop/failed flags, the resolution-request slot, the error reporter slot,
/// the event sink, and the shutdown token.
pub(super) struct CaptureContext {
    pub(super) monitor_name: String,
    pub(super) frames: Arc<BoundedDropOldest<Vec<u8>>>,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) resolution_request: Arc<Mutex<Option<(u32, u32)>>>,
    pub(super) failed: Arc<AtomicBool>,
    pub(super) last_error: Arc<Mutex<Option<String>>>,
    pub(super) events: mpsc::UnboundedSender<BackendEvent>,
    pub(super) shutdown: Shutdown,
}

/// Spawn the dedicated capture thread for the xcap path and hand back its
/// join handle. The thread pushes raw RGBA frames into the frame queue
/// (drop-oldest backpressure) at 30 fps and reports permanent failures
/// through the single-shot pipeline error reporter ([`super::report_once`]).
pub(super) fn spawn_capture_thread(ctx: CaptureContext) -> Result<JoinHandle<()>, String> {
    let CaptureContext {
        monitor_name,
        frames,
        stop,
        resolution_request,
        failed,
        last_error,
        events,
        shutdown,
    } = ctx;
    let mut capture = crate::screen::capture::start_capture(
        monitor_name,
        Arc::clone(&frames),
        Arc::clone(&stop),
        Arc::clone(&resolution_request),
        report_once(
            events,
            Arc::clone(&failed),
            Arc::clone(&stop),
            Arc::clone(&last_error),
        ),
        shutdown,
    )
    .map_err(|error| error.to_string())?;
    let handle = capture.join_handle();
    drop(capture);
    Ok(handle)
}

/// A cloneable handle that pushes frames into a running bridge (used by
/// tests and the runtime to feed the pipeline without a capture thread).
#[derive(Clone)]
pub struct FrameFeeder {
    frames: Arc<BoundedDropOldest<Vec<u8>>>,
}

impl FrameFeeder {
    pub(super) fn new(frames: Arc<BoundedDropOldest<Vec<u8>>>) -> Self {
        Self { frames }
    }

    /// Queue one raw RGBA frame (drop-oldest when the queue is full).
    pub fn push_frame(&self, bytes: Vec<u8>) {
        self.frames.push(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The module's usage pattern: the capture thread and a background
    /// feeder share one cap-2 queue, and overflow drops the oldest frame so
    /// the newest frames always win.
    #[test]
    fn feeder_and_capture_share_one_drop_oldest_queue() {
        let frames = Arc::new(BoundedDropOldest::new(FRAME_QUEUE_CAPACITY));
        let feeder = FrameFeeder::new(Arc::clone(&frames));
        for i in 0..5 {
            feeder.push_frame(vec![i as u8; 4]);
        }
        assert_eq!(frames.len(), FRAME_QUEUE_CAPACITY);
        assert_eq!(frames.try_pop(), Some(vec![3u8; 4]));
        assert_eq!(frames.try_pop(), Some(vec![4u8; 4]));
        assert!(frames.is_empty());
    }
}
