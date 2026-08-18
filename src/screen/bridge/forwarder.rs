// SPDX-License-Identifier: MIT OR Apache-2.0
//! Forwarder thread (`05-screen-capture.md` §6): moves output-queue
//! segments into the media server's live-stream channel and tears the
//! session down when the consumer closes (spec §4.2).
//!
//! Drop policy is media-aware: a fragment that does not fit is dropped as a
//! whole (a skipped play interval, never a truncated box); the init segment
//! is never dropped — it is retried until the consumer makes room, so a
//! restart always leads with a fresh, valid initialization.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::sync::mpsc;

use crate::screen::segments::EncodedSegment;
use crate::util::backpressure::BoundedDropOldest;

use super::IDLE_POLL;

/// Media-server live-stream channel capacity (forwarder transport), in
/// encoded segments.
pub(super) const SERVER_CHANNEL_CAPACITY: usize = 8;

/// Move one output-queue segment into the server's live-stream channel.
///
/// Returns `true` when the forwarder must stop: the consumer closed the
/// channel (client disconnect or source switch — the whole session is torn
/// down), or `stop` was requested while an init waited for room.
fn forward_segment(
    server_tx: &mpsc::Sender<Vec<u8>>,
    segment: EncodedSegment,
    stop: &AtomicBool,
    dropped: &AtomicUsize,
) -> bool {
    let is_init = matches!(segment, EncodedSegment::Init(_));
    let mut bytes = segment.into_bytes();
    loop {
        match server_tx.try_send(bytes) {
            Ok(()) => return false,
            Err(mpsc::error::TrySendError::Full(returned)) => {
                bytes = returned;
                if is_init {
                    // The init must reach the consumer: wait for the slow
                    // client to drain instead of dropping it (a fragment
                    // drop is a skip, an init drop is a corrupt stream).
                    tracing::debug!("waiting for the consumer to make room for the init segment");
                    std::thread::sleep(IDLE_POLL);
                } else {
                    // The server buffer is full (slow client): drop the
                    // whole fragment and accept a transient glitch (spec §6).
                    dropped.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        bytes = bytes.len(),
                        "dropping encoded fragment; consumer is slow"
                    );
                    return false;
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::info!("screen stream consumer closed; tearing down the encoder");
                stop.store(true, Ordering::Relaxed);
                return true;
            }
        }
        if stop.load(Ordering::Relaxed) {
            return true;
        }
    }
}

/// Move output-queue segments into the media server's live-stream channel.
/// When the consumer closes (client disconnect or source switch), the whole
/// session is torn down (spec §4.2).
pub(super) fn forwarder_loop(
    output: Arc<BoundedDropOldest<EncodedSegment>>,
    server_tx: mpsc::Sender<Vec<u8>>,
    stop: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
) {
    while !stop.load(Ordering::Relaxed) {
        if let Some(segment) = output.try_pop() {
            if forward_segment(&server_tx, segment, &stop, &dropped) {
                break;
            }
        }
        std::thread::sleep(IDLE_POLL);
    }
    tracing::debug!("forwarder stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    fn init(bytes: &[u8]) -> EncodedSegment {
        EncodedSegment::Init(bytes.to_vec())
    }

    fn fragment(bytes: &[u8]) -> EncodedSegment {
        EncodedSegment::Fragment(bytes.to_vec())
    }

    /// A fragment that does not fit is dropped as a whole (a skipped play
    /// interval), and the already-buffered bytes are untouched.
    #[test]
    fn full_server_buffer_drops_the_fragment_whole() {
        let (tx, mut rx) = mpsc::channel(1);
        let stop = AtomicBool::new(false);
        let dropped = AtomicUsize::new(0);
        tx.try_send(vec![0u8; 8])
            .expect("the test occupies the single slot");

        let should_stop = forward_segment(&tx, fragment(&[1u8; 4]), &stop, &dropped);

        assert!(!should_stop, "a dropped fragment is not a stop");
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert_eq!(
            rx.try_recv(),
            Ok(vec![0u8; 8]),
            "the buffered item is untouched"
        );
    }

    /// The init is never dropped: it is retried until the consumer makes
    /// room, then delivered.
    #[test]
    fn init_is_retried_until_the_consumer_makes_room() {
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(vec![0u8; 8])
            .expect("the test occupies the single slot");
        let stop = AtomicBool::new(false);
        let dropped = AtomicUsize::new(0);
        // A slow consumer drains the buffer shortly after the retry loop
        // starts; the init must then be delivered (2 ms retry cadence).
        let rx = Arc::new(Mutex::new(rx));
        let drainer_rx = Arc::clone(&rx);
        let drainer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drainer_rx.lock().unwrap().try_recv().ok()
        });

        let should_stop = forward_segment(&tx, init(&[3u8; 4]), &stop, &dropped);

        assert!(!should_stop);
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            0,
            "the init must never be dropped"
        );
        assert_eq!(drainer.join().unwrap(), Some(vec![0u8; 8]));
        assert_eq!(
            rx.lock().unwrap().try_recv(),
            Ok(vec![3u8; 4]),
            "the init must reach the consumer"
        );
    }

    /// The init retry loop gives up (and asks the forwarder to stop) when
    /// `stop` is requested while it waits for room.
    #[test]
    fn init_retry_stops_when_stop_is_requested() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(vec![0u8; 8])
            .expect("the test occupies the single slot");
        let stop = Arc::new(AtomicBool::new(false));
        let dropped = AtomicUsize::new(0);
        let stopper = Arc::clone(&stop);
        let stopper = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            stopper.store(true, Ordering::Relaxed);
        });

        let should_stop = forward_segment(&tx, init(&[4u8; 4]), &stop, &dropped);

        assert!(should_stop, "a stop request must end the retry loop");
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            0,
            "the init is never dropped"
        );
        stopper.join().unwrap();
        assert_eq!(
            rx.try_recv(),
            Ok(vec![0u8; 8]),
            "the buffered item is untouched"
        );
    }

    /// A closed consumer channel stops the forwarder and requests the
    /// session teardown.
    #[test]
    fn closed_consumer_stops_the_forwarder() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let stop = AtomicBool::new(false);
        let dropped = AtomicUsize::new(0);

        let should_stop = forward_segment(&tx, init(&[2u8; 4]), &stop, &dropped);

        assert!(should_stop, "a closed consumer must request teardown");
        assert!(stop.load(Ordering::Relaxed));
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }
}
