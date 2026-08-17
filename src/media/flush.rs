// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bounded flush cadence for streaming handlers (`04-media-proxy.md` §2).
//! A [`FlushTracker`] batches small writes until an accumulated-byte or
//! elapsed-time threshold is reached, instead of flushing after every chunk
//! (which defeats the `BufWriter` and burns a syscall per chunk). Callers
//! still flush the response head immediately so streaming starts promptly;
//! per-stream thresholds live next to the handlers that own them.

use std::time::{Duration, Instant};

/// Decide when a streaming handler should flush its buffered writer:
/// flush once `byte_threshold` bytes have accumulated since the last flush,
/// or `time_threshold` has elapsed, whichever comes first.
#[derive(Debug)]
pub struct FlushTracker {
    bytes_since_flush: usize,
    last_flush: Instant,
    byte_threshold: usize,
    time_threshold: Duration,
}

impl FlushTracker {
    /// Create a tracker that triggers a flush after `byte_threshold`
    /// accumulated bytes or `time_threshold` since the last flush.
    pub fn new(byte_threshold: usize, time_threshold: Duration) -> Self {
        Self {
            bytes_since_flush: 0,
            last_flush: Instant::now(),
            byte_threshold,
            time_threshold,
        }
    }

    /// Record `written` bytes and report whether the caller should flush now.
    /// The tracker resets its accounting on a `true` result, so the caller
    /// must flush exactly once when this returns `true`.
    pub fn should_flush(&mut self, written: usize) -> bool {
        self.bytes_since_flush += written;
        if self.bytes_since_flush >= self.byte_threshold
            || self.last_flush.elapsed() >= self.time_threshold
        {
            self.reset();
            true
        } else {
            false
        }
    }

    /// Deadline for the caller's wait-for-next-chunk sleep: when it elapses
    /// with no new bytes, the caller should [`reset`](Self::reset) and flush
    /// if [`has_pending`](Self::has_pending). Without this race the time
    /// threshold would never fire while a stream is idle, stalling the
    /// receiver until the next chunk arrives.
    pub fn next_deadline(&self) -> Instant {
        self.last_flush + self.time_threshold
    }

    /// True when bytes written since the last flush may still be buffered.
    pub fn has_pending(&self) -> bool {
        self.bytes_since_flush > 0
    }

    /// Restart the byte and time accounting after the caller flushed.
    pub fn reset(&mut self) {
        self.bytes_since_flush = 0;
        self.last_flush = Instant::now();
    }
}
