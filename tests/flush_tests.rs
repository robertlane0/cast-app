// SPDX-License-Identifier: MIT OR Apache-2.0
//! Unit tests for the bounded flush cadence (`04-media-proxy.md` §2):
//! byte-threshold and time-threshold triggering, reset after a flush, and
//! no premature flush below both thresholds.
//! Gate: `cargo test --test flush_tests`.

#![forbid(unsafe_code)]

use std::time::Duration;

use cast_app::media::flush::FlushTracker;

#[test]
fn no_flush_below_both_thresholds() {
    let mut tracker = FlushTracker::new(100, Duration::from_secs(60));
    assert!(!tracker.should_flush(40));
    assert!(!tracker.should_flush(59));
}

#[test]
fn byte_threshold_triggers_and_resets() {
    let mut tracker = FlushTracker::new(100, Duration::from_secs(60));
    assert!(!tracker.should_flush(60));
    assert!(
        tracker.should_flush(40),
        "crossing the byte threshold flushes"
    );
    assert!(
        !tracker.should_flush(60),
        "accounting must reset after a flush"
    );
    assert!(tracker.should_flush(100), "next crossing flushes again");
}

#[test]
fn oversized_chunk_flushes_immediately() {
    let mut tracker = FlushTracker::new(100, Duration::from_secs(60));
    assert!(tracker.should_flush(10_000));
}

#[test]
fn time_threshold_triggers_even_without_bytes() {
    let mut tracker = FlushTracker::new(usize::MAX, Duration::from_millis(5));
    assert!(!tracker.should_flush(1));
    std::thread::sleep(Duration::from_millis(20));
    assert!(
        tracker.should_flush(0),
        "elapsed time must flush even with no new bytes"
    );
}

#[test]
fn time_threshold_resets_after_flush() {
    let mut tracker = FlushTracker::new(usize::MAX, Duration::from_millis(5));
    assert!(!tracker.should_flush(1));
    std::thread::sleep(Duration::from_millis(20));
    assert!(tracker.should_flush(0));
    assert!(
        !tracker.should_flush(1),
        "timer must restart after a time-triggered flush"
    );
}

#[test]
fn zero_byte_writes_do_not_accumulate_toward_the_byte_threshold() {
    let mut tracker = FlushTracker::new(100, Duration::from_secs(60));
    assert!(!tracker.should_flush(0));
    assert!(!tracker.should_flush(99));
    assert!(tracker.should_flush(1));
}

#[test]
fn next_deadline_extends_interval_into_the_future() {
    let interval = Duration::from_millis(500);
    let tracker = FlushTracker::new(100, interval);
    let deadline = tracker.next_deadline();
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    assert!(
        remaining <= interval && remaining > interval - Duration::from_millis(50),
        "deadline must be ~one interval ahead, got {remaining:?}"
    );
}

#[test]
fn has_pending_tracks_unflushed_bytes() {
    let mut tracker = FlushTracker::new(100, Duration::from_secs(60));
    assert!(!tracker.has_pending());
    assert!(!tracker.should_flush(30));
    assert!(tracker.has_pending(), "unflushed bytes are pending");
    tracker.reset();
    assert!(!tracker.has_pending(), "reset clears the pending bytes");
    assert!(!tracker.should_flush(1));
    tracker.reset();
    assert!(!tracker.has_pending());
}
