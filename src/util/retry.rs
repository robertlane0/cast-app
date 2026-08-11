#![forbid(unsafe_code)]

//! Exponential-backoff iterator for reconnect / retry policies.
//! Owned by `06-concurrency.md` §5 and `03-cast-engine.md` §7.

use std::time::Duration;

/// Default initial delay between attempts: 1 second.
pub const DEFAULT_INITIAL: Duration = Duration::from_secs(1);
/// Default maximum delay: 30 seconds.
pub const DEFAULT_CAP: Duration = Duration::from_secs(30);
/// Default maximum number of delays yielded: 5.
pub const DEFAULT_MAX: usize = 5;

/// An iterator yielding the sleep `Duration` to wait before each retry.
///
/// Default sequence: 1 s, 2 s, 4 s, 8 s, 16 s, then exhausted. Delays double
/// per step and are capped at [`DEFAULT_CAP`] for custom configurations.
#[derive(Debug, Clone)]
pub struct Backoff {
    next: Duration,
    cap: Duration,
    remaining: usize,
}

impl Backoff {
    /// Default backoff policy (1 s, 2 s, 4 s, 8 s, 16 s).
    pub fn new() -> Self {
        Self::with_params(DEFAULT_INITIAL, DEFAULT_CAP, DEFAULT_MAX)
    }

    /// Backoff with explicit initial delay, maximum delay and yield count.
    pub fn with_params(initial: Duration, cap: Duration, max: usize) -> Self {
        Self {
            next: initial,
            cap,
            remaining: max,
        }
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for Backoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.cap);
        Some(delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sequence_is_five_doubling_delays() {
        let delays: Vec<Duration> = Backoff::new().collect();
        assert_eq!(
            delays,
            vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(16),
            ]
        );
    }

    #[test]
    fn exhausted_iterator_stays_empty() {
        let mut backoff = Backoff::new();
        assert_eq!(backoff.next(), Some(Duration::from_secs(1)));
        assert_eq!(backoff.next(), Some(Duration::from_secs(2)));
        assert_eq!(backoff.next(), Some(Duration::from_secs(4)));
        assert_eq!(backoff.next(), Some(Duration::from_secs(8)));
        assert_eq!(backoff.next(), Some(Duration::from_secs(16)));
        assert_eq!(backoff.next(), None);
        assert_eq!(backoff.next(), None);
    }

    #[test]
    fn delays_cap_at_maximum() {
        let backoff = Backoff::with_params(Duration::from_secs(1), Duration::from_secs(3), 10);
        let delays: Vec<Duration> = backoff.collect();
        let mut expected = vec![Duration::from_secs(1), Duration::from_secs(2)];
        expected.extend(std::iter::repeat_n(Duration::from_secs(3), 8));
        assert_eq!(delays, expected);
    }

    #[test]
    fn zero_max_yields_nothing() {
        let delays: Vec<Duration> =
            Backoff::with_params(Duration::from_secs(1), Duration::from_secs(30), 0).collect();
        assert!(delays.is_empty());
    }

    #[test]
    fn never_exceeds_cap() {
        let backoff = Backoff::with_params(Duration::from_secs(1), Duration::from_secs(30), 20);
        let max: Duration = backoff.max().unwrap();
        assert!(max <= Duration::from_secs(30));
    }
}
