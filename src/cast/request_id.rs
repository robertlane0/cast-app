//! Monotonic `requestId` allocation and the pending-request map with
//! per-entry timeouts. Owned by `03-cast-engine.md` §6.

use std::collections::HashMap;

use tokio::time::Instant;

/// Default response timeout for a pending request (`03-cast-engine.md` §6.0):
/// responses must arrive within 5 seconds or the request is considered
/// failed.
pub const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Allocates per-connection monotonic `u32` request IDs
/// (`03-cast-engine.md` §6.0). Wraps from `u32::MAX` back to 1 so ID 0 is
/// never handed out (0 signals "no request" in some receivers).
#[derive(Debug, Clone, Copy, Default)]
pub struct RequestId {
    next: u32,
}

impl RequestId {
    /// Start a fresh sequence; the first allocated ID is 1.
    pub fn new() -> Self {
        Self { next: 1 }
    }

    /// Allocate the next request ID.
    ///
    /// (FR-021) Every request SHALL carry a `requestId`; this counter is the
    /// per-connection source of those IDs.
    pub fn allocate(&mut self) -> u32 {
        let id = self.next;
        self.next = if self.next == u32::MAX {
            1
        } else {
            self.next + 1
        };
        id
    }
}

/// Map of outstanding requests keyed by `requestId`, recording when each was
/// sent so expired entries can be reaped after
/// [`DEFAULT_REQUEST_TIMEOUT`] (`03-cast-engine.md` §6.0).
///
/// All methods are synchronous; the connection task drives expiry from its
/// own loop using [`PendingMap::expire`].
#[derive(Debug, Clone)]
pub struct PendingMap {
    pending: HashMap<u32, Instant>,
    timeout: std::time::Duration,
}

impl PendingMap {
    /// Create an empty map with the given per-entry timeout.
    pub fn new(timeout: std::time::Duration) -> Self {
        Self {
            pending: HashMap::new(),
            timeout,
        }
    }

    /// Create an empty map with the spec's 5-second timeout.
    pub fn with_default_timeout() -> Self {
        Self::new(DEFAULT_REQUEST_TIMEOUT)
    }

    /// Register an outstanding request. Returns `false` (and leaves the
    /// existing deadline untouched) if `id` is already pending.
    ///
    /// (FR-021) Incoming responses are correlated to outstanding requests by
    /// `requestId`.
    pub fn insert(&mut self, id: u32, sent_at: Instant) -> bool {
        if self.pending.contains_key(&id) {
            return false;
        }
        self.pending.insert(id, sent_at);
        true
    }

    /// Resolve a response: remove and return `true` if `id` was pending,
    /// `false` for an uncorrelated (duplicate or unknown) `requestId`.
    ///
    /// (FR-021) Correlation hit/miss.
    pub fn resolve(&mut self, id: u32) -> bool {
        self.pending.remove(&id).is_some()
    }

    /// Whether `id` currently has an outstanding request.
    pub fn is_pending(&self, id: u32) -> bool {
        self.pending.contains_key(&id)
    }

    /// Remove and return every request whose deadline (sent time plus the
    /// map timeout) is at or before `now`. The connection layer calls this
    /// each loop iteration and treats returned IDs as failed
    /// (`03-cast-engine.md` §6.0: "considered failed and logged").
    pub fn expire(&mut self, now: Instant) -> Vec<u32> {
        let timeout = self.timeout;
        let mut expired = Vec::new();
        self.pending.retain(|&id, sent_at| {
            if now >= *sent_at + timeout {
                expired.push(id);
                false
            } else {
                true
            }
        });
        expired
    }

    /// Number of outstanding requests.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Whether no requests are outstanding.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// The per-entry response timeout.
    pub fn timeout(&self) -> std::time::Duration {
        self.timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn ids_are_monotonic_and_never_zero() {
        // (FR-021) Monotonic sequence starting at 1.
        let mut ids = RequestId::new();
        let mut previous = 0u32;
        for _ in 0..10_000 {
            let id = ids.allocate();
            assert_ne!(id, 0, "ID 0 is never allocated");
            assert!(id > previous || previous == u32::MAX, "monotonic increase");
            previous = id;
        }
    }

    #[test]
    fn sequence_wraps_at_u32_max() {
        let mut ids = RequestId { next: u32::MAX };
        assert_eq!(ids.allocate(), u32::MAX);
        assert_eq!(ids.allocate(), 1, "wraps to 1, skipping 0");
        assert_eq!(ids.allocate(), 2);
    }

    #[test]
    fn correlation_hit_and_miss() {
        // (FR-021) A pending ID resolves once; unknown and duplicate
        // requestIds are misses.
        let mut map = PendingMap::with_default_timeout();
        let now = Instant::now();
        assert!(map.insert(7, now), "first insert succeeds");
        assert!(!map.insert(7, now), "duplicate insert is rejected");

        assert!(map.is_pending(7));
        assert!(map.resolve(7), "correlation hit");
        assert!(!map.resolve(7), "second resolve is a miss");
        assert!(!map.resolve(99), "unknown requestId is a miss");
        assert!(!map.resolve(0), "requestId 0 is never pending");
        assert!(map.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_fires_after_five_seconds() {
        // (FR-021) A request is still pending just before the deadline and
        // expires once 5 s have elapsed.
        let mut map = PendingMap::with_default_timeout();
        let start = Instant::now();
        assert!(map.insert(1, start));
        assert!(map.insert(2, start));

        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(map.expire(Instant::now()).is_empty(), "not yet expired");

        tokio::time::advance(Duration::from_secs(1)).await;
        let mut expired = map.expire(Instant::now());
        expired.sort_unstable();
        assert_eq!(expired, vec![1, 2], "both requests expired");
        assert!(map.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_is_per_entry() {
        let mut map = PendingMap::with_default_timeout();
        let start = Instant::now();
        assert!(map.insert(1, start));
        assert!(map.insert(2, start + Duration::from_secs(3)));

        tokio::time::advance(Duration::from_secs(6)).await;
        let mut expired = map.expire(Instant::now());
        expired.sort_unstable();
        assert_eq!(expired, vec![1], "only the earlier request expired");
        assert!(map.is_pending(2));
        assert_eq!(map.len(), 1);

        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(map.expire(Instant::now()), vec![2]);
    }

    #[test]
    fn custom_timeout_is_used() {
        let map = PendingMap::new(Duration::from_millis(250));
        assert_eq!(map.timeout(), Duration::from_millis(250));
        assert_eq!(
            PendingMap::with_default_timeout().timeout(),
            Duration::from_secs(5)
        );
    }
}
