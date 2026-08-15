//! Bounded channel with drop-oldest semantics for backpressure.
//! Owned by `06-concurrency.md` §4 and `05-screen-capture.md`.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::mpsc;

/// A bounded FIFO queue that, on overflow, drops the oldest buffered item so
/// the newest items are always kept (drop-oldest backpressure).
///
/// Built on `tokio::sync::mpsc::channel`: producers push with `try_send` and,
/// on a full channel, one buffered item is drained before retrying
/// (`05-screen-capture.md` bridge channels). Producer and consumer share the
/// same handle, typically behind an `Arc`.
#[derive(Clone)]
pub struct BoundedDropOldest<T> {
    tx: mpsc::Sender<T>,
    rx: Arc<Mutex<mpsc::Receiver<T>>>,
    capacity: usize,
}

impl<T> fmt::Debug for BoundedDropOldest<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedDropOldest")
            .field("capacity", &self.capacity)
            .field("len", &self.len())
            .finish()
    }
}

impl<T> BoundedDropOldest<T> {
    /// Create a queue that buffers at most `capacity` items.
    ///
    /// Panics if `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be at least 1");
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
            capacity,
        }
    }

    /// Push an item, dropping the oldest buffered item if the queue is full.
    ///
    /// The newest items are always retained. No-op once the consumer side has
    /// been dropped (the item is discarded).
    pub fn push(&self, item: T) {
        let mut item = item;
        loop {
            match self.tx.try_send(item) {
                Ok(()) => return,
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    item = returned;
                    // Drop the oldest buffered item to make room.
                    let _ = self.lock_rx().try_recv();
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return,
            }
        }
    }

    /// Non-blocking pop of the oldest item, or `None` if the queue is empty.
    pub fn try_pop(&self) -> Option<T> {
        self.lock_rx().try_recv().ok()
    }

    /// Number of items currently buffered.
    pub fn len(&self) -> usize {
        self.lock_rx().len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Maximum number of buffered items.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn lock_rx(&self) -> MutexGuard<'_, mpsc::Receiver<T>> {
        self.rx.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_order_preserved_without_overflow() {
        let queue = BoundedDropOldest::new(4);
        for i in 1..=4 {
            queue.push(i);
        }
        assert_eq!(queue.len(), 4);
        for i in 1..=4 {
            assert_eq!(queue.try_pop(), Some(i));
        }
    }

    #[test]
    fn drops_oldest_when_full() {
        let queue = BoundedDropOldest::new(2);
        queue.push(1);
        queue.push(2);
        queue.push(3);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.try_pop(), Some(2));
        assert_eq!(queue.try_pop(), Some(3));
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn keeps_only_newest_items() {
        let queue = BoundedDropOldest::new(2);
        for i in 0..5 {
            queue.push(i);
        }
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.try_pop(), Some(3));
        assert_eq!(queue.try_pop(), Some(4));
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn empty_queue_pops_none() {
        let queue = BoundedDropOldest::<u32>::new(3);
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.try_pop(), None);
    }

    #[test]
    fn drop_oldest_under_continuous_load() {
        let queue = BoundedDropOldest::new(3);
        for i in 0..100 {
            queue.push(i);
        }
        assert_eq!(queue.try_pop(), Some(97));
        assert_eq!(queue.try_pop(), Some(98));
        assert_eq!(queue.try_pop(), Some(99));
    }

    #[test]
    fn shared_across_threads() {
        let queue = Arc::new(BoundedDropOldest::new(2));
        let producer = Arc::clone(&queue);
        let thread = std::thread::spawn(move || {
            for i in 0..100 {
                producer.push(i);
            }
        });
        thread.join().unwrap();
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.try_pop(), Some(98));
        assert_eq!(queue.try_pop(), Some(99));
    }

    #[test]
    fn reports_capacity_and_len() {
        let queue = BoundedDropOldest::new(5);
        assert_eq!(queue.capacity(), 5);
        queue.push("a");
        queue.push("b");
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.try_pop(), Some("a"));
        assert_eq!(queue.len(), 1);
    }

    #[test]
    #[should_panic(expected = "capacity must be at least 1")]
    fn zero_capacity_rejected() {
        let _ = BoundedDropOldest::<u8>::new(0);
    }
}
