// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bounded FIFO queue with drop-oldest semantics for backpressure.
//! Owned by `06-concurrency.md` §4 and `05-screen-capture.md`.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// A bounded FIFO queue that, on overflow, drops the oldest buffered item so
/// the newest items are always kept (drop-oldest backpressure).
///
/// Producers push with [`BoundedDropOldest::push`] (or
/// [`BoundedDropOldest::push_or`] when some buffered items must be protected
/// from eviction) and consumers poll with [`BoundedDropOldest::try_pop`];
/// both sides share one handle, typically behind an `Arc`. The queue never
/// blocks: overflow evicts rather than back-pressuring the producer, and
/// consumers poll. It is therefore safe to share between `std::thread`s
/// without a runtime (`05-screen-capture.md` bridge channels).
#[derive(Clone)]
pub struct BoundedDropOldest<T> {
    items: Arc<Mutex<VecDeque<T>>>,
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
        Self {
            items: Arc::new(Mutex::new(VecDeque::new())),
            capacity,
        }
    }

    /// Push an item, dropping the oldest buffered item if the queue is full.
    ///
    /// The newest items are always retained.
    pub fn push(&self, item: T) {
        let mut items = lock(&self.items);
        if items.len() >= self.capacity {
            items.pop_front();
        }
        items.push_back(item);
    }

    /// Push an item, but on a full queue evict only the oldest buffered item
    /// for which `evictable` returns true. Items for which `evictable` is
    /// false (e.g. a media stream's initialization segment, which every
    /// following item depends on) stay buffered.
    ///
    /// Returns the item to the caller when the queue is full and no
    /// buffered item is evictable; the caller must decide whether to drop it
    /// or retry later.
    pub fn push_or(&self, item: T, evictable: impl Fn(&T) -> bool) -> Option<T> {
        let mut items = lock(&self.items);
        if items.len() < self.capacity {
            items.push_back(item);
            return None;
        }
        if let Some(index) = items.iter().position(evictable) {
            items.remove(index);
            items.push_back(item);
            return None;
        }
        Some(item)
    }

    /// Drop every buffered item. Used when a stream generation ends so its
    /// stale bytes never reach the consumer.
    pub fn clear(&self) {
        lock(&self.items).clear();
    }

    /// Non-blocking pop of the oldest item, or `None` if the queue is empty.
    pub fn try_pop(&self) -> Option<T> {
        lock(&self.items).pop_front()
    }

    /// Number of items currently buffered.
    pub fn len(&self) -> usize {
        lock(&self.items).len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Maximum number of buffered items.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

fn lock<T>(items: &Mutex<T>) -> MutexGuard<'_, T> {
    items.lock().unwrap_or_else(PoisonError::into_inner)
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

    /// `push_or` skips protected items when evicting: a protected head (e.g.
    /// a stream init segment) survives overflow and stays first in FIFO
    /// order.
    #[test]
    fn push_or_evicts_only_evictable_items() {
        let queue = BoundedDropOldest::new(4);
        assert!(queue.push_or(0u32, |item| *item > 0).is_none());
        for i in 1..=4 {
            assert!(queue.push_or(i, |item| *item > 0).is_none());
        }
        // The queue is [0, 1, 2, 3] after pushing 4: 0 (protected) survived,
        // and the oldest evictable items were evicted as newer ones arrived.
        assert_eq!(queue.len(), 4);
        assert_eq!(queue.try_pop(), Some(0));
        assert_eq!(queue.try_pop(), Some(2));
        assert_eq!(queue.try_pop(), Some(3));
        assert_eq!(queue.try_pop(), Some(4));
    }

    /// When the queue is full of protected items only, `push_or` hands the
    /// new item back instead of evicting a protected one.
    #[test]
    fn push_or_returns_item_when_nothing_is_evictable() {
        let queue = BoundedDropOldest::new(3);
        for i in 0..3 {
            assert!(queue.push_or(i, |item| *item > 100).is_none());
        }
        assert_eq!(queue.push_or(99, |item| *item > 100), Some(99));
        // Nothing was evicted.
        assert_eq!(queue.len(), 3);
        assert_eq!(queue.try_pop(), Some(0));
        assert_eq!(queue.try_pop(), Some(1));
        assert_eq!(queue.try_pop(), Some(2));
    }

    /// `clear` drops every buffered item (generation switch).
    #[test]
    fn clear_empties_the_queue() {
        let queue = BoundedDropOldest::new(4);
        for i in 0..4 {
            queue.push(i);
        }
        queue.clear();
        assert!(queue.is_empty());
        assert_eq!(queue.try_pop(), None);
        // The queue remains usable after a clear.
        queue.push(7);
        assert_eq!(queue.try_pop(), Some(7));
    }
}
