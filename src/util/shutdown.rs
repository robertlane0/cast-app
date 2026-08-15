//! Cooperative shutdown token wrapping `tokio::sync::watch::<bool>`.
//! Owned by `06-concurrency.md` §5.

use tokio::sync::watch;

/// Shared shutdown signal observed by every task, the capture thread and the
/// `ffmpeg` bridge (`06-concurrency.md` §5).
#[derive(Debug, Clone)]
pub struct Shutdown {
    sender: watch::Sender<bool>,
    // Kept alive so `trigger` always has a live receiver and the value change
    // is never dropped, even if no task has subscribed yet.
    _receiver: watch::Receiver<bool>,
}

impl Shutdown {
    /// Create a new shutdown token in the running state.
    pub fn new() -> Self {
        let (sender, receiver) = watch::channel(false);
        Self {
            sender,
            _receiver: receiver,
        }
    }

    /// Subscribe to the shutdown signal.
    ///
    /// The returned receiver yields the current state immediately and `true`
    /// once [`Shutdown::trigger`] has been called.
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.sender.subscribe()
    }

    /// Whether shutdown has been triggered.
    pub fn is_shutting_down(&self) -> bool {
        *self.sender.borrow()
    }

    /// Trigger shutdown for all observers. Idempotent.
    pub fn trigger(&self) {
        let _ = self.sender.send(true);
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_running_and_triggers() {
        let shutdown = Shutdown::new();
        assert!(!shutdown.is_shutting_down());
        shutdown.trigger();
        assert!(shutdown.is_shutting_down());
    }

    #[test]
    fn trigger_is_idempotent() {
        let shutdown = Shutdown::new();
        shutdown.trigger();
        shutdown.trigger();
        assert!(shutdown.is_shutting_down());
    }

    #[test]
    fn clones_share_the_signal() {
        let first = Shutdown::new();
        let second = first.clone();
        first.trigger();
        assert!(second.is_shutting_down());
        assert!(first.is_shutting_down());
    }

    #[tokio::test]
    async fn subscribers_observe_trigger() {
        let shutdown = Shutdown::new();
        let mut first = shutdown.subscribe();
        let mut second = shutdown.subscribe();
        assert!(!*first.borrow());

        shutdown.trigger();

        // Both receivers observe the change event.
        assert!(first.changed().await.is_ok());
        assert!(*first.borrow_and_update());
        assert!(second.changed().await.is_ok());
        assert!(*second.borrow());
    }

    #[test]
    fn receiver_yields_initial_state() {
        let shutdown = Shutdown::new();
        let receiver = shutdown.subscribe();
        assert!(!*receiver.borrow());
    }
}
