// SPDX-License-Identifier: MIT OR Apache-2.0
//! Full Cast connection lifecycle (`03-cast-engine.md` §7): the
//! `Disconnected → Connecting → Connected → Launching → Ready → Streaming →
//! Teardown` state machine, heartbeat watchdog, reconnect backoff, inbound
//! JSON routing, and ordered teardown. All blocking TLS I/O runs on a
//! dedicated reader thread or `spawn_blocking` workers, never on the tokio
//! executor (Phase 3 lesson).
//!
//! The implementation is split across sub-modules for reviewability:
//! [`transport`] (trait + connector), [`reader`] (frame accumulation +
//! reader thread), [`writer`] (framed writes), [`state_machine`] (phases,
//! commands, run loop, reconnect policy) and [`teardown`] (ordered
//! shutdown). This module exposes the full public API and the [`CastConnection`]
//! handle.

mod reader;
mod state_machine;
mod teardown;
mod transport;
mod writer;

pub use state_machine::{Command, ConnectionConfig, ConnectionError, ConnectionEvent, Phase, run};
pub use transport::{
    Connector, READ_POLL_INTERVAL, SharedTransport, TlsConnector, Transport, WRITE_TIMEOUT,
};

use tokio::sync::mpsc;

use crate::cast::namespaces::StreamType;
use crate::cast::tls::TlsError;
use crate::cast::tofu::PinCheck;
use crate::state::CastDevice;
use crate::util::shutdown::Shutdown;

// ---------------------------------------------------------------------------
// Public facade
// ---------------------------------------------------------------------------

/// Handle for driving one connection task. All methods are non-blocking:
/// they enqueue a [`Command`] and return immediately.
#[derive(Debug, Clone)]
pub struct CastConnection {
    commands: mpsc::UnboundedSender<Command>,
}

impl CastConnection {
    /// Spawn the connection task on the current tokio runtime with default
    /// timers ([`ConnectionConfig::default`]). The connector uses an
    /// in-memory TOFU store; the backend-facing path constructs
    /// [`TlsConnector::new`] with the persisted store (see
    /// [`crate::cast::tofu::TofuStore::load_default`]).
    pub fn start(events: mpsc::UnboundedSender<ConnectionEvent>, shutdown: Shutdown) -> Self {
        Self::start_with_handle(events, shutdown, TlsConnector::default()).0
    }

    /// Spawn the connection task with an explicit connector, returning the
    /// task handle so the runtime supervisor can await teardown completion.
    pub fn start_with_handle<C: Connector>(
        events: mpsc::UnboundedSender<ConnectionEvent>,
        shutdown: Shutdown,
        connector: C,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (commands, receiver) = mpsc::unbounded_channel();
        let handle = tokio::spawn(run(
            receiver,
            events,
            shutdown,
            connector,
            ConnectionConfig::default(),
        ));
        (Self { commands }, handle)
    }

    /// Select (or re-select) a receiver; tears down any current session.
    pub fn select(&self, device: CastDevice) {
        let _ = self.commands.send(Command::Select(device));
    }

    /// `LAUNCH` the Default Media Receiver (FR-009).
    pub fn launch_default_receiver(&self) {
        let _ = self.commands.send(Command::LaunchDefaultReceiver);
    }

    /// `LOAD` a media URL (FR-020). If the session has not been launched
    /// yet, it is launched first and the `LOAD` is queued.
    ///
    /// `content_type` is required by the `LOAD` message
    /// (`03-cast-engine.md` §6.4) and comes from the media MIME map
    /// (Phase 7).
    pub fn load(&self, content_id: &str, content_type: &str, stream_type: StreamType) {
        let _ = self.commands.send(Command::Load {
            content_id: content_id.to_string(),
            content_type: content_type.to_string(),
            stream_type,
        });
    }

    /// `PLAY` (FR-018).
    pub fn play(&self) {
        let _ = self.commands.send(Command::Play);
    }

    /// `PAUSE` (FR-018).
    pub fn pause(&self) {
        let _ = self.commands.send(Command::Pause);
    }

    /// Media `STOP` (FR-018).
    pub fn stop(&self) {
        let _ = self.commands.send(Command::Stop);
    }

    /// `SET_VOLUME` (FR-018).
    pub fn set_volume(&self, level: f32, muted: bool) {
        let _ = self.commands.send(Command::SetVolume { level, muted });
    }

    /// `GET_STATUS` (`03-cast-engine.md` §6.3): request a fresh
    /// `RECEIVER_STATUS` snapshot on demand — e.g. after an external volume
    /// change, another client starting/stopping the Default Media Receiver,
    /// or a reconnect where no unsolicited status has arrived yet. The
    /// response flows back as `Volume`/`Ready` events.
    pub fn get_status(&self) {
        let _ = self.commands.send(Command::GetStatus);
    }

    /// Full teardown (`STOP → STOP_APP → close_notify`) and stop the
    /// connection task.
    pub fn shutdown(&self) {
        let _ = self.commands.send(Command::Shutdown);
    }
}

/// Test double plumbing shared by the in-module gate tests and
/// `tests/connection_tests.rs`. Test-only: integration tests link the
/// library without `cfg(test)`, so this module is always compiled (it is
/// dead weight in release builds, never referenced by production code).
#[doc(hidden)]
pub mod test_support {
    use super::{CastDevice, Connector, PinCheck, SharedTransport, TlsError, Transport};
    use std::io::{self, Read, Write};
    use std::sync::{Arc, Condvar, Mutex, MutexGuard};
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct MockState {
        incoming: Vec<u8>,
        outgoing: Vec<u8>,
        closed: bool,
    }

    struct MockCore {
        state: Mutex<MockState>,
        incoming: Condvar,
        outgoing: Condvar,
    }

    /// Byte pipe shared between a `MockTransport` (reader/writer side) and
    /// the test (control side).
    #[derive(Clone)]
    pub struct MockPipe {
        core: Arc<MockCore>,
    }

    impl MockPipe {
        pub fn new() -> Self {
            Self {
                core: Arc::new(MockCore {
                    state: Mutex::new(MockState::default()),
                    incoming: Condvar::new(),
                    outgoing: Condvar::new(),
                }),
            }
        }

        fn lock(&self) -> MutexGuard<'_, MockState> {
            match self.core.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }

        /// Feed bytes to the transport's reader.
        pub fn push_incoming(&self, bytes: &[u8]) {
            let mut state = self.lock();
            state.incoming.extend_from_slice(bytes);
            self.core.incoming.notify_all();
        }

        /// Block up to `timeout` for the transport to write something;
        /// returns and clears whatever is buffered (possibly empty on
        /// timeout/close).
        pub fn wait_outgoing(&self, timeout: Duration) -> Vec<u8> {
            let deadline = Instant::now() + timeout;
            let mut state = self.lock();
            loop {
                if !state.outgoing.is_empty() || state.closed {
                    return std::mem::take(&mut state.outgoing);
                }
                let now = Instant::now();
                if now >= deadline {
                    return std::mem::take(&mut state.outgoing);
                }
                let (new_state, timed_out) =
                    match self.core.outgoing.wait_timeout(state, deadline - now) {
                        Ok(result) => result,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                state = new_state;
                if timed_out.timed_out() {
                    return std::mem::take(&mut state.outgoing);
                }
            }
        }

        /// Non-blocking take of everything the transport has written so far.
        pub fn take_outgoing(&self) -> Vec<u8> {
            std::mem::take(&mut self.lock().outgoing)
        }

        /// Re-inject bytes that were already taken out of the outgoing buffer
        /// (used by poll helpers that must not consume frames after the one
        /// they matched); they are prepended ahead of any newer bytes.
        pub fn push_outgoing(&self, bytes: &[u8]) {
            let mut state = self.lock();
            let mut combined = bytes.to_vec();
            combined.extend_from_slice(&state.outgoing);
            state.outgoing = combined;
            self.core.outgoing.notify_all();
        }

        /// Simulate socket shutdown; unblocks the transport's reader with
        /// clean EOF.
        pub fn close(&self) {
            let mut state = self.lock();
            state.closed = true;
            self.core.incoming.notify_all();
            self.core.outgoing.notify_all();
        }

        pub fn is_closed(&self) -> bool {
            self.lock().closed
        }
    }

    impl Default for MockPipe {
        fn default() -> Self {
            Self::new()
        }
    }

    /// `Read`/`Write`/`Transport` impl over a `MockPipe`. Reads block until
    /// data arrives or the pipe closes (mirrors blocking socket semantics).
    pub struct MockTransport {
        pipe: MockPipe,
    }

    impl Read for MockTransport {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            // Mirror the real socket's blocking-socket-with-timeout
            // semantics: wake on data, clean EOF on close, otherwise a
            // WouldBlock after a short poll interval (so the transport
            // mutex is never held indefinitely, exactly like the real
            // READ_POLL_INTERVAL).
            const POLL_INTERVAL: Duration = Duration::from_millis(100);
            let mut state = self.pipe.lock();
            loop {
                if !state.incoming.is_empty() {
                    let n = state.incoming.len().min(buf.len());
                    buf[..n].copy_from_slice(&state.incoming[..n]);
                    state.incoming.drain(..n);
                    return Ok(n);
                }
                if state.closed {
                    return Ok(0);
                }
                let (new_state, timed_out) =
                    match self.pipe.core.incoming.wait_timeout(state, POLL_INTERVAL) {
                        Ok(result) => result,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                state = new_state;
                if timed_out.timed_out() {
                    return Err(io::Error::new(io::ErrorKind::WouldBlock, "no data yet"));
                }
            }
        }
    }

    impl Write for MockTransport {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut state = self.pipe.lock();
            state.outgoing.extend_from_slice(buf);
            self.pipe.core.outgoing.notify_all();
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Transport for MockTransport {
        fn close(&mut self) {
            self.pipe.close();
        }

        fn shutdown(&self) {
            self.pipe.close();
        }
    }

    #[derive(Default)]
    struct MockConnectorState {
        fail_connections: usize,
        pipes: Vec<MockPipe>,
    }

    /// Succeeds by handing out a fresh `MockPipe`; can be armed to fail the
    /// next `n` connect attempts (for reconnect/backoff tests).
    #[derive(Clone, Default)]
    pub struct MockConnector {
        state: Arc<Mutex<MockConnectorState>>,
    }

    impl MockConnector {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn fail_next(&self, n: usize) {
            self.state
                .lock()
                .expect("no poisoned mock state")
                .fail_connections = n;
        }

        pub fn pipes(&self) -> Vec<MockPipe> {
            self.state
                .lock()
                .expect("no poisoned mock state")
                .pipes
                .clone()
        }

        pub fn last_pipe(&self) -> Option<MockPipe> {
            self.state
                .lock()
                .expect("no poisoned mock state")
                .pipes
                .last()
                .cloned()
        }
    }

    impl Connector for MockConnector {
        async fn connect(
            &self,
            _device: &CastDevice,
        ) -> Result<(SharedTransport, PinCheck), TlsError> {
            let mut state = self.state.lock().expect("no poisoned mock state");
            if state.fail_connections > 0 {
                state.fail_connections -= 1;
                return Err(TlsError::Connect {
                    addr: _device.addr,
                    source: io::Error::new(io::ErrorKind::ConnectionRefused, "mock: refused"),
                });
            }
            let pipe = MockPipe::new();
            state.pipes.push(pipe.clone());
            // No TLS in the mock: no certificate, no pin check.
            Ok((
                Arc::new(Mutex::new(MockTransport { pipe })),
                PinCheck::Disabled,
            ))
        }
    }
}
