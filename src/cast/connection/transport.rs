// SPDX-License-Identifier: MIT OR Apache-2.0
//! Transport abstraction (`03-cast-engine.md` §3, §7): the `Transport`
//! trait, the shared mutex wrapper, the `Connector` trait and the real TLS
//! connector with read/write timeouts.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::cast::tls::{self, CastTlsStream, TlsError};

/// How long a socket read may block before the reader re-polls. Also bounds
/// the reader's lock hold time so writes and teardown always make progress.
pub const READ_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Write timeout applied to the socket so teardown cannot hang on a dead
/// peer that has stopped reading.
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// A blocking byte-stream transport with teardown support. The real
/// implementation is the rustls [`CastTlsStream`]; tests substitute an
/// in-memory duplex.
pub trait Transport: Read + Write + Send + 'static {
    /// Send `close_notify`, flush, then interrupt any blocked reader.
    /// Best effort — a dead peer must not block teardown.
    /// (`03-cast-engine.md` §7: teardown SHALL close with `close_notify`.)
    fn close(&mut self) {}

    /// Interrupt any blocked reader without a graceful close. Best effort.
    fn shutdown(&self) {}
}

impl Transport for CastTlsStream {
    fn close(&mut self) {
        self.conn.send_close_notify();
        let _ = self.flush();
        let _ = self.sock.shutdown(std::net::Shutdown::Both);
    }

    fn shutdown(&self) {
        let _ = self.sock.shutdown(std::net::Shutdown::Both);
    }
}

/// The transport shared between the reader thread and `spawn_blocking`
/// writers. Lock hold times are bounded by [`READ_POLL_INTERVAL`].
pub type SharedTransport = Arc<Mutex<dyn Transport>>;

/// Lock the shared transport, tolerating a poisoned mutex (the inner state
/// is still usable — a panicked lock holder never corrupted the socket).
pub(super) fn lock_transport(transport: &Mutex<dyn Transport>) -> MutexGuard<'_, dyn Transport> {
    match transport.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Establishes the TLS transport to a receiver address. Pluggable so tests
/// can substitute a mock transport.
///
/// The returned future is `Send` so `run` (and therefore the whole cast
/// task) can be spawned with `tokio::spawn` from a generic context; every
/// implementation in this crate (real and mock) satisfies this.
pub trait Connector: Send + Sync + 'static {
    /// Connect and return a shared transport. The reader thread and the
    /// `spawn_blocking` writers lock it for each I/O operation.
    fn connect(
        &self,
        addr: SocketAddr,
    ) -> impl std::future::Future<Output = Result<SharedTransport, TlsError>> + Send;
}

/// Real connector: TCP + rustls handshake via [`tls::connect`]
/// (`03-cast-engine.md` §3), plus read/write timeouts that bound reader lock
/// holds and teardown writes.
pub struct TlsConnector;

impl Connector for TlsConnector {
    async fn connect(&self, addr: SocketAddr) -> Result<SharedTransport, TlsError> {
        let stream = tls::connect(addr).await?;
        stream
            .sock
            .set_read_timeout(Some(READ_POLL_INTERVAL))
            .map_err(TlsError::from)?;
        stream
            .sock
            .set_write_timeout(Some(WRITE_TIMEOUT))
            .map_err(TlsError::from)?;
        Ok(Arc::new(Mutex::new(stream)))
    }
}
