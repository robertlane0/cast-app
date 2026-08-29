// SPDX-License-Identifier: MIT OR Apache-2.0
//! Transport abstraction (`03-cast-engine.md` §3, §7): the `Transport`
//! trait, the shared mutex wrapper, the `Connector` trait and the real TLS
//! connector with read/write timeouts.

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, MutexGuard};

use crate::cast::tls::{self, CastTlsStream, TlsError};
use crate::cast::tofu::{Fingerprint, PinCheck, TofuStore, fingerprint_to_hex};
use crate::state::CastDevice;

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
///
/// `parking_lot::Mutex` is used instead of `std::sync::Mutex` because the
/// latter is not fair: a reader thread that re-locks microseconds after
/// releasing can starve a queued writer indefinitely (mutex barging). The
/// parking_lot mutex queues waiters FIFO so a blocked writer always acquires
/// before the reader's next re-lock, removing the need for the former
/// `thread::sleep` workaround in the reader.
pub type SharedTransport = Arc<Mutex<dyn Transport>>;

/// Lock the shared transport. `parking_lot::Mutex` never poisons.
pub(super) fn lock_transport(transport: &Mutex<dyn Transport>) -> MutexGuard<'_, dyn Transport> {
    transport.lock()
}

/// Establishes the TLS transport to a receiver. Pluggable so tests can
/// substitute a mock transport.
///
/// The returned future is `Send` so `run` (and therefore the whole cast
/// task) can be spawned with `tokio::spawn` from a generic context; every
/// implementation in this crate (real and mock) satisfies this.
pub trait Connector: Send + Sync + 'static {
    /// Connect to the device and return a shared transport plus the outcome
    /// of the TOFU certificate-pin check (`03-cast-engine.md` §3.1). The
    /// reader thread and the `spawn_blocking` writers lock the transport for
    /// each I/O operation.
    fn connect(
        &self,
        device: &CastDevice,
    ) -> impl std::future::Future<Output = Result<(SharedTransport, PinCheck), TlsError>> + Send;
}

/// Real connector: TCP + rustls handshake via [`tls::connect`]
/// (`03-cast-engine.md` §3), plus read/write timeouts that bound reader lock
/// holds and teardown writes, plus the TOFU certificate-pin check against
/// the receiver's key.
pub struct TlsConnector {
    store: Arc<TofuStore>,
}

impl TlsConnector {
    /// A connector that pins receiver certificates in `store`
    /// (`03-cast-engine.md` §3.1).
    pub fn new(store: Arc<TofuStore>) -> Self {
        Self { store }
    }
}

impl Default for TlsConnector {
    fn default() -> Self {
        Self::new(Arc::new(TofuStore::in_memory()))
    }
}

impl Connector for TlsConnector {
    async fn connect(&self, device: &CastDevice) -> Result<(SharedTransport, PinCheck), TlsError> {
        let stream = tls::connect(device.addr).await?;
        stream
            .sock
            .set_read_timeout(Some(READ_POLL_INTERVAL))
            .map_err(TlsError::from)?;
        stream
            .sock
            .set_write_timeout(Some(WRITE_TIMEOUT))
            .map_err(TlsError::from)?;
        let pin = match tls::peer_fingerprint(&stream) {
            Some(fingerprint) => self.record_pin(device, fingerprint),
            None => {
                tracing::warn!(
                    device = %device.name,
                    addr = %device.addr,
                    "receiver presented no certificate; no TOFU pin recorded"
                );
                PinCheck::Disabled
            }
        };
        Ok((Arc::new(Mutex::new(stream)), pin))
    }
}

impl TlsConnector {
    /// Compare the presented fingerprint against the store under the
    /// device's TOFU key, log the outcome, and return it for surfacing.
    /// A mismatch never blocks the connection (`03-cast-engine.md` §3.1).
    fn record_pin(&self, device: &CastDevice, fingerprint: Fingerprint) -> PinCheck {
        let check = self.store.check(&device.tofu_key, fingerprint);
        match check {
            PinCheck::Pinned => {
                tracing::info!(
                    device = %device.name,
                    addr = %device.addr,
                    key = %device.tofu_key,
                    fingerprint = %fingerprint_to_hex(&fingerprint),
                    "first-seen receiver certificate pinned (TOFU)"
                );
            }
            PinCheck::Matched => {
                tracing::debug!(
                    device = %device.name,
                    addr = %device.addr,
                    "receiver certificate matches the stored TOFU pin"
                );
            }
            PinCheck::Mismatch { previous, current } => {
                tracing::warn!(
                    device = %device.name,
                    addr = %device.addr,
                    key = %device.tofu_key,
                    previous = %fingerprint_to_hex(&previous),
                    current = %fingerprint_to_hex(&current),
                    "receiver certificate does NOT match the stored TOFU pin; \
                     the device may have been replaced or a man-in-the-middle \
                     is intercepting the connection — proceeding anyway"
                );
            }
            PinCheck::Disabled => {}
        }
        check
    }
}
