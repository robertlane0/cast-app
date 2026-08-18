// SPDX-License-Identifier: MIT OR Apache-2.0
//! rustls client configuration and a permissive certificate verifier for
//! self-signed Chromecast certificates. Owned by `03-cast-engine.md` §3.

use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ClientConnection, DigitallySignedStruct, SignatureScheme};
use sha2::Digest;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::cast::tofu::Fingerprint;

/// TLS handshake deadline (`03-cast-engine.md` §3.2): the handshake SHALL
/// complete within 5 seconds or the connection attempt SHALL fail.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Errors produced while establishing the TLS transport.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("TCP connect to {addr} failed: {source}")]
    Connect { addr: SocketAddr, source: io::Error },
    #[error("TCP connect to {addr} timed out after {timeout:?}")]
    ConnectTimeout { addr: SocketAddr, timeout: Duration },
    #[error("TLS handshake failed: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("TLS I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("TLS handshake timed out after {0:?}")]
    HandshakeTimeout(Duration),
    #[error("TLS handshake worker panicked")]
    WorkerPanicked,
}

/// A TLS stream over an established Cast connection. The socket is blocking;
/// the connection layer owns concurrency.
pub type CastTlsStream = rustls::StreamOwned<ClientConnection, std::net::TcpStream>;

/// Instrumentation gauge: the number of handshake workers currently running
/// on the blocking pool. Every attempt increments before spawn and the worker
/// decrements on exit (including panic, via [`InFlightGuard`]); the caller's
/// timeout path does not need to touch it. Kept private; in-module tests use
/// it to assert workers never accumulate.
static IN_FLIGHT_HANDSHAKES: AtomicUsize = AtomicUsize::new(0);

/// Decrements [`IN_FLIGHT_HANDSHAKES`] on drop so a panicking worker can
/// never leak the gauge.
struct InFlightGuard;

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        IN_FLIGHT_HANDSHAKES.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Install the `ring` crypto provider once per process. rustls 0.23 requires
/// an explicit provider; a second call returns an error, which is ignored.
///
/// Called by [`client_config`], so it is safe to invoke repeatedly and works
/// from tests. The `ring` feature is pinned in `Cargo.toml`; `native-tls` is
/// banned by `deny.toml`.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// A certificate verifier that completes the TLS handshake, including
/// verification of the server's handshake signature, but skips chain and
/// hostname trust evaluation (`03-cast-engine.md` §3.1) — Cast receivers use
/// self-signed certificates and no trust anchor exists. Trust is built via
/// trust-on-first-use pinning instead: see [`crate::cast::tofu`] and
/// [`peer_fingerprint`].
#[derive(Debug)]
struct PermissiveVerifier;

impl ServerCertVerifier for PermissiveVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if end_entity.is_empty() {
            return Err(rustls::Error::General(
                "receiver sent no certificate".into(),
            ));
        }
        tracing::debug!("accepting self-signed certificate without chain validation");
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A `ClientConfig` for Cast receivers: ring provider, no ALPN, no SNI, the
/// permissive verifier, and no client authentication (`03-cast-engine.md` §3.2).
pub fn client_config() -> Result<ClientConfig, rustls::Error> {
    install_crypto_provider();
    Ok(
        ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PermissiveVerifier))
            .with_no_client_auth(),
    )
}

/// Connect to a receiver and complete the TLS handshake with a
/// [`HANDSHAKE_TIMEOUT`] deadline (`03-cast-engine.md` §3.2). No SNI is sent
/// (the server name is an IP literal); the verifier ignores the certificate
/// hostname.
pub async fn connect(addr: SocketAddr) -> Result<CastTlsStream, TlsError> {
    connect_with_timeout(addr, HANDSHAKE_TIMEOUT).await
}

/// [`connect`] with an explicit handshake deadline (testable with short
/// timeouts). The TCP connect itself is bounded by the same deadline.
///
/// The synchronous rustls handshake runs on a `spawn_blocking` worker whose
/// lifetime is bounded *independently* of the caller: per-op socket
/// read/write timeouts are re-armed to the remaining handshake budget on
/// every `complete_io` cycle, so a stalled peer cannot strand the worker —
/// it exits (dropping the socket) at latest ~one op timeout after the
/// deadline even if the caller's oneshot receiver is long gone
/// (`spawn_blocking` cancellation is cooperative, so the caller's timeout
/// cannot stop the thread by itself).
pub async fn connect_with_timeout(
    addr: SocketAddr,
    deadline: Duration,
) -> Result<CastTlsStream, TlsError> {
    let tcp = timeout(deadline, TcpStream::connect(addr))
        .await
        .map_err(|_| TlsError::ConnectTimeout {
            addr,
            timeout: deadline,
        })?
        .map_err(|source| TlsError::Connect { addr, source })?;

    let mut std_tcp = tcp
        .into_std()
        .map_err(|source| TlsError::Connect { addr, source })?;
    // `into_std` leaves the socket non-blocking; the blocking worker driving
    // the synchronous rustls handshake needs a blocking socket.
    std_tcp
        .set_nonblocking(false)
        .map_err(|source| TlsError::Connect { addr, source })?;
    std_tcp
        .set_nodelay(true)
        .map_err(|source| TlsError::Connect { addr, source })?;

    // Drive the synchronous rustls handshake on a blocking worker with a
    // tokio timeout around a oneshot result channel. The worker re-arms
    // per-op socket timeouts against the remaining handshake budget, so it
    // is guaranteed to terminate (and release the socket) near the deadline
    // without manual intervention.
    let config = client_config().map_err(TlsError::Rustls)?;
    let server_name = ServerName::IpAddress(addr.ip().into());
    let deadline_at = Instant::now() + deadline;
    let (tx, rx) = oneshot::channel();

    IN_FLIGHT_HANDSHAKES.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(target: "cast::tls", %addr, in_flight = IN_FLIGHT_HANDSHAKES.load(Ordering::Relaxed), "TLS handshake worker started");
    let handle = tokio::task::spawn_blocking(move || {
        let _gauge = InFlightGuard;
        let result = (|| {
            let mut conn = ClientConnection::new(Arc::new(config), server_name)?;
            loop {
                let remaining = deadline_at.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(TlsError::HandshakeTimeout(deadline));
                }
                // `complete_io` blocks on each socket op; SO_RCVTIMEO /
                // SO_SNDTIMEO expiry surfaces as WouldBlock (Linux) or
                // TimedOut (Windows) and bounds every op to the remaining
                // budget, which bounds the worker's total lifetime.
                std_tcp.set_read_timeout(Some(remaining))?;
                std_tcp.set_write_timeout(Some(remaining))?;
                match conn.complete_io(&mut std_tcp) {
                    // Handshake finished: return the usable stream.
                    Ok(_) if !conn.is_handshaking() => {
                        return Ok(rustls::StreamOwned::new(conn, std_tcp));
                    }
                    // No progress (op timed out or the peer stalled mid-
                    // handshake): re-arm against the remaining budget.
                    Ok(_) => {}
                    Err(source)
                        if matches!(
                            source.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) => {}
                    // On Windows a refused peer often surfaces only when the
                    // handshake writes (WSAECONNRESET 10054) rather than at
                    // `connect`; keep the contract "refused ⇒ TlsError::Connect"
                    // on every platform.
                    Err(source)
                        if matches!(
                            source.kind(),
                            io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset
                        ) =>
                    {
                        return Err(TlsError::Connect { addr, source });
                    }
                    Err(source) => return Err(TlsError::Io(source)),
                }
            }
        })();
        let ok = result.is_ok();
        let _ = tx.send(result);
        drop(_gauge);
        tracing::debug!(target: "cast::tls", %addr, in_flight = IN_FLIGHT_HANDSHAKES.load(Ordering::Relaxed), ok, "TLS handshake worker finished");
    });

    match timeout(deadline, rx).await {
        Err(_) => {
            // The worker is deadline-bounded and exits on its own shortly
            // after this returns; the socket is dropped by the worker, not
            // left stranded on a blocked thread.
            tracing::warn!(target: "cast::tls", %addr, ?deadline, in_flight = IN_FLIGHT_HANDSHAKES.load(Ordering::Relaxed), "TLS handshake timed out; worker self-terminates");
            Err(TlsError::HandshakeTimeout(deadline))
        }
        Ok(Err(_)) => {
            let _ = handle.await;
            Err(TlsError::WorkerPanicked)
        }
        Ok(Ok(result)) => result,
    }
}

/// Graceful TLS shutdown: send `close_notify`, flush it and close the socket
/// (`03-cast-engine.md` §3.2). Best-effort — a dead peer must not block
/// teardown.
pub fn close_notify(stream: &mut CastTlsStream) {
    stream.conn.send_close_notify();
    let _ = stream.flush();
}

/// The SHA-256 fingerprint of the receiver's end-entity certificate as
/// presented in the completed handshake, used by the TOFU pin store
/// (`03-cast-engine.md` §3.1). `None` when no certificate was presented.
pub fn peer_fingerprint(stream: &CastTlsStream) -> Option<Fingerprint> {
    let end_entity = stream.conn.peer_certificates()?.first()?;
    let digest = sha2::Sha256::digest(end_entity.as_ref());
    let mut fingerprint = [0u8; 32];
    fingerprint.copy_from_slice(&digest);
    Some(fingerprint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// Poll the in-flight gauge until every handshake worker has exited, or
    /// fail the test. Proves the deadline-bounded workers terminate on their
    /// own without the caller awaiting them.
    async fn wait_for_in_flight_zero() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while IN_FLIGHT_HANDSHAKES.load(Ordering::Relaxed) != 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "handshake workers leaked past the deadline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn stalled_peer_times_out_and_worker_self_terminates() {
        // A peer that accepts TCP but never produces TLS bytes fails with
        // HandshakeTimeout within the configured deadline, and the blocking
        // worker exits on its own shortly afterwards (no stranded thread).
        install_crypto_provider();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hold_open = tokio::spawn(async move {
            let (_tcp, _peer) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let deadline = Duration::from_millis(300);
        let started = Instant::now();
        let result = connect_with_timeout(addr, deadline).await;
        assert!(matches!(result, Err(TlsError::HandshakeTimeout(_))));
        assert!(
            started.elapsed() < deadline + Duration::from_secs(1),
            "timeout returned late: {:?}",
            started.elapsed()
        );
        wait_for_in_flight_zero().await;
        drop(hold_open);
    }

    #[tokio::test]
    async fn partial_tls_bytes_then_stall_times_out_and_worker_self_terminates() {
        // A peer that sends a partial TLS record and then stalls must also
        // time out (the handshake never completes) with no stranded worker.
        install_crypto_provider();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hold_open = tokio::spawn(async move {
            let (mut tcp, _peer) = listener.accept().await.unwrap();
            // Handshake record header claiming a 16-byte body; the body is
            // never sent, so `complete_io` blocks mid-record until the op
            // timeout fires.
            tcp.write_all(&[0x16, 0x03, 0x01, 0x00, 0x10])
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let deadline = Duration::from_millis(300);
        let result = connect_with_timeout(addr, deadline).await;
        assert!(matches!(result, Err(TlsError::HandshakeTimeout(_))));
        wait_for_in_flight_zero().await;
        drop(hold_open);
    }

    #[tokio::test]
    async fn many_stalled_peers_do_not_accumulate_workers() {
        // Many simultaneous stalled peers must all time out and every worker
        // must terminate: the in-flight gauge returns to zero, proving the
        // blocking pool cannot accumulate stranded handshake threads.
        install_crypto_provider();
        const N: usize = 32;
        let deadline = Duration::from_millis(400);

        let mut addrs = Vec::with_capacity(N);
        let mut hold_open = Vec::with_capacity(N);
        for _ in 0..N {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            addrs.push(listener.local_addr().unwrap());
            hold_open.push(tokio::spawn(async move {
                let (_tcp, _peer) = listener.accept().await.unwrap();
                tokio::time::sleep(Duration::from_secs(60)).await;
            }));
        }

        let started = Instant::now();
        let mut handles = Vec::with_capacity(N);
        for addr in addrs {
            handles.push(tokio::spawn(connect_with_timeout(addr, deadline)));
        }
        for handle in handles {
            let result = handle.await.expect("attempt task joins");
            assert!(matches!(result, Err(TlsError::HandshakeTimeout(_))));
        }
        assert!(
            started.elapsed() < deadline + Duration::from_secs(1),
            "all attempts should return near the deadline: {:?}",
            started.elapsed()
        );
        wait_for_in_flight_zero().await;
        for task in hold_open {
            task.abort();
        }
    }

    fn self_signed_chain() -> Vec<CertificateDer<'static>> {
        let certified_key = rcgen::generate_simple_self_signed(vec!["cast.local".into()])
            .expect("rcgen generates a self-signed cert");
        vec![certified_key.cert.der().clone()]
    }

    #[test]
    fn verifier_accepts_self_signed_certificate() {
        // (FR-011) A self-signed receiver certificate is accepted; the
        // verifier skips chain and hostname trust evaluation.
        let chain = self_signed_chain();
        let server_name = ServerName::IpAddress(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 42)).into());
        let result = PermissiveVerifier.verify_server_cert(
            &chain[0],
            &chain[1..],
            &server_name,
            &[],
            UnixTime::now(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn verifier_rejects_missing_certificate() {
        // An empty end-entity certificate must not be accepted.
        let server_name = ServerName::IpAddress(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)).into());
        let empty = CertificateDer::from(&[][..]);
        let result =
            PermissiveVerifier.verify_server_cert(&empty, &[], &server_name, &[], UnixTime::now());
        assert!(result.is_err());
    }

    #[test]
    fn supported_verify_schemes_match_ring_provider() {
        // Handshake signature verification is offered for exactly the schemes
        // the ring provider supports.
        let provider = rustls::crypto::ring::default_provider();
        let schemes = PermissiveVerifier.supported_verify_schemes();
        assert!(!schemes.is_empty());
        for scheme in &schemes {
            assert!(
                provider
                    .signature_verification_algorithms
                    .supported_schemes()
                    .contains(scheme)
            );
        }
    }

    #[test]
    fn client_config_offers_no_alpn() {
        // (FR-012) No ALPN protocol is offered to the receiver.
        let config = client_config().expect("config builds");
        assert!(config.alpn_protocols.is_empty());
        assert!(config.check_selected_alpn);
    }
}
