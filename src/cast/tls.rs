//! rustls client configuration and a permissive certificate verifier for
//! self-signed Chromecast certificates. Owned by `03-cast-engine.md` §3.

use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ClientConnection, DigitallySignedStruct, SignatureScheme};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::timeout;

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
/// self-signed certificates and no trust anchor exists.
///
/// Hostname/certificate pinning is documented as a future hardening option.
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

    // The rustls handshake API is synchronous; drive it on a blocking worker
    // with a tokio timeout around a oneshot result channel. If the timeout
    // fires, the socket is dropped by the worker once the TLS reads return.
    let config = client_config().map_err(TlsError::Rustls)?;
    let server_name = ServerName::IpAddress(addr.ip().into());
    let (tx, rx) = oneshot::channel();

    let handle = tokio::task::spawn_blocking(move || {
        let result = (|| {
            let mut conn = ClientConnection::new(Arc::new(config), server_name)?;
            match conn.complete_io(&mut std_tcp) {
                Ok(_) => Ok(rustls::StreamOwned::new(conn, std_tcp)),
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
                    Err(TlsError::Connect { addr, source })
                }
                Err(source) => Err(TlsError::Io(source)),
            }
        })();
        let _ = tx.send(result);
    });

    match timeout(deadline, rx).await {
        Err(_) => Err(TlsError::HandshakeTimeout(deadline)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

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
