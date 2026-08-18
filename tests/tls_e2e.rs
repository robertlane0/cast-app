// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end TLS transport tests against a local rustls server with a
//! self-signed certificate (`03-cast-engine.md` §3). The connected stream is
//! exercised with plain text (no CastV2 framing yet — that is Phase 4), and
//! the TOFU certificate-pin lifecycle is verified over real handshakes
//! (§3.1): first-seen certificates are pinned, identical ones match, and a
//! different certificate produces a mismatch warning without blocking.

#![forbid(unsafe_code)]

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use cast_app::cast::connection::{Connector, TlsConnector};
use cast_app::cast::tls::{
    TlsError, close_notify, connect, connect_with_timeout, install_crypto_provider,
};
use cast_app::cast::tofu::{PinCheck, TofuStore, fingerprint_to_hex};
use cast_app::state::CastDevice;
use rustls::pki_types::PrivateKeyDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn self_signed_server_config() -> rustls::ServerConfig {
    install_crypto_provider();
    let (certs, key) = self_signed_cert();
    server_config(certs, key)
}

/// A fresh self-signed cert + key pair (rcgen).
fn self_signed_cert() -> (
    Vec<rustls::pki_types::CertificateDer<'static>>,
    PrivateKeyDer<'static>,
) {
    let certified = rcgen::generate_simple_self_signed(vec!["cast.local".into()])
        .expect("rcgen generates a self-signed cert");
    let cert = certified.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(certified.signing_key.serialize_der().into());
    (vec![cert], key)
}

fn server_config(
    certs: Vec<rustls::pki_types::CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> rustls::ServerConfig {
    rustls::ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .expect("server protocol versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("server cert installs")
}

async fn echo_server(
    config: Arc<rustls::ServerConfig>,
    listener: TcpListener,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (tcp, _peer) = listener.accept().await.expect("accept");
        // All rustls I/O is synchronous: run it on the blocking pool so the
        // tokio executor (single-threaded in tests) is never stalled.
        tokio::task::spawn_blocking(move || {
            let mut tcp: std::net::TcpStream = tcp
                .into_std()
                .expect("convert to blocking socket for rustls server");
            tcp.set_nonblocking(false).expect("server socket blocking");
            let mut conn = rustls::ServerConnection::new(config).expect("server conn");
            conn.complete_io(&mut tcp).expect("server handshake");
            let mut stream = rustls::StreamOwned::new(conn, tcp);
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).expect("server read");
            stream.write_all(&buf).expect("server write");
            stream.flush().expect("server flush");
            stream.conn.send_close_notify();
            let _ = stream.flush();
        })
        .await
        .expect("server worker joins");
    })
}

/// Drain the accepted socket until the peer closes it (clean EOF). The
/// client's handshake worker produces that EOF when it exits and drops its
/// socket — the assertion "EOF observed" proves no worker stayed stranded
/// holding the fd open. Any bytes read before the EOF are the client's
/// `ClientHello`, which the server side must discard, not fail on.
async fn drain_until_eof(tcp: &mut tokio::net::TcpStream) -> std::io::Result<()> {
    let mut buf = [0u8; 4096];
    loop {
        let n = tcp.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
    }
}

/// One-shot TLS server: accepts a single connection and completes the
/// handshake on the blocking pool, then exits when the client drops the
/// stream. Returns the listen address and the server task.
async fn one_shot_tls_server(
    config: Arc<rustls::ServerConfig>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let (tcp, _peer) = listener.accept().await.expect("accept");
        tokio::task::spawn_blocking(move || {
            let mut tcp: std::net::TcpStream = tcp
                .into_std()
                .expect("convert to blocking socket for rustls server");
            tcp.set_nonblocking(false).expect("server socket blocking");
            let mut conn = rustls::ServerConnection::new(config).expect("server conn");
            conn.complete_io(&mut tcp).expect("server handshake");
        })
        .await
        .expect("server worker joins");
    });
    (addr, handle)
}

fn tofu_device(addr: std::net::SocketAddr) -> CastDevice {
    CastDevice {
        id: addr.to_string(),
        name: "TOFU TV".to_string(),
        addr,
        tofu_key: "TOFU TV+127.0.0.1".to_string(),
    }
}

#[tokio::test]
async fn handshake_with_self_signed_server_succeeds_and_echoes() {
    // (FR-013) The permissive verifier completes the TLS handshake against a
    // self-signed server and plain-text I/O works over the established stream.
    let config = Arc::new(self_signed_server_config());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = echo_server(config, listener).await;

    let mut stream = connect(addr).await.expect("client handshake");
    stream.write_all(b"ping").expect("client write");
    let mut echo = [0u8; 4];
    std::io::Read::read_exact(&mut stream, &mut echo).expect("client read");
    assert_eq!(&echo, b"ping");

    close_notify(&mut stream);
    server.await.expect("server task joins");
}

#[tokio::test]
async fn handshake_times_out_against_a_non_tls_peer() {
    // (FR-014) A peer that never produces TLS bytes fails the handshake with
    // HandshakeTimeout instead of blocking forever, and the deadline-bounded
    // worker releases the socket on its own: the peer observes a clean EOF
    // shortly after the timeout returns (no stranded blocking thread holding
    // the fd open).
    install_crypto_provider();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut tcp, _peer) = listener.accept().await.expect("accept");
        let drained = tokio::time::timeout(Duration::from_secs(3), drain_until_eof(&mut tcp)).await;
        assert!(
            matches!(drained, Ok(Ok(()))),
            "expected clean EOF when the client's handshake worker exits, got {drained:?}"
        );
    });

    let result = connect_with_timeout(addr, Duration::from_millis(300)).await;
    assert!(matches!(result, Err(TlsError::HandshakeTimeout(_))));
    peer.await.expect("peer task joins");
}

#[tokio::test]
async fn handshake_times_out_after_partial_tls_bytes() {
    // A peer that sends a partial TLS record and then stalls mid-handshake
    // fails with HandshakeTimeout (not a hang), and the socket is released
    // when the bounded worker exits.
    install_crypto_provider();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut tcp, _peer) = listener.accept().await.expect("accept");
        // Handshake record header claiming a 16-byte body; the body is never
        // sent, so the client blocks mid-record until its op timeout fires.
        tcp.write_all(&[0x16, 0x03, 0x01, 0x00, 0x10])
            .await
            .expect("partial write");
        let drained = tokio::time::timeout(Duration::from_secs(3), drain_until_eof(&mut tcp)).await;
        assert!(
            matches!(drained, Ok(Ok(()))),
            "expected clean EOF when the client's handshake worker exits, got {drained:?}"
        );
    });

    let result = connect_with_timeout(addr, Duration::from_millis(300)).await;
    assert!(matches!(result, Err(TlsError::HandshakeTimeout(_))));
    peer.await.expect("peer task joins");
}

#[tokio::test]
async fn connect_to_unreachable_port_fails() {
    // A refused connection surfaces as TlsError::Connect, not a hang.
    // Windows loopback quirk: connecting to a closed port can report success
    // at the socket layer with the refusal arriving later, in which case the
    // bounded ConnectTimeout fires instead. Both prove "bounded failure".
    install_crypto_provider();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // nothing listens now

    let result = connect_with_timeout(addr, Duration::from_secs(2)).await;
    assert!(
        matches!(
            result,
            Err(TlsError::Connect { .. }) | Err(TlsError::ConnectTimeout { .. })
        ),
        "expected Connect or ConnectTimeout, got {result:?}"
    );
}

#[tokio::test]
async fn tofu_pins_first_seen_cert_matches_identical_and_warns_on_mismatch() {
    // (03-cast-engine.md §3.1) The TOFU lifecycle over real handshakes:
    // first-seen certificates are pinned, the same certificate on a later
    // connection matches, and a different certificate is a non-blocking
    // mismatch that keeps the original pin (SSH host-key semantics).
    install_crypto_provider();
    let store = Arc::new(TofuStore::in_memory());
    let connector = TlsConnector::new(store.clone());
    // Two distinct self-signed certificates for the same receiver; cert A's
    // key material is kept so a later server can present it again.
    let (certs_a, key_a) = self_signed_cert();
    let (certs_b, key_b) = self_signed_cert();
    let config_a = Arc::new(server_config(certs_a.clone(), key_a.clone_key()));
    let config_b = Arc::new(server_config(certs_b, key_b));

    // 1. First connection: the certificate is pinned.
    let (addr, server_a) = one_shot_tls_server(config_a.clone()).await;
    let device = tofu_device(addr);
    let (stream, check) = connector.connect(&device).await.expect("first connect");
    assert_eq!(check, PinCheck::Pinned, "first-seen certificate is pinned");
    // The pin the connector stored must be the SHA-256 of the end-entity DER
    // (`03-cast-engine.md` §3.1); the later Matched/Mismatch steps verify the
    // comparison against it.
    let first_fingerprint: [u8; 32] = {
        use sha2::Digest;
        sha2::Sha256::digest(certs_a[0].as_ref()).into()
    };
    drop(stream);
    server_a.await.expect("first server joins");

    // 2. Same certificate again: a match.
    let (addr, server_a2) = one_shot_tls_server(config_a).await;
    let (stream, check) = connector
        .connect(&tofu_device(addr))
        .await
        .expect("second connect");
    assert_eq!(check, PinCheck::Matched, "identical certificate matches");
    drop(stream);
    server_a2.await.expect("second server joins");

    // 3. Different certificate: a non-blocking mismatch carrying both
    //    digests; the store keeps the original pin.
    let (addr, server_b) = one_shot_tls_server(config_b).await;
    let (stream, check) = connector
        .connect(&tofu_device(addr))
        .await
        .expect("third connect");
    let PinCheck::Mismatch { previous, current } = check else {
        panic!("different certificate must be reported as a mismatch, got {check:?}");
    };
    assert_eq!(
        previous, first_fingerprint,
        "previous is the first-seen pin"
    );
    assert_ne!(
        current, first_fingerprint,
        "the presented certificate really differs"
    );
    assert_eq!(
        fingerprint_to_hex(&previous).len(),
        64,
        "the GUI warning shows full hex digests"
    );
    drop(stream);
    server_b.await.expect("third server joins");

    // 4. The original certificate still matches afterwards — the mismatch
    //    never re-pins (SSH semantics: degrade gracefully, keep the pin).
    let (addr, server_a3) =
        one_shot_tls_server(Arc::new(server_config(certs_a, key_a.clone_key()))).await;
    let (stream, check) = connector
        .connect(&tofu_device(addr))
        .await
        .expect("fourth connect");
    assert_eq!(
        check,
        PinCheck::Matched,
        "original pin survives the mismatch"
    );
    drop(stream);
    server_a3.await.expect("fourth server joins");
}
