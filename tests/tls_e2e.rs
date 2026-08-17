// SPDX-License-Identifier: MIT OR Apache-2.0
//! End-to-end TLS transport tests against a local rustls server with a
//! self-signed certificate (`03-cast-engine.md` §3). The connected stream is
//! exercised with plain text (no CastV2 framing yet — that is Phase 4).

#![forbid(unsafe_code)]

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use cast_app::cast::tls::{
    TlsError, close_notify, connect, connect_with_timeout, install_crypto_provider,
};
use rustls::pki_types::PrivateKeyDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn self_signed_server_config() -> rustls::ServerConfig {
    install_crypto_provider();
    let certified = rcgen::generate_simple_self_signed(vec!["cast.local".into()])
        .expect("rcgen generates a self-signed cert");
    let cert = certified.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(certified.signing_key.serialize_der().into());
    rustls::ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .expect("server protocol versions")
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
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
