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
    // HandshakeTimeout instead of blocking forever.
    install_crypto_provider();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hold_open = tokio::spawn(async move {
        let (_tcp, _peer) = listener.accept().await.expect("accept");
        tokio::time::sleep(Duration::from_secs(60)).await;
    });

    let result = connect_with_timeout(addr, Duration::from_millis(300)).await;
    assert!(matches!(result, Err(TlsError::HandshakeTimeout(_))));
    drop(hold_open);
}

#[tokio::test]
async fn connect_to_unreachable_port_fails() {
    // A refused connection surfaces as TlsError::Connect, not a hang.
    install_crypto_provider();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // nothing listens now

    let result = connect_with_timeout(addr, Duration::from_secs(2)).await;
    assert!(matches!(result, Err(TlsError::Connect { .. })));
}
