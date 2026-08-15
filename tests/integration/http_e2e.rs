//! In-process media-server end-to-end tests (`04-media-proxy.md` §6):
//! 200/206/416/404/405, `HEAD`, MIME detection, URL proxying with `Range`
//! forwarding, `502` on remote failure, non-2xx pass-through, live screen
//! streaming, source-switch cancellation of in-flight connections, and port
//! rebinding.
//! Gate: `cargo test --test http_e2e`.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::time::Duration;

use cast_app::media::server::MediaServer;
use cast_app::media::source::ActiveSource;
use cast_app::util::shutdown::Shutdown;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(10);

/// A running media server plus its shutdown token, torn down when dropped.
struct TestServer {
    server: MediaServer,
    shutdown: Shutdown,
    port: u16,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.trigger();
    }
}

async fn start_server() -> TestServer {
    let shutdown = Shutdown::new();
    let server = MediaServer::start(shutdown.clone(), 0);
    let port = wait_for_port(&server).await;
    TestServer {
        server,
        shutdown,
        port,
    }
}

async fn wait_for_port(server: &MediaServer) -> u16 {
    let mut rx = server.subscribe_port();
    if *rx.borrow() != 0 {
        return *rx.borrow();
    }
    timeout(WAIT, async {
        loop {
            if rx.changed().await.is_err() {
                panic!("media server task exited before binding");
            }
            if *rx.borrow() != 0 {
                return *rx.borrow();
            }
        }
    })
    .await
    .expect("media server never bound a port")
}

/// Wait until the generation watch moves past `before` — the server has
/// processed the command that bumped it.
async fn wait_for_generation(rx: &mut watch::Receiver<u64>, before: u64) {
    timeout(WAIT, async {
        loop {
            if *rx.borrow() != before {
                return;
            }
            if rx.changed().await.is_err() {
                panic!("media server task exited");
            }
        }
    })
    .await
    .expect("media server never processed the command")
}

/// Deterministic `set_source`: returns only once the server has processed
/// the switch, so the follow-up request cannot race it.
async fn set_source(server: &MediaServer, source: ActiveSource) {
    let mut rx = server.subscribe_generation();
    let before = *rx.borrow();
    server.set_source(source);
    wait_for_generation(&mut rx, before).await;
}

/// Deterministic screen-stream attach.
async fn attach_screen_stream(server: &MediaServer, receiver: mpsc::Receiver<Vec<u8>>) {
    let mut rx = server.subscribe_generation();
    let before = *rx.borrow();
    server.attach_screen_stream(receiver);
    wait_for_generation(&mut rx, before).await;
}

fn stream_url(port: u16) -> String {
    format!("http://127.0.0.1:{port}/stream")
}

fn write_temp_file(name: &str, contents: &[u8]) -> PathBuf {
    let path =
        std::env::temp_dir().join(format!("cast-app-http-e2e-{}-{name}", std::process::id()));
    std::fs::write(&path, contents).expect("temp file write");
    path
}

/// A dead-port URL for connection-failure tests: bind, note the port, drop.
async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind free port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// Minimal in-process origin server: reads one request head, records it on
/// `requests`, and replies with `status`, `extra_headers`, and `body`.
async fn spawn_origin(
    status: u16,
    extra_headers: &'static [(&'static str, &'static str)],
    body: &'static [u8],
) -> (u16, mpsc::UnboundedReceiver<String>) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("origin bind");
    let port = listener.local_addr().expect("origin local addr").port();
    let (requests_tx, requests_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("origin accept");
        let mut head = Vec::new();
        let mut buffer = [0u8; 1024];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = socket.read(&mut buffer).await.expect("origin read");
            if read == 0 {
                break;
            }
            head.extend_from_slice(&buffer[..read]);
            if head.len() > 64 * 1024 {
                break;
            }
        }
        let _ = requests_tx.send(String::from_utf8_lossy(&head).into_owned());
        let mut response = format!("HTTP/1.1 {status} Origin\r\nConnection: close\r\n");
        for (name, value) in extra_headers {
            response.push_str(name);
            response.push_str(": ");
            response.push_str(value);
            response.push_str("\r\n");
        }
        response.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
        socket
            .write_all(response.as_bytes())
            .await
            .expect("origin write head");
        socket.write_all(body).await.expect("origin write body");
    });
    (port, requests_rx)
}

// ---------------------------------------------------------------------------
// Local files: 200 / 206 / 416 / HEAD / MIME / 404
// ---------------------------------------------------------------------------

#[tokio::test]
async fn local_file_serves_200_with_full_body_and_headers() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(write_temp_file("full.mp4", b"hello world")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client.get(stream_url(ts.port)).send().await.expect("GET");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert_eq!(response.headers()["accept-ranges"], "bytes");
    assert_eq!(response.headers()["cache-control"], "no-cache");
    assert_eq!(response.headers()["content-length"], "11");
    assert_eq!(response.bytes().await.expect("body"), &b"hello world"[..]);
}

#[tokio::test]
async fn local_file_serves_206_with_sliced_body() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(write_temp_file("range.mp4", b"0123456789")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .get(stream_url(ts.port))
        .header("Range", "bytes=2-5")
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status().as_u16(), 206);
    assert_eq!(response.headers()["content-range"], "bytes 2-5/10");
    assert_eq!(response.headers()["content-length"], "4");
    assert_eq!(response.bytes().await.expect("body"), &b"2345"[..]);
}

#[tokio::test]
async fn local_file_serves_suffix_range() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(write_temp_file("suffix.mp4", b"0123456789")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .get(stream_url(ts.port))
        .header("Range", "bytes=-3")
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status().as_u16(), 206);
    assert_eq!(response.headers()["content-range"], "bytes 7-9/10");
    assert_eq!(response.bytes().await.expect("body"), &b"789"[..]);
}

#[tokio::test]
async fn local_file_unsatisfiable_range_is_416() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(write_temp_file("unsat.mp4", b"0123456789")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .get(stream_url(ts.port))
        .header("Range", "bytes=50-60")
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status().as_u16(), 416);
    assert_eq!(response.headers()["content-range"], "bytes */10");
    assert_eq!(response.bytes().await.expect("body").len(), 0);
}

#[tokio::test]
async fn local_file_multi_range_is_ignored_as_200() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(write_temp_file("multi.mp4", b"0123456789")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .get(stream_url(ts.port))
        .header("Range", "bytes=0-1,5-6")
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.bytes().await.expect("body"), &b"0123456789"[..]);
}

#[tokio::test]
async fn local_file_head_returns_headers_without_body() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(write_temp_file("head.mp4", b"0123456789")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client.head(stream_url(ts.port)).send().await.expect("HEAD");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.headers()["content-length"], "10");
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert_eq!(response.bytes().await.expect("body").len(), 0);
}

#[tokio::test]
async fn unknown_extension_gets_octet_stream_mime() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(write_temp_file("clip.bin", b"data")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client.get(stream_url(ts.port)).send().await.expect("GET");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response.headers()["content-type"],
        "application/octet-stream"
    );
}

#[tokio::test]
async fn missing_file_is_404() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(PathBuf::from("/nonexistent/cast-app-missing.mp4")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client.get(stream_url(ts.port)).send().await.expect("GET");
    assert_eq!(response.status().as_u16(), 404);
}

// ---------------------------------------------------------------------------
// Routing: 404 unknown path, 405 bad method, 400 malformed head
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_path_is_404() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(write_temp_file("route.mp4", b"data")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://127.0.0.1:{}/other", ts.port))
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn post_is_405_with_allow_header() {
    let ts = start_server().await;

    let client = reqwest::Client::new();
    let response = client
        .post(stream_url(ts.port))
        .body("x")
        .send()
        .await
        .expect("POST");
    assert_eq!(response.status().as_u16(), 405);
    assert_eq!(response.headers()["allow"], "GET, HEAD");
}

#[tokio::test]
async fn no_active_source_is_404() {
    let ts = start_server().await;

    let client = reqwest::Client::new();
    let response = client.get(stream_url(ts.port)).send().await.expect("GET");
    assert_eq!(response.status().as_u16(), 404);
}

// ---------------------------------------------------------------------------
// URL proxy: Range forwarding, header isolation, pass-through, 502, HEAD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn url_proxy_forwards_range_and_mirrors_206() {
    let (origin_port, mut requests) = spawn_origin(
        206,
        &[
            ("Content-Type", "video/mp4"),
            ("Content-Range", "bytes 5-9/10"),
        ],
        b"56789",
    )
    .await;
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::Url(format!("http://127.0.0.1:{origin_port}/clip.mp4")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client
        .get(stream_url(ts.port))
        .header("Range", "bytes=5-9")
        .header("X-Client-Marker", "do-not-forward")
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status().as_u16(), 206);
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert_eq!(response.headers()["content-range"], "bytes 5-9/10");
    assert_eq!(response.headers()["content-length"], "5");
    assert_eq!(response.bytes().await.expect("body"), &b"56789"[..]);

    let origin_request = requests
        .recv()
        .await
        .expect("origin never received the proxied request");
    let origin_request_lower = origin_request.to_ascii_lowercase();
    assert!(
        origin_request_lower.contains("range: bytes=5-9"),
        "Range must be forwarded to the origin: {origin_request}"
    );
    assert!(
        !origin_request.contains("X-Client-Marker"),
        "client headers other than Range must not be forwarded: {origin_request}"
    );
}

#[tokio::test]
async fn url_proxy_passes_through_non_2xx_status_and_body() {
    let (origin_port, _requests) =
        spawn_origin(403, &[("Content-Type", "text/plain")], b"forbidden").await;
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::Url(format!("http://127.0.0.1:{origin_port}/denied")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client.get(stream_url(ts.port)).send().await.expect("GET");
    assert_eq!(response.status().as_u16(), 403);
    assert_eq!(response.headers()["content-type"], "text/plain");
    assert_eq!(response.bytes().await.expect("body"), &b"forbidden"[..]);
}

#[tokio::test]
async fn url_proxy_returns_502_on_remote_connection_failure() {
    let dead_port = free_port().await;
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::Url(format!("http://127.0.0.1:{dead_port}/down")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client.get(stream_url(ts.port)).send().await.expect("GET");
    assert_eq!(response.status().as_u16(), 502);
    assert_eq!(response.bytes().await.expect("body"), &b"Bad Gateway"[..]);
}

#[tokio::test]
async fn url_proxy_rejects_userinfo_urls() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::Url("http://user:pass@127.0.0.1:1/x".to_string()),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client.get(stream_url(ts.port)).send().await.expect("GET");
    assert_eq!(response.status().as_u16(), 400);
}

#[tokio::test]
async fn url_proxy_head_returns_headers_without_body() {
    let (origin_port, _requests) =
        spawn_origin(200, &[("Content-Type", "video/mp4")], b"0123456789").await;
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::Url(format!("http://127.0.0.1:{origin_port}/clip.mp4")),
    )
    .await;

    let client = reqwest::Client::new();
    let response = client.head(stream_url(ts.port)).send().await.expect("HEAD");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert_eq!(response.headers()["content-length"], "10");
    assert_eq!(response.bytes().await.expect("body").len(), 0);
}

// ---------------------------------------------------------------------------
// Source switching terminates in-flight connections
// ---------------------------------------------------------------------------

#[tokio::test]
async fn source_switch_aborts_in_flight_stream() {
    let size = 32 * 1024 * 1024;
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(write_temp_file("big.mp4", &vec![0xAB; size])),
    )
    .await;

    let client = reqwest::Client::new();
    let mut response = client
        .get(stream_url(ts.port))
        .header("Range", "bytes=0-")
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status().as_u16(), 206);

    let mut received = 0usize;
    while received < 64 * 1024 {
        match response.chunk().await.expect("chunk") {
            Some(chunk) => received += chunk.len(),
            None => panic!("stream ended before the source switch"),
        }
    }
    ts.server
        .set_source(ActiveSource::Url("http://127.0.0.1:1/switched".to_string()));

    let mut total = received;
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => total += chunk.len(),
            Ok(None) => break,
            Err(_) => break, // connection reset by the abort — expected
        }
    }
    assert!(
        total < size,
        "in-flight stream must be aborted by the source switch; got {total} of {size} bytes"
    );
}

// ---------------------------------------------------------------------------
// Live screen stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn live_screen_stream_is_continuous_video_mp4() {
    let ts = start_server().await;
    ts.server
        .set_source(ActiveSource::Screen("test-monitor".to_string()));
    let (tx, rx) = mpsc::channel::<Vec<u8>>(8);
    attach_screen_stream(&ts.server, rx).await;

    let client = reqwest::Client::new();
    let mut response = client.get(stream_url(ts.port)).send().await.expect("GET");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.headers()["content-type"], "video/mp4");
    assert!(
        response.headers().get("content-length").is_none(),
        "live streams must not send Content-Length"
    );

    tx.send(vec![1, 2, 3, 4]).await.expect("first chunk");
    let first = response.chunk().await.expect("first").expect("first chunk");
    assert_eq!(&first[..], &[1, 2, 3, 4]);

    tx.send(vec![5, 6]).await.expect("second chunk");
    let second = response
        .chunk()
        .await
        .expect("second")
        .expect("second chunk");
    assert_eq!(&second[..], &[5, 6]);

    drop(tx);
    assert!(
        response.chunk().await.expect("end").is_none(),
        "stream must end when the encoder channel closes"
    );
}

#[tokio::test]
async fn live_screen_stream_busy_is_503() {
    let ts = start_server().await;
    ts.server
        .set_source(ActiveSource::Screen("test-monitor".to_string()));
    let (_tx, rx) = mpsc::channel::<Vec<u8>>(8);
    attach_screen_stream(&ts.server, rx).await;

    // First connection consumes the single-consumer screen stream.
    let client = reqwest::Client::new();
    let first = client
        .get(stream_url(ts.port))
        .send()
        .await
        .expect("first GET");
    assert_eq!(first.status().as_u16(), 200);

    // A second concurrent consumer must be refused while it is held.
    let second = client
        .get(stream_url(ts.port))
        .send()
        .await
        .expect("second GET");
    assert_eq!(second.status().as_u16(), 503);
    assert_eq!(
        second
            .headers()
            .get("content-length")
            .expect("content-length"),
        "4",
        "503 Content-Length must match the body"
    );
    assert_eq!(
        second.text().await.expect("503 body"),
        "busy",
        "503 body must match Content-Length"
    );
}

// ---------------------------------------------------------------------------
// Port rebinding
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_port_rebinds_the_listener() {
    let ts = start_server().await;
    set_source(
        &ts.server,
        ActiveSource::File(write_temp_file("rebind.mp4", b"hello")),
    )
    .await;

    let client = reqwest::Client::new();
    let old = client
        .get(stream_url(ts.port))
        .send()
        .await
        .expect("old port GET");
    assert_eq!(old.status().as_u16(), 200);

    let new_port = free_port().await;
    ts.server.set_port(new_port);
    let mut rx = ts.server.subscribe_port();
    timeout(WAIT, async {
        loop {
            if *rx.borrow() == new_port {
                return;
            }
            let _ = rx.changed().await;
        }
    })
    .await
    .expect("server never rebound");

    let rebound = client
        .get(format!("http://127.0.0.1:{new_port}/stream"))
        .send()
        .await
        .expect("rebound GET");
    assert_eq!(rebound.status().as_u16(), 200);
    assert_eq!(rebound.bytes().await.expect("body"), &b"hello"[..]);
}
