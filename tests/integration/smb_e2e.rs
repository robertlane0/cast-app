// SPDX-License-Identifier: MIT OR Apache-2.0
#![forbid(unsafe_code)]
#![cfg(feature = "e2e-smb")]

// Real-network-share end-to-end tests (`04-media-proxy.md` §4.4). These
// exercise the production `Smb2Connector` (the `smb2` crate client) against
// a real SMB server and assert the `/stream` serving semantics that the
// fake-based `tests/smb_tests.rs` covers with doubles.
//
// The tests are `#[ignore]`d and gated behind the `e2e-smb` feature; they
// are never run by default CI:
//
//     SMB_E2E_SERVER=nas:445 SMB_E2E_SHARE=media SMB_E2E_PATH=dir/video.mp4 \
//       cargo test --features e2e-smb --test smb_e2e -- --ignored --test-threads=1
//
// Environment:
//   - `SMB_E2E_SERVER`  (required) "host:port" of the SMB server; the share
//     must accept anonymous/guest access.
//   - `SMB_E2E_SHARE`   (required) share name.
//   - `SMB_E2E_PATH`    (required) file path inside the share.
//   - `SMB_E2E_AUTH_SERVER` (optional) "host:port" of a server whose guest
//     logon is rejected; the 401 path is asserted when set.
//
// Gate: `cargo test --features e2e-smb --test smb_e2e -- --ignored`.

use std::collections::HashMap;

use cast_app::media::smb_source::{self, Smb2Connector};
use tokio::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn required_env(name: &str) -> String {
    env(name).unwrap_or_else(|| {
        panic!("{name} must be set to run the SMB e2e tests (see the module docs)")
    })
}

/// Run one `serve` call against the production connector over a real TCP
/// pair and return the raw response bytes.
async fn serve_once(raw_url: &str, range: Option<&str>, head_only: bool) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let raw_url = raw_url.to_string();
    let range = range.map(str::to_string);
    let serve_result = tokio::spawn(async move {
        let (stream, _peer) = listener.accept().await.expect("accept");
        let (_read_half, write_half) = stream.into_split();
        let mut writer = BufWriter::new(write_half);
        smb_source::serve(
            &mut writer,
            &Smb2Connector,
            &raw_url,
            range.as_deref(),
            head_only,
        )
        .await
    });
    let stream = TcpStream::connect(addr).await.expect("connect");
    let (read_half, _write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.expect("read");
    serve_result.await.expect("task").expect("serve ok");
    bytes
}

/// Split a raw HTTP/1.1 response into (status, headers, body).
fn parse_response(bytes: &[u8]) -> (u16, HashMap<String, String>, Vec<u8>) {
    let head_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response head terminator")
        + 4;
    let head = std::str::from_utf8(&bytes[..head_end]).expect("ascii head");
    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("status line");
    let status: u16 = status_line
        .split(' ')
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric");
    let mut headers = HashMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').expect("header colon");
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    (status, headers, bytes[head_end..].to_vec())
}

fn guest_url() -> String {
    let server = required_env("SMB_E2E_SERVER");
    let share = required_env("SMB_E2E_SHARE");
    let path = required_env("SMB_E2E_PATH");
    format!("smb://{server}/{share}/{path}")
}

/// (FR-033) A guest-accessible share serves the whole file with `200`.
#[tokio::test]
#[ignore]
async fn guest_share_serves_200_with_full_body() {
    let (status, headers, body) = parse_response(&serve_once(&guest_url(), None, false).await);
    assert_eq!(status, 200, "guest share must serve 200");
    let length: usize = headers["content-length"]
        .parse()
        .expect("content-length numeric");
    assert_eq!(body.len(), length, "body must match Content-Length");
    assert!(!body.is_empty(), "a real media file has bytes");
    assert!(
        headers.contains_key("content-type"),
        "Content-Type must be present"
    );
    assert_eq!(headers["accept-ranges"], "bytes");
}

/// (FR-033) A `Range` request is served as `206` with `Content-Range`.
#[tokio::test]
#[ignore]
async fn guest_share_serves_206_range() {
    let (status, headers, body) =
        parse_response(&serve_once(&guest_url(), Some("bytes=0-1023"), false).await);
    assert_eq!(status, 206, "a byte range on a guest share must yield 206");
    assert_eq!(body.len(), 1024, "exactly the requested range");
    let range = headers["content-range"].clone();
    assert!(
        range.starts_with("bytes 0-1023/"),
        "unexpected Content-Range: {range}"
    );
}

/// (FR-034) A server that rejects the guest logon fails with `401`; the
/// client must never attempt authentication.
#[tokio::test]
#[ignore]
async fn auth_required_server_fails_with_401() {
    let Some(server) = env("SMB_E2E_AUTH_SERVER") else {
        eprintln!("SMB_E2E_AUTH_SERVER unset; skipping the 401 assertion");
        return;
    };
    let share = required_env("SMB_E2E_SHARE");
    let path = required_env("SMB_E2E_PATH");
    let (status, _, body) =
        parse_response(&serve_once(&format!("smb://{server}/{share}/{path}"), None, false).await);
    assert_eq!(status, 401, "an auth-required share must be rejected");
    assert!(
        body.windows(b"authentication required".len())
            .any(|window| window == b"authentication required"),
        "the 401 body must state the reason"
    );
}

/// The e2e counterpart of the fake `close` contract: a `HEAD` request opens
/// and closes a handle without streaming a body.
#[tokio::test]
#[ignore]
async fn head_returns_headers_without_body() {
    let (status, headers, body) = parse_response(&serve_once(&guest_url(), None, true).await);
    assert_eq!(status, 200);
    assert!(headers.contains_key("content-length"));
    assert!(body.is_empty(), "HEAD must not stream a body");
}
