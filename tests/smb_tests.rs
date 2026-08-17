// SPDX-License-Identifier: MIT OR Apache-2.0
#![forbid(unsafe_code)]

//! Network-share (`smb://`) source tests (`04-media-proxy.md` §4.4): URL
//! parsing/validation (anonymous-only, share + file required) and the
//! `/stream` serving semantics (200/206/416/400/401/403/404/502, HEAD) driven
//! against fake `SmbConnector`/`SmbFile` doubles so no share is needed.
//!
//! The real `smb2` crate client is exercised by the feature-gated,
//! env-configured `tests/integration/smb_e2e.rs`.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::Arc;

use cast_app::media::smb_source::{self, SmbConnector, SmbError, SmbFile, SmbUrl};
use tokio::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

/// A `SmbFile` double serving in-memory bytes; optionally failing reads.
struct FakeFile {
    data: Vec<u8>,
    /// When set, every read fails with a freshly built `Transport` error.
    read_error: bool,
}

impl SmbFile for FakeFile {
    fn size(&self) -> u64 {
        self.data.len() as u64
    }

    async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, SmbError> {
        if self.read_error {
            return Err(SmbError::Transport {
                source: smb2::Error::invalid_data("share went away"),
            });
        }
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let end = start
            .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
            .min(self.data.len());
        if start >= self.data.len() {
            return Ok(Vec::new());
        }
        Ok(self.data[start..end].to_vec())
    }

    async fn close(self) -> Result<(), SmbError> {
        Ok(())
    }
}

/// How a fake connector answers `open`.
#[derive(Clone)]
enum OpenOutcome {
    Ok,
    AuthRequired,
    NotFound,
    AccessDenied,
    Transport,
}

/// A `SmbConnector` double: guest-accepts (or rejects, per `outcome`) any
/// parsed URL and returns [`FakeFile`] over `data`.
struct FakeConnector {
    outcome: OpenOutcome,
    data: Vec<u8>,
    /// When set, every read on the opened file fails.
    read_error: bool,
}

impl FakeConnector {
    fn guest(data: Vec<u8>) -> Self {
        Self {
            outcome: OpenOutcome::Ok,
            data,
            read_error: false,
        }
    }

    fn failing(outcome: OpenOutcome) -> Self {
        Self {
            outcome,
            data: Vec::new(),
            read_error: false,
        }
    }

    fn with_read_error(data: Vec<u8>) -> Self {
        Self {
            outcome: OpenOutcome::Ok,
            data,
            read_error: true,
        }
    }
}

impl SmbConnector<FakeFile> for FakeConnector {
    fn open(&self, url: &SmbUrl) -> impl Future<Output = Result<FakeFile, SmbError>> + Send {
        let outcome = self.outcome.clone();
        let data = self.data.clone();
        let read_error = self.read_error;
        let url = url.clone();
        async move {
            // The parsed type can never carry credentials, so an anonymous
            // open is structural; this only sanity-checks the fields that
            // were parsed.
            assert!(!url.host.is_empty());
            assert!(!url.share.is_empty());
            assert!(!url.file_path.is_empty());
            match outcome {
                OpenOutcome::Ok => Ok(FakeFile { data, read_error }),
                OpenOutcome::AuthRequired => Err(SmbError::AuthRequired {
                    detail: "guest logon rejected".to_string(),
                }),
                OpenOutcome::NotFound => Err(SmbError::NotFound {
                    detail: "share not found".to_string(),
                }),
                OpenOutcome::AccessDenied => Err(SmbError::AccessDenied {
                    detail: "file locked".to_string(),
                }),
                OpenOutcome::Transport => Err(SmbError::Transport {
                    source: smb2::Error::invalid_data("server gone"),
                }),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Response harness
// ---------------------------------------------------------------------------

/// A parsed HTTP/1.1 response written by [`smb_source::serve`].
struct RawResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn parse_response(bytes: &[u8]) -> RawResponse {
    let head_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response head terminator")
        + 4;
    let head = std::str::from_utf8(&bytes[..head_end]).expect("ascii head");
    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("status line");
    let mut parts = status_line.splitn(3, ' ');
    assert_eq!(parts.next(), Some("HTTP/1.1"));
    let status = parts.next().expect("status code").parse().expect("numeric");
    let mut headers = HashMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').expect("header colon");
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    RawResponse {
        status,
        headers,
        body: bytes[head_end..].to_vec(),
    }
}

/// Run one `serve` call over a real TCP pair and return the response bytes
/// plus the serve result. The client side never sends a request: `serve` is
/// the handler, so it only writes.
async fn serve_once<C, F>(
    connector: C,
    url: &str,
    range: Option<&str>,
    head_only: bool,
) -> (io::Result<()>, RawResponse)
where
    C: SmbConnector<F> + Send + 'static,
    F: SmbFile + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let url = url.to_string();
    let range = range.map(str::to_string);
    let serve_result = tokio::spawn(async move {
        let (stream, _peer) = listener.accept().await.expect("accept");
        let (_read_half, write_half) = stream.into_split();
        let mut writer = BufWriter::new(write_half);
        smb_source::serve(&mut writer, &connector, &url, range.as_deref(), head_only).await
    });
    let stream = TcpStream::connect(addr).await.expect("connect");
    let (read_half, _write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.expect("read");
    let serve_result = serve_result.await.expect("task");
    // A mid-stream abort can leave the peer with a truncated head (the
    // response head is still buffered when the body loop fails); tolerate
    // that in the harness and let the test assert what it saw.
    let response = if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        parse_response(&bytes)
    } else {
        RawResponse {
            status: 0,
            headers: HashMap::new(),
            body: bytes,
        }
    };
    (serve_result, response)
}

// ---------------------------------------------------------------------------
// URL parsing
// ---------------------------------------------------------------------------

#[test]
fn parses_host_share_and_path() {
    let url = SmbUrl::parse("smb://nas.local/media/dir/video.mp4").expect("parse");
    assert_eq!(
        url,
        SmbUrl {
            host: "nas.local".to_string(),
            port: 445,
            share: "media".to_string(),
            file_path: "dir/video.mp4".to_string(),
        }
    );
}

#[test]
fn defaults_port_to_445() {
    let url = SmbUrl::parse("smb://192.168.1.50/share/f.mp4").expect("parse");
    assert_eq!(url.port, 445);
}

#[test]
fn honors_explicit_port() {
    let url = SmbUrl::parse("smb://host:1445/share/f.mp4").expect("parse");
    assert_eq!(url.port, 1445);
}

#[test]
fn percent_decodes_share_and_path() {
    let url = SmbUrl::parse("smb://host/My%20Share/My%20Movie.mp4").expect("parse");
    assert_eq!(url.share, "My Share");
    assert_eq!(url.file_path, "My Movie.mp4");
}

#[test]
fn scheme_is_case_insensitive() {
    let url = SmbUrl::parse("SMB://HOST/SHARE/F.MP4").expect("parse");
    assert_eq!(url.host, "HOST");
}

#[test]
fn rejects_userinfo() {
    for raw in [
        "smb://user@host/share/f.mp4",
        "smb://user:pass@host/share/f.mp4",
    ] {
        let error = SmbUrl::parse(raw).expect_err("must reject userinfo");
        assert!(
            error.to_string().contains("userinfo"),
            "unexpected message: {error}"
        );
    }
}

#[test]
fn rejects_missing_host() {
    assert!(SmbUrl::parse("smb:///share/f.mp4").is_err());
}

#[test]
fn rejects_missing_share_or_file_path() {
    assert!(SmbUrl::parse("smb://host").is_err());
    assert!(SmbUrl::parse("smb://host/").is_err());
    assert!(SmbUrl::parse("smb://host/share").is_err());
    assert!(SmbUrl::parse("smb://host/share/").is_err());
}

#[test]
fn rejects_query_and_fragment() {
    assert!(SmbUrl::parse("smb://host/share/f.mp4?token=1").is_err());
    assert!(SmbUrl::parse("smb://host/share/f.mp4#frag").is_err());
}

#[test]
fn rejects_non_smb_schemes() {
    assert!(SmbUrl::parse("http://host/share/f.mp4").is_err());
    assert!(SmbUrl::parse("file:///etc/passwd").is_err());
}

#[test]
fn is_smb_url_detects_scheme_prefix() {
    assert!(smb_source::is_smb_url("smb://host/share/f.mp4"));
    assert!(smb_source::is_smb_url("  SMB://host/share/f.mp4"));
    assert!(!smb_source::is_smb_url("http://host/share/f.mp4"));
    assert!(!smb_source::is_smb_url("smbx://host/share/f.mp4"));
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

const SAMPLE: &[u8] = b"network share media payload";

#[tokio::test]
async fn serves_200_with_full_body() {
    let mut data = SAMPLE.to_vec();
    // Cross the 1 MiB READ_CHUNK boundary to exercise multi-chunk streaming.
    data.extend(std::iter::repeat_n(0xAB, 2 * 1024 * 1024));
    let (result, response) = serve_once(
        FakeConnector::guest(data.clone()),
        "smb://nas/media/video.mp4",
        None,
        false,
    )
    .await;
    result.expect("serve ok");
    assert_eq!(response.status, 200);
    assert_eq!(response.headers["content-type"], "video/mp4");
    assert_eq!(response.headers["accept-ranges"], "bytes");
    assert_eq!(response.headers["content-length"], data.len().to_string());
    assert_eq!(response.body.len(), data.len());
    assert_eq!(response.body, data);
}

#[tokio::test]
async fn serves_206_for_closed_range() {
    let data = vec![0x42; 1024 * 1024];
    let (result, response) = serve_once(
        FakeConnector::guest(data.clone()),
        "smb://nas/media/clip.mp4",
        Some("bytes=100000-199999"),
        false,
    )
    .await;
    result.expect("serve ok");
    assert_eq!(response.status, 206);
    assert_eq!(
        response.headers["content-range"],
        "bytes 100000-199999/1048576"
    );
    assert_eq!(response.body.len(), 100000);
    assert_eq!(response.body, data[100000..200000].to_vec());
}

#[tokio::test]
async fn serves_206_for_open_ended_range() {
    let data = vec![0x2A; 4096];
    let (result, response) = serve_once(
        FakeConnector::guest(data.clone()),
        "smb://nas/media/audio.mp3",
        Some("bytes=1000-"),
        false,
    )
    .await;
    result.expect("serve ok");
    assert_eq!(response.status, 206);
    assert_eq!(response.headers["content-range"], "bytes 1000-4095/4096");
    assert_eq!(response.body, data[1000..].to_vec());
}

#[tokio::test]
async fn serves_206_for_suffix_range() {
    let data = vec![0x5A; 2048];
    let (result, response) = serve_once(
        FakeConnector::guest(data.clone()),
        "smb://nas/media/tail.mp4",
        Some("bytes=-500"),
        false,
    )
    .await;
    result.expect("serve ok");
    assert_eq!(response.status, 206);
    assert_eq!(response.headers["content-range"], "bytes 1548-2047/2048");
    assert_eq!(response.body, data[1548..].to_vec());
}

#[tokio::test]
async fn serves_416_for_unsatisfiable_range() {
    let data = SAMPLE.to_vec();
    let (result, response) = serve_once(
        FakeConnector::guest(data.clone()),
        "smb://nas/media/video.mp4",
        Some("bytes=99999999-"),
        false,
    )
    .await;
    result.expect("serve ok");
    assert_eq!(response.status, 416);
    assert_eq!(
        response.headers["content-range"],
        format!("bytes */{}", data.len())
    );
    assert!(response.body.is_empty());
}

#[tokio::test]
async fn head_returns_headers_without_body() {
    let data = SAMPLE.to_vec();
    let (result, response) = serve_once(
        FakeConnector::guest(data.clone()),
        "smb://nas/media/video.mp4",
        None,
        true,
    )
    .await;
    result.expect("serve ok");
    assert_eq!(response.status, 200);
    assert_eq!(response.headers["content-length"], data.len().to_string());
    assert!(response.body.is_empty());
}

#[tokio::test]
async fn rejects_auth_required_share_with_401() {
    let (result, response) = serve_once(
        FakeConnector::failing(OpenOutcome::AuthRequired),
        "smb://nas/private/video.mp4",
        None,
        false,
    )
    .await;
    result.expect("serve ok");
    assert_eq!(response.status, 401);
    assert!(
        response
            .body
            .windows(b"authentication required".len())
            .any(|window| window == b"authentication required"),
        "401 body must say authentication is required"
    );
}

#[tokio::test]
async fn serves_404_for_missing_share_or_file() {
    let (result, response) = serve_once(
        FakeConnector::failing(OpenOutcome::NotFound),
        "smb://nas/media/missing.mp4",
        None,
        false,
    )
    .await;
    result.expect("serve ok");
    assert_eq!(response.status, 404);
}

#[tokio::test]
async fn serves_403_for_access_denied() {
    let (result, response) = serve_once(
        FakeConnector::failing(OpenOutcome::AccessDenied),
        "smb://nas/media/locked.mp4",
        None,
        false,
    )
    .await;
    result.expect("serve ok");
    assert_eq!(response.status, 403);
}

#[tokio::test]
async fn serves_502_for_transport_failure() {
    let (result, response) = serve_once(
        FakeConnector::failing(OpenOutcome::Transport),
        "smb://nas/media/video.mp4",
        None,
        false,
    )
    .await;
    result.expect("serve ok");
    assert_eq!(response.status, 502);
}

#[tokio::test]
async fn serves_400_for_invalid_urls() {
    for raw in [
        "smb://host/share",             // no file path
        "smb://user:pass@host/s/f.mp4", // credentials
        "http://host/share/f.mp4",      // wrong scheme
    ] {
        let (result, response) =
            serve_once(FakeConnector::guest(Vec::new()), raw, None, false).await;
        result.expect("serve ok");
        assert_eq!(response.status, 400, "URL {raw} must yield 400");
    }
}

#[tokio::test]
async fn mid_stream_read_error_aborts_the_connection() {
    let connector = FakeConnector::with_read_error(vec![0x11; 4096]);
    let (result, response) = serve_once(connector, "smb://nas/media/video.mp4", None, false).await;
    let error = result.expect_err("a failing read mid-body must abort the stream");
    assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    // The head is still buffered when the read fails, so the peer sees no
    // complete response (the same truncation signal local-file serving
    // gives mid-stream).
    assert!(response.body.is_empty());
}

#[tokio::test]
async fn range_read_requests_carry_offset_and_length() {
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let requests_for_task = requests.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let serve_task = tokio::spawn(async move {
        let (stream, _peer) = listener.accept().await.expect("accept");
        let (_read_half, write_half) = stream.into_split();
        let mut writer = BufWriter::new(write_half);
        let connector = RecordingConnector {
            requests: requests_for_task,
        };
        smb_source::serve(
            &mut writer,
            &connector,
            "smb://nas/media/video.mp4",
            Some("bytes=10-19"),
            false,
        )
        .await
    });
    let stream = TcpStream::connect(addr).await.expect("connect");
    let (read_half, _write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.expect("read");
    serve_task.await.expect("task").expect("serve ok");
    let requests = requests.lock().expect("lock");
    assert_eq!(
        requests.as_slice(),
        &[(10, 10)],
        "the body loop must translate the Range into one positioned read"
    );
    let response = parse_response(&bytes);
    assert_eq!(response.status, 206);
    assert_eq!(response.body, b"0123456789".to_vec());
}

/// Records `(offset, len)` of every `read_at` so tests can assert the Range
/// header was translated into positioned reads.
struct RecordingConnector {
    requests: Arc<std::sync::Mutex<Vec<(u64, u64)>>>,
}

struct RecordingFile {
    data: Vec<u8>,
    requests: Arc<std::sync::Mutex<Vec<(u64, u64)>>>,
}

impl SmbFile for RecordingFile {
    fn size(&self) -> u64 {
        self.data.len() as u64
    }

    async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, SmbError> {
        self.requests.lock().expect("lock").push((offset, len));
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let end = start
            .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
            .min(self.data.len());
        if start >= self.data.len() {
            return Ok(Vec::new());
        }
        Ok(self.data[start..end].to_vec())
    }

    async fn close(self) -> Result<(), SmbError> {
        Ok(())
    }
}

impl SmbConnector<RecordingFile> for RecordingConnector {
    async fn open(&self, _url: &SmbUrl) -> Result<RecordingFile, SmbError> {
        Ok(RecordingFile {
            data: b"01234567890123456789".to_vec(),
            requests: self.requests.clone(),
        })
    }
}
