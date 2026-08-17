// SPDX-License-Identifier: MIT OR Apache-2.0
//! Anonymous-only network-share streaming for the `/stream` endpoint
//! (`04-media-proxy.md` §4.4): `smb://` URLs are served through a pure-Rust
//! SMB2 client with guest/anonymous credentials only. Shares that require
//! authentication are rejected with `401` — never prompted — mirroring the
//! URL proxy's no-userinfo rule (`04-media-proxy.md` §4.2).
//!
//! The protocol client is behind the [`SmbConnector`]/[`SmbFile`] traits so
//! the HTTP serving logic is testable with fakes; the production wiring uses
//! [`Smb2Connector`] (the `smb2` crate).

use std::io;
use std::time::Duration;

use percent_encoding::percent_decode_str;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::media::mime::mime_for_extension;
use crate::media::range::{RangeDecision, content_range, parse_range, unsatisfiable_content_range};
use crate::media::server::write_response_head;

/// Default SMB port when the URL omits one (`04-media-proxy.md` §4.4).
pub const DEFAULT_PORT: u16 = 445;

/// Deadline for the TCP connect + NEGOTIATE + SESSION_SETUP + TREE_CONNECT +
/// CREATE exchange (`04-media-proxy.md` §4.4). A dead share should surface as
/// a fast `502`, not a minutes-long hang.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bytes fetched per `read_at` in the body loop. SMB READ round trips are the
/// dominant cost on a LAN; 1 MiB amortizes them (the `smb2` crate splits a
/// large request into `MaxReadSize`-sized wire READs and pipelines them)
/// without adding meaningful latency for the receiver.
pub const READ_CHUNK: u64 = 1024 * 1024;

/// An `smb://` media-source URL, parsed and validated (`04-media-proxy.md`
/// §4.4). The type deliberately carries **no credentials**: anonymous-only
/// access is structural, so "unauthenticated only" cannot be violated by a
/// caller of [`serve`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmbUrl {
    /// Server host (or literal IPv6 address without brackets).
    pub host: String,
    /// Port (default [`DEFAULT_PORT`] when the URL omits it).
    pub port: u16,
    /// Share name (first path segment, percent-decoded).
    pub share: String,
    /// File path relative to the share root (percent-decoded, `/`-separated,
    /// no leading slash).
    pub file_path: String,
}

impl SmbUrl {
    /// Parse and validate a user-supplied `smb://` URL (`04-media-proxy.md`
    /// §4.4):
    ///
    /// - the scheme must be `smb`;
    /// - userinfo (`user:pass@`) is rejected — authentication is not
    ///   supported, and a URL that tries to supply it must fail;
    /// - a host, a share (first path segment) and a file path are all
    ///   required;
    /// - query strings and fragments are rejected (a share URL names one
    ///   file).
    pub fn parse(raw: &str) -> Result<Self, SmbError> {
        let parsed = url::Url::parse(raw).map_err(|source| SmbError::InvalidUrl {
            message: source.to_string(),
        })?;
        if !parsed.scheme().eq_ignore_ascii_case("smb") {
            return Err(SmbError::InvalidUrl {
                message: "scheme must be smb://".to_string(),
            });
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(SmbError::InvalidUrl {
                message: "userinfo (user:pass@) is rejected; network shares are anonymous-only"
                    .to_string(),
            });
        }
        let Some(host) = parsed.host_str() else {
            return Err(SmbError::InvalidUrl {
                message: "URL has no host".to_string(),
            });
        };
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(SmbError::InvalidUrl {
                message: "query strings and fragments are not supported".to_string(),
            });
        }
        let port = parsed.port().unwrap_or(DEFAULT_PORT);

        let decoded = percent_decode_str(parsed.path()).decode_utf8_lossy();
        let trimmed = decoded.trim_start_matches('/');
        let mut segments = trimmed.split('/');
        let share = segments.next().unwrap_or_default();
        let rest: Vec<&str> = segments.collect();
        if share.is_empty() || rest.is_empty() || rest.iter().any(|segment| segment.is_empty()) {
            return Err(SmbError::InvalidUrl {
                message: "URL must name a share and a file path: smb://host/share/dir/file.mp4"
                    .to_string(),
            });
        }
        Ok(Self {
            host: host.to_string(),
            port,
            share: share.to_string(),
            file_path: rest.join("/"),
        })
    }
}

/// Errors serving an `smb://` source (`04-media-proxy.md` §4.4).
#[derive(Debug, thiserror::Error)]
pub enum SmbError {
    #[error("invalid smb:// URL: {message}")]
    InvalidUrl { message: String },
    #[error("network share requires authentication; anonymous access was rejected: {detail}")]
    AuthRequired { detail: String },
    #[error("network share resource not found: {detail}")]
    NotFound { detail: String },
    #[error("network share access denied: {detail}")]
    AccessDenied { detail: String },
    #[error("network share transport error: {source}")]
    Transport {
        #[source]
        source: smb2::Error,
    },
}

/// One open file on a share: positioned reads over a single handle
/// (`04-media-proxy.md` §4.4). Mirrors the `smb2` crate's `FileReader`, but
/// behind a trait so serving logic is testable without a share.
pub trait SmbFile {
    /// Total file size in bytes, as seen when the handle was opened.
    fn size(&self) -> u64;
    /// Read up to `len` bytes at absolute `offset` (positioned like
    /// `pread`). Fewer bytes are returned only at end of file; an empty
    /// `Vec` means `offset` is at/past EOF.
    fn read_at(
        &self,
        offset: u64,
        len: u64,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, SmbError>> + Send;
    /// Release the server-side handle. Must be called when the caller is
    /// done; dropping without `close` leaks the handle until session
    /// teardown.
    fn close(self) -> impl std::future::Future<Output = Result<(), SmbError>> + Send;
}

/// Opens files on network shares with **anonymous credentials only**
/// (`04-media-proxy.md` §4.4): the connector never receives or stores a
/// password, and it fails with [`SmbError::AuthRequired`] when the server
/// rejects the guest logon.
pub trait SmbConnector<F: SmbFile>: Send + Sync {
    /// Connect anonymously and open `url.file_path` on `url.share`.
    fn open(&self, url: &SmbUrl) -> impl std::future::Future<Output = Result<F, SmbError>> + Send;
}

/// The production connector: a `smb2` crate client with empty credentials
/// (guest/anonymous) and no DFS referral (one deterministic hop to the host
/// in the URL).
#[derive(Debug, Clone, Copy, Default)]
pub struct Smb2Connector;

impl SmbConnector<Smb2File> for Smb2Connector {
    async fn open(&self, url: &SmbUrl) -> Result<Smb2File, SmbError> {
        let addr = if url.host.contains(':') {
            // Literal IPv6 address: brackets are required in the socket form.
            format!("[{}]:{}", url.host, url.port)
        } else {
            format!("{}:{}", url.host, url.port)
        };
        let config = smb2::ClientConfig {
            addr,
            timeout: CONNECT_TIMEOUT,
            // Empty credentials = guest/anonymous (documented by the crate).
            // Deliberately hard-coded: no userinfo can ever reach the client.
            username: String::new(),
            password: String::new(),
            domain: String::new(),
            auto_reconnect: false,
            compression: true,
            dfs_enabled: false,
            dfs_target_overrides: std::collections::HashMap::new(),
        };
        let mut client = smb2::SmbClient::connect(config).await.map_err(classify)?;
        let tree = client.connect_share(&url.share).await.map_err(classify)?;
        let reader = client
            .open_file_reader(&tree, &url.file_path)
            .await
            .map_err(classify)?;
        // The client itself drops here; the reader keeps the connection (and
        // therefore the guest session) alive via its `Arc<Connection>`.
        Ok(Smb2File { reader })
    }
}

/// The `smb2`-backed [`SmbFile`] produced by [`Smb2Connector`].
pub struct Smb2File {
    reader: smb2::FileReader,
}

impl SmbFile for Smb2File {
    fn size(&self) -> u64 {
        self.reader.size()
    }

    async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, SmbError> {
        self.reader.read_at(offset, len).await.map_err(classify)
    }

    async fn close(self) -> Result<(), SmbError> {
        self.reader.close().await.map_err(classify)
    }
}

/// Whether a raw URL string names an SMB source; the media server uses this
/// to dispatch before parsing (a prefix check is enough — the parser enforces
/// the details).
pub fn is_smb_url(raw: &str) -> bool {
    raw.trim().to_ascii_lowercase().starts_with("smb://")
}

/// Map a `smb2` error onto the HTTP-facing [`SmbError`] (`04-media-proxy.md`
/// §4.4). The classification is the crate's own: a rejected guest logon
/// (logon failure, account restriction, ...) is [`smb2::ErrorKind::AuthRequired`],
/// which is precisely the "this share needs credentials" signal.
fn classify(error: smb2::Error) -> SmbError {
    let detail = error.to_string();
    match error.kind() {
        smb2::ErrorKind::AuthRequired => SmbError::AuthRequired { detail },
        smb2::ErrorKind::NotFound => SmbError::NotFound { detail },
        smb2::ErrorKind::AccessDenied => SmbError::AccessDenied { detail },
        _ => SmbError::Transport { source: error },
    }
}

/// Serve one `/stream` connection for an `smb://` source (`04-media-proxy.md`
/// §4.4), with the same status semantics as local-file serving
/// (`04-media-proxy.md` §3):
///
/// - no `Range` / multi-range -> `200 OK` with the full body;
/// - valid single range -> `206 Partial Content` + `Content-Range`;
/// - unsatisfiable/malformed -> `416 Range Not Satisfiable`;
/// - invalid `smb://` URL -> `400`;
/// - share/file missing -> `404`;
/// - server denies guest access (authentication required) -> `401`;
/// - other transport failures -> `502`;
/// - `HEAD` -> the same headers as `GET` with no body.
///
/// A fresh anonymous session is opened per request; the handle is closed on
/// the happy path, and dropped (leaking until session teardown) on a
/// mid-stream error.
pub async fn serve<W, C, F>(
    writer: &mut W,
    connector: &C,
    raw_url: &str,
    range_header: Option<&str>,
    head_only: bool,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    C: SmbConnector<F> + ?Sized,
    F: SmbFile,
{
    let url = match SmbUrl::parse(raw_url) {
        Ok(url) => url,
        Err(error) => {
            tracing::warn!(%error, "rejecting invalid smb URL at request time");
            write_response_head(writer, 400, "Bad Request", &[]).await?;
            return writer.flush().await;
        }
    };

    let file = match connector.open(&url).await {
        Ok(file) => file,
        Err(SmbError::AuthRequired { detail }) => {
            // (FR-034) Anonymous-only: never prompt, never retry with
            // credentials — the failure is permanent for this source.
            tracing::warn!(%detail, "network share requires authentication; refusing");
            return plain_error(
                writer,
                401,
                "Unauthorized",
                "guest access rejected: authentication required",
            )
            .await;
        }
        Err(SmbError::NotFound { detail }) => {
            tracing::warn!(%detail, "network share resource not found");
            return plain_error(writer, 404, "Not Found", "network share resource not found").await;
        }
        Err(SmbError::AccessDenied { detail }) => {
            tracing::warn!(%detail, "network share access denied");
            return plain_error(writer, 403, "Forbidden", "network share access denied").await;
        }
        Err(error) => {
            tracing::warn!(%error, "network share fetch failed; returning 502");
            return plain_error(writer, 502, "Bad Gateway", "Bad Gateway").await;
        }
    };

    let size = file.size();
    let mime = mime_for_extension(file_extension(&url.file_path));
    let decision = parse_range(range_header, size);

    let result: io::Result<()> = match decision {
        RangeDecision::Full => {
            write_response_head(
                writer,
                200,
                "OK",
                &[
                    ("Accept-Ranges", "bytes"),
                    ("Content-Type", mime),
                    ("Content-Length", &size.to_string()),
                    ("Cache-Control", "no-cache"),
                ],
            )
            .await?;
            if !head_only {
                stream_reader(writer, &file, 0, size).await?;
            }
            Ok(())
        }
        RangeDecision::Partial { start, end } => {
            let length = end - start + 1;
            write_response_head(
                writer,
                206,
                "Partial Content",
                &[
                    ("Accept-Ranges", "bytes"),
                    ("Content-Type", mime),
                    ("Content-Length", &length.to_string()),
                    ("Content-Range", &content_range(start, end, size)),
                    ("Cache-Control", "no-cache"),
                ],
            )
            .await?;
            if !head_only {
                stream_reader(writer, &file, start, length).await?;
            }
            Ok(())
        }
        RangeDecision::Unsatisfiable => {
            write_response_head(
                writer,
                416,
                "Range Not Satisfiable",
                &[
                    ("Accept-Ranges", "bytes"),
                    ("Content-Type", mime),
                    ("Content-Length", "0"),
                    ("Content-Range", &unsatisfiable_content_range(size)),
                ],
            )
            .await?;
            Ok(())
        }
    };

    result?;

    // The body (if any) is fully written; releasing the handle is best-effort.
    if let Err(error) = file.close().await {
        tracing::warn!(%error, "smb file handle close failed");
    }
    writer.flush().await
}

/// Stream exactly `remaining` bytes from `start` in [`READ_CHUNK`] chunks.
/// A short read (the file shrank since the handle opened) ends the stream
/// early — the mismatched `Content-Length` makes the client treat the
/// response as incomplete, the same signal local-file serving uses.
async fn stream_reader<W, F>(
    writer: &mut W,
    file: &F,
    start: u64,
    mut remaining: u64,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    F: SmbFile,
{
    let mut offset = start;
    while remaining > 0 {
        let want = remaining.min(READ_CHUNK);
        let chunk = file
            .read_at(offset, want)
            .await
            .map_err(|error| io::Error::new(io::ErrorKind::ConnectionAborted, error))?;
        if chunk.is_empty() {
            break;
        }
        writer.write_all(&chunk).await?;
        offset += chunk.len() as u64;
        remaining -= chunk.len() as u64;
    }
    Ok(())
}

/// The lowercase file extension of a share-relative path, for MIME lookup.
fn file_extension(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or_default()
}

/// A status response with a tiny text body; used for all pre-stream failures.
async fn plain_error<W>(writer: &mut W, status: u16, reason: &str, body: &str) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_response_head(
        writer,
        status,
        reason,
        &[
            ("Content-Type", "text/plain"),
            ("Content-Length", &body.len().to_string()),
        ],
    )
    .await?;
    writer.write_all(body.as_bytes()).await?;
    writer.flush().await
}
