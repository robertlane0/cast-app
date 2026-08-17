// SPDX-License-Identifier: MIT OR Apache-2.0
//! Remote-URL proxying for the `/stream` endpoint (`04-media-proxy.md` §4):
//! `reqwest` GET with `Range` forwarding, ≤5 redirects, a 30 s first-byte
//! timeout with no overall streaming limit, non-2xx pass-through, and `502`
//! on remote connection failure.

use std::io;
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

use crate::media::server::{reason_phrase, write_response_head};

/// Connect + first-byte deadline (`04-media-proxy.md` §4.2). There is no
/// overall response limit while the body streams.
pub const FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum redirects followed by the proxy (`04-media-proxy.md` §4.2).
pub const MAX_REDIRECTS: usize = 5;

/// A shared `reqwest` client for all `/stream` URL-source connections.
#[derive(Debug, Clone)]
pub struct UrlProxy {
    client: reqwest::Client,
}

/// Errors validating a user-supplied remote URL (`04-media-proxy.md` §4.2).
#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("URL is not valid: {source}")]
    InvalidUrl {
        #[source]
        source: url::ParseError,
    },
    #[error("URL contains userinfo (user:pass@) and is rejected")]
    Userinfo,
    #[error("URL has no host")]
    MissingHost,
}

impl UrlProxy {
    /// Build a client with the spec's redirect policy. The per-request
    /// first-byte timeout is applied around each `send`, not on the client,
    /// so streaming bodies are never cut off mid-transfer.
    pub fn new() -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::limited(MAX_REDIRECTS))
            .build()?;
        Ok(Self { client })
    }

    /// Validate a remote URL at source-switch time: must parse, must have a
    /// host, and must not carry userinfo (`04-media-proxy.md` §4.2).
    pub fn validate_url(&self, raw: &str) -> Result<reqwest::Url, ProxyError> {
        let url = reqwest::Url::parse(raw).map_err(|source| ProxyError::InvalidUrl { source })?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(ProxyError::Userinfo);
        }
        if url.host_str().is_none() {
            return Err(ProxyError::MissingHost);
        }
        Ok(url)
    }

    /// Serve one `/stream` connection for the active URL source
    /// (`04-media-proxy.md` §4):
    ///
    /// - the request's `Range` header is forwarded (nothing else);
    /// - the remote status line, `Content-Type`, `Content-Length`,
    ///   `Content-Range` and `Accept-Ranges` are mirrored to the receiver —
    ///   including `206`/`416` from the origin;
    /// - non-2xx statuses pass through with their body;
    /// - a failed/aborted outbound request yields `502 Bad Gateway`;
    /// - a `HEAD` request forwards headers but not the body.
    pub async fn serve<W>(
        &self,
        writer: &mut W,
        raw_url: &str,
        range_header: Option<&str>,
        head_only: bool,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let url = match self.validate_url(raw_url) {
            Ok(url) => url,
            Err(error) => {
                tracing::warn!(%error, "rejecting invalid active URL at request time");
                write_response_head(writer, 400, "Bad Request", &[]).await?;
                return writer.flush().await;
            }
        };

        let mut request = self.client.get(url);
        if let Some(range) = range_header {
            request = request.header(reqwest::header::RANGE, range);
        }

        let mut response = match timeout(FIRST_BYTE_TIMEOUT, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                // Connection refused / DNS / TLS failure.
                tracing::warn!(%error, "remote fetch failed; returning 502");
                return bad_gateway(writer).await;
            }
            Err(_) => {
                tracing::warn!("remote fetch exceeded the first-byte timeout; returning 502");
                return bad_gateway(writer).await;
            }
        };

        let status = response.status();
        let mut headers: Vec<(String, String)> = Vec::with_capacity(4);
        for name in [
            "content-type",
            "content-length",
            "content-range",
            "accept-ranges",
        ] {
            if let Some(value) = response.headers().get(name) {
                if let Ok(value) = value.to_str() {
                    headers.push((name.to_string(), value.to_string()));
                }
            }
        }
        let header_refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        write_response_head(
            writer,
            status.as_u16(),
            reason_phrase(status.as_u16()),
            &header_refs,
        )
        .await?;

        if head_only {
            return writer.flush().await;
        }

        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| io::Error::new(io::ErrorKind::ConnectionAborted, error))?
        {
            writer.write_all(&chunk).await?;
            // Remote chunks are small; flush each so the receiver sees
            // steady progress instead of 8 KiB bursts.
            writer.flush().await?;
        }
        writer.flush().await
    }
}

async fn bad_gateway<W>(writer: &mut W) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_response_head(
        writer,
        502,
        "Bad Gateway",
        &[("Content-Type", "text/plain"), ("Content-Length", "11")],
    )
    .await?;
    writer.write_all(b"Bad Gateway").await?;
    writer.flush().await
}
