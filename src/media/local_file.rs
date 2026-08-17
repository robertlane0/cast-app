// SPDX-License-Identifier: MIT OR Apache-2.0
//! Local-file streaming with `200`/`206`/`416` semantics, 64 KiB chunked
//! reads, and `HEAD` support (`04-media-proxy.md` §3).

use std::io;
use std::path::Path;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::media::mime::mime_for_path;
use crate::media::range::{RangeDecision, content_range, unsatisfiable_content_range};
use crate::media::server::write_response_head;

/// Fixed chunk size for streaming file bytes (`04-media-proxy.md` §3): 64 KiB.
pub const CHUNK_SIZE: usize = 64 * 1024;

/// Serve `path` on one `/stream` connection (`04-media-proxy.md` §3):
///
/// - no `Range` / multi-range -> `200 OK` with the full body;
/// - valid single range -> `206 Partial Content` + `Content-Range`;
/// - unsatisfiable/malformed -> `416 Range Not Satisfiable`;
/// - missing file -> `404 Not Found`;
/// - `HEAD` -> the same headers as `GET` with no body.
///
/// A missing file is the only pre-stream error handled here; mid-stream I/O
/// errors propagate to the caller (which aborts the connection).
pub async fn serve<W>(
    writer: &mut W,
    path: &Path,
    range_header: Option<&str>,
    head_only: bool,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            tracing::warn!(path = %path.display(), "stream request for a missing file");
            write_response_head(writer, 404, "Not Found", &[]).await?;
            return writer.flush().await;
        }
        Err(error) => return Err(error),
    };
    let size = file.metadata().await?.len();

    let mime = mime_for_path(path);
    let decision = crate::media::range::parse_range(range_header, size);
    match decision {
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
                stream_reader(writer, &mut file, size).await?;
            }
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
                file.seek(io::SeekFrom::Start(start)).await?;
                stream_reader(writer, &mut file, length).await?;
            }
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
        }
    }
    writer.flush().await
}

/// Stream exactly `remaining` bytes from the current file position in
/// [`CHUNK_SIZE`] chunks. A short/truncated read ends the stream early — the
/// mismatched `Content-Length` makes the client treat the response as
/// incomplete, which is the correct signal.
async fn stream_reader<W>(
    writer: &mut W,
    file: &mut tokio::fs::File,
    mut remaining: u64,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0u8; CHUNK_SIZE];
    while remaining > 0 {
        let want = usize::try_from(remaining.min(CHUNK_SIZE as u64)).unwrap_or(CHUNK_SIZE);
        let read = file.read(&mut buffer[..want]).await?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read]).await?;
        remaining -= read as u64;
    }
    Ok(())
}
