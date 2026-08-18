// SPDX-License-Identifier: MIT OR Apache-2.0
//! The local HTTP/1.1 media server (`04-media-proxy.md` §2): async accept
//! loop, request-line + header parsing, `/stream` routing, per-source
//! serving (file / URL proxy / live screen), source-switch cancellation of
//! in-flight connections, and address/port rebinding.
//!
//! The listener binds a specific interface address (the interface resolved
//! for the current receiver, `04-media-proxy.md` §1.1), never the wildcard
//! on its own: the app path starts unbound and the runtime drives every bind
//! via [`MediaServer::set_bind_addr`], falling back to `0.0.0.0` only after
//! explicit user consent. The legacy [`MediaServer::start`] constructor
//! (wildcard at spawn) exists for standalone tests.

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

use crate::media::flush::FlushTracker;
use crate::media::local_file;
use crate::media::smb_source::{self, Smb2Connector};
use crate::media::source::ActiveSource;
use crate::media::url_proxy::UrlProxy;
use crate::util::shutdown::Shutdown;

/// Default proxy port (`04-media-proxy.md` §1.1).
pub const DEFAULT_PORT: u16 = 8080;

/// Deadline for reading the request head (request line + headers) so a slow
/// or dead client cannot pin a task forever.
const REQUEST_HEAD_TIMEOUT: Duration = Duration::from_secs(30);

/// Accumulated bytes before the live screen loop flushes the writer.
/// Encoder chunks are small; flushing each would fragment the stream into
/// tiny TCP segments and burn a syscall per chunk. 64 KiB amortizes those
/// costs without delaying fragment delivery noticeably.
const LIVE_FLUSH_BYTES: usize = 64 * 1024;

/// Longest the live screen loop holds buffered bytes before flushing,
/// bounding the added mirroring latency when the encoder emits slowly.
const LIVE_FLUSH_INTERVAL: Duration = Duration::from_millis(50);

/// Maximum length of the request line.
const MAX_REQUEST_LINE: usize = 8 * 1024;
/// Maximum length of a single header line.
const MAX_HEADER_LINE: usize = 8 * 1024;
/// Maximum number of request headers.
const MAX_HEADERS: usize = 64;

/// Commands accepted by the media-server task.
#[derive(Debug)]
pub enum ServerCommand {
    /// Switch the active `/stream` source; bumps the generation so every
    /// in-flight connection aborts (`04-media-proxy.md` §1.2).
    SetSource(ActiveSource),
    /// Attach the live screen-encoder byte stream (Phase 8 bridge output).
    AttachScreenStream(mpsc::Receiver<Vec<u8>>),
    /// Rebind the listener to the current bind address on `port`
    /// (`04-media-proxy.md` §1.1). No-op (with a warning) while the server
    /// is unbound — a bind address must be configured first.
    SetPort(u16),
    /// Rebind the listener to `addr` — the interface address resolved for
    /// the current receiver, or `0.0.0.0` after explicit user consent. The
    /// old listener is dropped before the new bind (a same-port rebind from
    /// the wildcard to a specific address would otherwise hit `EADDRINUSE`);
    /// a failed bind is reported through the ack and leaves the server
    /// unbound.
    SetBindAddr(SocketAddr, tokio::sync::oneshot::Sender<io::Result<()>>),
    /// Stop the server task.
    Shutdown,
}

/// Handle for driving the media-server task; all methods are non-blocking.
#[derive(Debug, Clone)]
pub struct MediaServer {
    commands: mpsc::UnboundedSender<ServerCommand>,
    /// The currently bound port (0 until the first successful bind).
    bound_port: watch::Receiver<u16>,
    /// Bumped after each source change / screen-stream attach is processed.
    generation: watch::Sender<u64>,
}

impl MediaServer {
    /// Spawn the media-server task on the current tokio runtime, binding
    /// `port` on the wildcard address (`04-media-proxy.md` §2; port 0 picks
    /// an ephemeral port). Test/standalone constructor: the app path uses
    /// [`MediaServer::start_unbound_with_handle`] so the runtime can gate
    /// every bind on user consent (`04-media-proxy.md` §1.1).
    pub fn start(shutdown: Shutdown, port: u16) -> Self {
        Self::start_with_handle(shutdown, port).0
    }

    /// Spawn the media-server task, returning the task handle so the runtime
    /// supervisor can await listener release during shutdown. Binds the
    /// wildcard address immediately (test/standalone constructor).
    pub fn start_with_handle(shutdown: Shutdown, port: u16) -> (Self, tokio::task::JoinHandle<()>) {
        let (commands, receiver) = mpsc::unbounded_channel();
        let (port_tx, bound_port) = watch::channel(0);
        let (generation_tx, _) = watch::channel(0u64);
        let initial = (IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), port);
        let handle = tokio::spawn(run(
            receiver,
            shutdown,
            Some(initial),
            port_tx,
            generation_tx.clone(),
        ));
        (
            Self {
                commands,
                bound_port,
                generation: generation_tx,
            },
            handle,
        )
    }

    /// Spawn the media-server task with **no listener**. Every bind is
    /// driven by [`MediaServer::set_bind_addr`]; `bound_port()` stays 0 and
    /// `SetPort` is a no-op until the first bind address is configured.
    pub fn start_unbound_with_handle(shutdown: Shutdown) -> (Self, tokio::task::JoinHandle<()>) {
        let (commands, receiver) = mpsc::unbounded_channel();
        let (port_tx, bound_port) = watch::channel(0);
        let (generation_tx, _) = watch::channel(0u64);
        let handle = tokio::spawn(run(
            receiver,
            shutdown,
            None,
            port_tx,
            generation_tx.clone(),
        ));
        (
            Self {
                commands,
                bound_port,
                generation: generation_tx,
            },
            handle,
        )
    }

    /// Switch the active source, aborting all in-flight `/stream`
    /// connections (`04-media-proxy.md` §1.2).
    pub fn set_source(&self, source: ActiveSource) {
        let _ = self.commands.send(ServerCommand::SetSource(source));
    }

    /// Attach the live screen-encoder stream. The receiver is served to the
    /// next `/stream` request while a `Screen` source is active.
    pub fn attach_screen_stream(&self, receiver: mpsc::Receiver<Vec<u8>>) {
        let _ = self
            .commands
            .send(ServerCommand::AttachScreenStream(receiver));
    }

    /// Rebind the listener to `port` (`04-media-proxy.md` §1.1). A failed
    /// bind keeps the current listener and logs a warning. While the server
    /// is unbound (no bind address configured yet) this is a no-op.
    pub fn set_port(&self, port: u16) {
        let _ = self.commands.send(ServerCommand::SetPort(port));
    }

    /// Rebind the listener to `addr` — the interface address resolved for
    /// the current receiver, or the wildcard `0.0.0.0` after explicit user
    /// consent (`04-media-proxy.md` §1.1). Non-blocking: the result is
    /// reported through `ack` once the server task has processed the
    /// command. The old listener is dropped before the new bind (the same
    /// port on a specific address conflicts with a wildcard listener), so a
    /// failed rebind leaves the server without a listener.
    pub fn set_bind_addr(
        &self,
        addr: SocketAddr,
        ack: tokio::sync::oneshot::Sender<io::Result<()>>,
    ) {
        let _ = self.commands.send(ServerCommand::SetBindAddr(addr, ack));
    }

    /// Stop the server task and release the listener.
    pub fn shutdown(&self) {
        let _ = self.commands.send(ServerCommand::Shutdown);
    }

    /// The currently bound port, or 0 before the first bind completes.
    pub fn bound_port(&self) -> u16 {
        *self.bound_port.borrow()
    }

    /// Subscribe to bound-port changes (used by tests and the runtime to
    /// learn the ephemeral port after a `SetPort`).
    pub fn subscribe_port(&self) -> watch::Receiver<u16> {
        self.bound_port.clone()
    }

    /// Subscribe to source-generation changes. Every `SetSource` (and
    /// screen-stream attach) bumps the generation after the server has
    /// processed it — a deterministic acknowledgement for tests and the
    /// runtime.
    pub fn subscribe_generation(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }
}

/// The media-server task: owns the listener and the current source; bumps a
/// generation watch on every source switch so in-flight handlers abort.
/// `initial_bind` is the startup listener address (`None` = start unbound;
/// the runtime then drives every bind through `SetBindAddr`, gating the
/// wildcard fallback on user consent, `04-media-proxy.md` §1.1).
async fn run(
    mut commands: mpsc::UnboundedReceiver<ServerCommand>,
    shutdown: Shutdown,
    initial_bind: Option<(IpAddr, u16)>,
    port_tx: watch::Sender<u16>,
    generation_tx: watch::Sender<u64>,
) {
    let mut shutdown_rx = shutdown.subscribe();

    // Client construction cannot fail with the rustls backend (reqwest
    // docs); this is init-time, and a failure here leaves nothing to serve.
    let proxy = match UrlProxy::new() {
        Ok(proxy) => Arc::new(proxy),
        Err(error) => {
            tracing::error!(%error, "failed to build the URL proxy client; media server aborted");
            return;
        }
    };

    let live_rx: Arc<Mutex<Option<mpsc::Receiver<Vec<u8>>>>> = Arc::new(Mutex::new(None));

    // The interface address the listener is bound to; `None` until the
    // first `SetBindAddr`, so a `SetPort` cannot create a wildcard listener
    // behind the runtime's back.
    let mut bind_ip: Option<IpAddr> = initial_bind.map(|(ip, _)| ip);
    let mut listener = match initial_bind {
        Some((ip, port)) => match bind(ip, port).await {
            Ok(listener) => {
                // Report the actual bound port (differs from `port` when an
                // ephemeral port (0) was requested).
                let actual = listener
                    .local_addr()
                    .map(|addr| addr.port())
                    .unwrap_or(port);
                let _ = port_tx.send(actual);
                Some(listener)
            }
            Err(error) => {
                tracing::error!(port, %error, "media server failed to bind; waiting for SetBindAddr");
                None
            }
        },
        None => None,
    };

    let mut current: Option<ActiveSource> = None;

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command {
                    ServerCommand::Shutdown => break,
                    ServerCommand::SetSource(source) => {
                        tracing::info!(source = %source.label(), "media source switched");
                        current = Some(source);
                        // Drop any unconsumed live screen stream: the new
                        // source replaces it; the encoder pipeline observes
                        // the closed channel.
                        lock(&live_rx).take();
                        generation_tx.send_modify(|generation| *generation += 1);
                    }
                    ServerCommand::AttachScreenStream(receiver) => {
                        *lock(&live_rx) = Some(receiver);
                        // Bump so callers can observe the attach (and any
                        // in-flight live connection re-subscribes cleanly).
                        generation_tx.send_modify(|generation| *generation += 1);
                    }
                    ServerCommand::SetPort(port) => {
                        let Some(ip) = bind_ip else {
                            tracing::warn!(port, "media server has no bind address; SetPort ignored (SetBindAddr first)");
                            continue;
                        };
                        match bind(ip, port).await {
                            Ok(new_listener) => {
                                listener = Some(new_listener);
                                let _ = port_tx.send(port);
                                tracing::info!(port, "media server rebound");
                            }
                            Err(error) => {
                                tracing::warn!(port, %error, "media server rebind failed; keeping current listener");
                            }
                        }
                    }
                    ServerCommand::SetBindAddr(addr, ack) => {
                        // Drop the current listener first: rebinding the
                        // same port from the wildcard to a specific address
                        // (or vice versa) conflicts while the old listener
                        // holds it.
                        listener = None;
                        match bind(addr.ip(), addr.port()).await {
                            Ok(new_listener) => {
                                let actual = new_listener
                                    .local_addr()
                                    .map(|local| local.port())
                                    .unwrap_or(addr.port());
                                bind_ip = Some(addr.ip());
                                listener = Some(new_listener);
                                let _ = port_tx.send(actual);
                                tracing::info!(%addr, "media server bound");
                                let _ = ack.send(Ok(()));
                            }
                            Err(error) => {
                                tracing::warn!(%addr, %error, "media server bind failed; no listener");
                                let _ = ack.send(Err(error));
                            }
                        }
                    }
                }
            }
            accepted = accept(&mut listener), if listener.is_some() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        tracing::warn!(%error, "media server accept failed");
                        continue;
                    }
                };
                let source = current.clone();
                let generation_rx = generation_tx.subscribe();
                let shutdown_rx = shutdown.subscribe();
                let live = live_rx.clone();
                let proxy = proxy.clone();
                tokio::spawn(async move {
                    handle_connection(peer, stream, source, live, generation_rx, shutdown_rx, proxy).await;
                });
            }
            _ = shutdown_rx.changed() => break,
        }
    }
    tracing::info!("media server stopped");
}

fn lock(
    slot: &Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
) -> MutexGuard<'_, Option<mpsc::Receiver<Vec<u8>>>> {
    match slot.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

async fn bind(ip: IpAddr, port: u16) -> io::Result<TcpListener> {
    let listener = TcpListener::bind((ip, port)).await?;
    let local = listener.local_addr()?;
    tracing::info!(ip = %local.ip(), port = local.port(), "media server listening");
    Ok(listener)
}

/// Pending forever when no listener is bound; only polled when one exists.
async fn accept(listener: &mut Option<TcpListener>) -> io::Result<(TcpStream, SocketAddr)> {
    match listener {
        Some(listener) => listener.accept().await,
        None => std::future::pending().await,
    }
}

/// A parsed HTTP/1.1 request head.
struct HttpRequest {
    method: String,
    target: String,
    /// Header names lowercased, values trimmed.
    headers: HashMap<String, String>,
}

/// Read the request line + headers. `UnexpectedEof` means the peer closed
/// before finishing; `InvalidData` means a malformed or oversized head.
async fn read_request<R>(reader: &mut R) -> io::Result<HttpRequest>
where
    R: AsyncBufRead + Unpin,
{
    let request_line = read_limited_line(reader, MAX_REQUEST_LINE)
        .await?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "closed before the request line",
            )
        })?;
    let request_line = request_line.trim_end_matches(['\r', '\n']);
    let mut parts = request_line.splitn(3, ' ');
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let version = parts.next().unwrap_or_default().to_string();
    if method.is_empty() || target.is_empty() || !version.starts_with("HTTP/1.") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed request line",
        ));
    }

    let mut headers = HashMap::new();
    loop {
        let line = read_limited_line(reader, MAX_HEADER_LINE)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "closed mid-headers"))?;
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "malformed header line",
            ));
        };
        if headers.len() >= MAX_HEADERS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "too many headers",
            ));
        }
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    Ok(HttpRequest {
        method,
        target,
        headers,
    })
}

/// Read one line (up to `max` bytes) from a buffered reader; `None` on
/// clean EOF with nothing buffered.
async fn read_limited_line<R>(reader: &mut R, max: usize) -> io::Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    let mut buf = Vec::with_capacity(256);
    loop {
        let read = reader.read_until(b'\n', &mut buf).await?;
        if buf.len() > max {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "line exceeds maximum length",
            ));
        }
        if read == 0 || buf.ends_with(b"\n") {
            break;
        }
    }
    if buf.is_empty() {
        return Ok(None);
    }
    Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
}

/// Serve one `/stream` connection. The body stream aborts when the source
/// generation changes (source switch) or shutdown fires.
async fn handle_connection(
    peer: SocketAddr,
    stream: TcpStream,
    source: Option<ActiveSource>,
    live_rx: Arc<Mutex<Option<mpsc::Receiver<Vec<u8>>>>>,
    mut generation_rx: watch::Receiver<u64>,
    mut shutdown_rx: watch::Receiver<bool>,
    proxy: Arc<UrlProxy>,
) {
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(write_half);

    let request = match timeout(REQUEST_HEAD_TIMEOUT, read_request(&mut reader)).await {
        Ok(Ok(request)) => request,
        Ok(Err(error)) if error.kind() == io::ErrorKind::UnexpectedEof => {
            tracing::debug!(%peer, "connection closed before the request head completed");
            return;
        }
        Ok(Err(_)) => {
            let _ = write_response_head(&mut writer, 400, "Bad Request", &[]).await;
            let _ = writer.flush().await;
            return;
        }
        Err(_) => {
            tracing::warn!(%peer, "slow client never finished the request head");
            return;
        }
    };

    if request.method != "GET" && request.method != "HEAD" {
        // Only GET/HEAD are supported (`04-media-proxy.md` §2).
        let _ = write_response_head(
            &mut writer,
            405,
            "Method Not Allowed",
            &[("Allow", "GET, HEAD")],
        )
        .await;
        let _ = writer.flush().await;
        return;
    }
    let head_only = request.method == "HEAD";

    if request.target != "/stream" {
        // `/stream` is the only route (`04-media-proxy.md` §1.1).
        let _ = write_response_head(&mut writer, 404, "Not Found", &[]).await;
        let _ = writer.flush().await;
        return;
    }

    let range = request.headers.get("range").map(String::as_str);

    let result = match source {
        None => {
            let _ = write_response_head(&mut writer, 404, "Not Found", &[]).await;
            let _ = writer.flush().await;
            return;
        }
        Some(ActiveSource::File(path)) => {
            tokio::select! {
                result = local_file::serve(&mut writer, &path, range, head_only) => result,
                _ = generation_rx.changed() => {
                    tracing::info!(%peer, "file stream aborted by source switch");
                    Ok(())
                }
                _ = shutdown_rx.changed() => Ok(()),
            }
        }
        Some(ActiveSource::Url(url)) => {
            // `smb://` URLs stream from a network share (anonymous only);
            // everything else goes through the HTTP(S) proxy
            // (`04-media-proxy.md` §4.4).
            let future = async move {
                if smb_source::is_smb_url(&url) {
                    smb_source::serve(&mut writer, &Smb2Connector, &url, range, head_only).await
                } else {
                    proxy.serve(&mut writer, &url, range, head_only).await
                }
            };
            tokio::select! {
                result = future => result,
                _ = generation_rx.changed() => {
                    tracing::info!(%peer, "proxy stream aborted by source switch");
                    Ok(())
                }
                _ = shutdown_rx.changed() => Ok(()),
            }
        }
        Some(ActiveSource::Screen(monitor)) => {
            // Live screen output is single-consumer: the encoder byte
            // stream is moved into the first `/stream` connection and
            // returned to the shared slot when it ends.
            let receiver = lock(&live_rx).take();
            match receiver {
                Some(receiver) => {
                    tracing::info!(%peer, %monitor, "serving live screen stream");
                    match serve_live_screen(
                        &mut writer,
                        receiver,
                        &mut generation_rx,
                        &mut shutdown_rx,
                    )
                    .await
                    {
                        Ok((end, receiver)) => {
                            // Only a stream that ended because the encoder
                            // output closed can be returned to the shared
                            // slot; a switch/shutdown abort means the source
                            // changed and a stale receiver must not be
                            // recycled.
                            if end == ScreenEnd::ChannelClosed {
                                *lock(&live_rx) = Some(receiver);
                            }
                            Ok(())
                        }
                        Err(error) => Err(error),
                    }
                }
                None => {
                    // Another connection holds the stream, or it is not
                    // attached yet.
                    let _ = write_response_head(
                        &mut writer,
                        503,
                        "Service Unavailable",
                        &[("Content-Type", "text/plain"), ("Content-Length", "4")],
                    )
                    .await;
                    let _ = writer.write_all(b"busy").await;
                    let _ = writer.flush().await;
                    return;
                }
            }
        }
    };

    if let Err(error) = result {
        tracing::warn!(%peer, %error, "stream connection ended with an error");
    }
}

/// How a live screen stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScreenEnd {
    /// The encoder output channel closed: the stream finished naturally.
    ChannelClosed,
    /// Aborted by a source switch or shutdown.
    Aborted,
}

/// Serve the continuous screen-encoder output: `200 OK` + `video/mp4`, no
/// `Content-Length` (close-delimited), body until the channel ends or the
/// generation/shutdown aborts it (`04-media-proxy.md` §5).
async fn serve_live_screen<W>(
    writer: &mut W,
    mut receiver: mpsc::Receiver<Vec<u8>>,
    generation_rx: &mut watch::Receiver<u64>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> io::Result<(ScreenEnd, mpsc::Receiver<Vec<u8>>)>
where
    W: AsyncWrite + Unpin,
{
    write_response_head(writer, 200, "OK", &[("Content-Type", "video/mp4")]).await?;
    // The head must reach the wire before the stream loop: encoder chunks
    // are small and may never fill the writer's buffer.
    writer.flush().await?;

    // Batch small encoder chunks on byte/time thresholds instead of flushing
    // each one: at most LIVE_FLUSH_INTERVAL of added latency, and the head
    // already went out immediately.
    let mut flush = FlushTracker::new(LIVE_FLUSH_BYTES, LIVE_FLUSH_INTERVAL);
    loop {
        tokio::select! {
            _ = generation_rx.changed() => {
                tracing::info!("live screen stream aborted by source switch");
                return Ok((ScreenEnd::Aborted, receiver));
            }
            _ = shutdown_rx.changed() => return Ok((ScreenEnd::Aborted, receiver)),
            _ = tokio::time::sleep_until(flush.next_deadline().into()) => {
                    // Time threshold elapsed with no new chunk: push whatever
                    // is buffered so the receiver never waits past the
                    // interval.
                    if flush.has_pending() {
                        writer.flush().await?;
                    }
                    flush.reset();
                }
            chunk = receiver.recv() => match chunk {
                Some(chunk) => {
                    writer.write_all(&chunk).await?;
                    if flush.should_flush(chunk.len()) {
                        writer.flush().await?;
                    }
                }
                None => {
                    tracing::info!("screen encoder output ended");
                    // Flush the tail: the response is close-delimited, so
                    // buffered bytes would be lost when the writer drops.
                    writer.flush().await?;
                    return Ok((ScreenEnd::ChannelClosed, receiver));
                }
            },
        }
    }
}

/// Write an HTTP/1.1 status line + headers + `Connection: close`. Shared by
/// every serving module so response heads are byte-consistent.
pub(crate) async fn write_response_head<W>(
    writer: &mut W,
    status: u16,
    reason: &str,
    headers: &[(&str, &str)],
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut head = format!("HTTP/1.1 {status} {reason}\r\n");
    for (name, value) in headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("Connection: close\r\n\r\n");
    writer.write_all(head.as_bytes()).await
}

/// Canonical reason phrase for common status codes; empty is valid HTTP/1.1.
pub(crate) fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        410 => "Gone",
        416 => "Range Not Satisfiable",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}
