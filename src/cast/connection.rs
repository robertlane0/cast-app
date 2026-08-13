#![forbid(unsafe_code)]

//! Full Cast connection lifecycle (`03-cast-engine.md` §7): the
//! `Disconnected → Connecting → Connected → Launching → Ready → Streaming →
//! Teardown` state machine, heartbeat watchdog, reconnect backoff, inbound
//! JSON routing, and ordered teardown. All blocking TLS I/O runs on a
//! dedicated reader thread or `spawn_blocking` workers, never on the tokio
//! executor (Phase 3 lesson).

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, Sleep};

use crate::cast::framing::{FrameError, encode_frame, read_frame};
use crate::cast::namespaces::{
    CONNECTION_NS, HEARTBEAT_NS, MEDIA_NS, RECEIVER_ID, RECEIVER_NS, SOURCE_ID, StreamType,
    TRANSPORT_ID, connect, launch, load, media_destination_id, parse_media_status,
    parse_receiver_status, pause, ping, play, set_volume, stop, stop_app,
};
use crate::cast::proto::{decode_cast_message, encode_cast_message};
use crate::cast::request_id::{PendingMap, RequestId};
use crate::cast::tls::{self, CastTlsStream, TlsError};
use crate::state::CastDevice;
use crate::util::retry::Backoff;
use crate::util::shutdown::Shutdown;

/// How long a socket read may block before the reader re-polls. Also bounds
/// the reader's lock hold time so writes and teardown always make progress.
pub const READ_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Idle-read backoff: after a WouldBlock poll the reader sleeps this long
/// before re-locking the transport mutex, so a queued writer (which loses
/// every instant re-lock race — barging) can acquire it deterministically.
const IDLE_READ_BACKOFF: Duration = Duration::from_millis(5);

/// Write timeout applied to the socket so teardown cannot hang on a dead
/// peer that has stopped reading.
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Reader read buffer size; frames are accumulated across reads.
const READ_BUFFER_SIZE: usize = 16 * 1024;

// ---------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------

/// Connection lifecycle phases (`03-cast-engine.md` §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// No transport; waiting for a receiver selection.
    Disconnected,
    /// TCP+TLS establishment in flight.
    Connecting,
    /// TLS established, `CONNECT` sent, heartbeat running.
    Connected,
    /// `LAUNCH` sent, awaiting `RECEIVER_STATUS`.
    Launching,
    /// Default Media Receiver launched; media commands accepted.
    Ready,
    /// Media is loaded; transport commands accepted.
    Streaming,
    /// Graceful shutdown in progress (`STOP → STOP_APP → close_notify`).
    Teardown,
}

// ---------------------------------------------------------------------------
// Transport abstraction
// ---------------------------------------------------------------------------

/// A blocking byte-stream transport with teardown support. The real
/// implementation is the rustls [`CastTlsStream`]; tests substitute an
/// in-memory duplex.
pub trait Transport: Read + Write + Send + 'static {
    /// Send `close_notify`, flush, then interrupt any blocked reader.
    /// Best effort — a dead peer must not block teardown.
    /// (`03-cast-engine.md` §7: teardown SHALL close with `close_notify`.)
    fn close(&mut self) {}

    /// Interrupt any blocked reader without a graceful close. Best effort.
    fn shutdown(&self) {}
}

impl Transport for CastTlsStream {
    fn close(&mut self) {
        self.conn.send_close_notify();
        let _ = self.flush();
        let _ = self.sock.shutdown(std::net::Shutdown::Both);
    }

    fn shutdown(&self) {
        let _ = self.sock.shutdown(std::net::Shutdown::Both);
    }
}

/// The transport shared between the reader thread and `spawn_blocking`
/// writers. Lock hold times are bounded by [`READ_POLL_INTERVAL`].
pub type SharedTransport = Arc<Mutex<dyn Transport>>;

fn lock_transport(transport: &Mutex<dyn Transport>) -> MutexGuard<'_, dyn Transport> {
    match transport.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

// ---------------------------------------------------------------------------
// Connector abstraction
// ---------------------------------------------------------------------------

/// Establishes the TLS transport to a receiver address. Pluggable so tests
/// can substitute a mock transport.
///
/// All implementations in this crate (real and mock) produce `Send` futures,
/// so the trait stays usable with `tokio::spawn`; the lint is allowed
/// deliberately for this crate-internal trait.
#[allow(async_fn_in_trait)]
pub trait Connector: Send + Sync + 'static {
    /// Connect and return a shared transport. The reader thread and the
    /// `spawn_blocking` writers lock it for each I/O operation.
    async fn connect(&self, addr: SocketAddr) -> Result<SharedTransport, TlsError>;
}

/// Real connector: TCP + rustls handshake via [`tls::connect`]
/// (`03-cast-engine.md` §3), plus read/write timeouts that bound reader lock
/// holds and teardown writes.
pub struct TlsConnector;

impl Connector for TlsConnector {
    async fn connect(&self, addr: SocketAddr) -> Result<SharedTransport, TlsError> {
        let stream = tls::connect(addr).await?;
        stream
            .sock
            .set_read_timeout(Some(READ_POLL_INTERVAL))
            .map_err(TlsError::from)?;
        stream
            .sock
            .set_write_timeout(Some(WRITE_TIMEOUT))
            .map_err(TlsError::from)?;
        Ok(Arc::new(Mutex::new(stream)))
    }
}

// ---------------------------------------------------------------------------
// Commands and events
// ---------------------------------------------------------------------------

/// Commands sent to the connection task. [`run`] consumes these; the GUI or
/// runtime dispatches through [`CastConnection`].
#[derive(Debug, Clone)]
pub enum Command {
    /// Select (or re-select) a receiver, tearing down any current session.
    Select(CastDevice),
    /// `LAUNCH` the Default Media Receiver (`FR-009`).
    LaunchDefaultReceiver,
    /// `LOAD` a media URL (`FR-020`). Launches first if not yet ready.
    Load {
        content_id: String,
        content_type: String,
        stream_type: StreamType,
    },
    /// `PLAY` (`FR-018`).
    Play,
    /// `PAUSE` (`FR-018`).
    Pause,
    /// Media `STOP` (`FR-018`).
    Stop,
    /// `SET_VOLUME` (`FR-018`).
    SetVolume { level: f32, muted: bool },
    /// Full teardown and exit of the connection task.
    Shutdown,
}

/// Events emitted by the connection task to the backend/GUI.
#[derive(Debug)]
pub enum ConnectionEvent {
    /// TLS established, `CONNECT` sent; heartbeat running.
    Connected(CastDevice),
    /// The session was torn down (loss, re-select, or shutdown).
    Disconnected(CastDevice),
    /// A fatal error: initial connect failure or reconnect exhaustion.
    Error(ConnectionError),
    /// `LAUNCH` succeeded; `transportId`/`sessionId` extracted.
    Ready {
        transport_id: String,
        session_id: String,
    },
    /// `MEDIA_STATUS` playback state (`FR-018`).
    MediaStatus { playing: bool, buffering: bool },
    /// `RECEIVER_STATUS` volume (`FR-018`).
    Volume { level: f32, muted: bool },
}

/// Fatal connection errors surfaced to the GUI (`03-cast-engine.md` §7.1).
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("TLS connection to {addr} failed: {source}")]
    Tls { addr: SocketAddr, source: TlsError },
    #[error("connection to {name} ({addr}) lost after {attempts} reconnect attempts")]
    ReconnectExhausted {
        name: String,
        addr: SocketAddr,
        attempts: usize,
    },
    #[error("spawning the reader thread failed: {source}")]
    ReaderThread { source: io::Error },
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Timers and backoff for a connection session (`03-cast-engine.md` §6–7):
/// PING every 5 s, PONG watchdog 10 s, request timeout 5 s, backoff
/// 1/2/4/8/16 s with 5 attempts.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// `PING` interval (`03-cast-engine.md` §6.2). Default 5 s.
    pub heartbeat_interval: Duration,
    /// Maximum silence from the receiver before teardown + reconnect.
    /// Default 10 s.
    pub watchdog_timeout: Duration,
    /// Per-request response timeout (`03-cast-engine.md` §6.0). Default 5 s.
    pub request_timeout: Duration,
    /// Reconnect backoff policy (`03-cast-engine.md` §7.1). Default 5
    /// attempts, 1 s → 16 s.
    pub backoff: Backoff,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            watchdog_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(5),
            backoff: Backoff::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

/// A queued media command issued before the session was ready; dispatched
/// when `RECEIVER_STATUS` makes the session `Ready`.
#[derive(Debug, Clone)]
enum PendingCommand {
    Load {
        content_id: String,
        content_type: String,
        stream_type: StreamType,
    },
}

/// Per-connection mutable state. Exists while a transport is established.
struct Session {
    phase: Phase,
    device: CastDevice,
    transport: SharedTransport,
    request_id: RequestId,
    pending: PendingMap,
    transport_id: Option<String>,
    session_id: Option<String>,
    pending_command: Option<PendingCommand>,
}

impl Session {
    fn new(device: CastDevice, transport: SharedTransport, request_timeout: Duration) -> Self {
        Self {
            phase: Phase::Connected,
            device,
            transport,
            request_id: RequestId::new(),
            pending: PendingMap::new(request_timeout),
            transport_id: None,
            session_id: None,
            pending_command: None,
        }
    }

    /// Allocate a request ID and register it as pending (`FR-021`).
    fn next_request(&mut self) -> u32 {
        let id = self.request_id.allocate();
        self.pending.insert(id, Instant::now());
        id
    }

    fn take_pending_command(&mut self) -> Option<PendingCommand> {
        self.pending_command.take()
    }
}

// ---------------------------------------------------------------------------
// Frame accumulation
// ---------------------------------------------------------------------------

/// Accumulates transport bytes into complete CastV2 frames
/// (`03-cast-engine.md` §4). The socket read is interruptible (timeouts), so
/// partial frames must survive across reads; this buffer holds them.
#[derive(Debug, Default)]
struct FrameAccumulator {
    buf: Vec<u8>,
}

impl FrameAccumulator {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Append bytes and extract every complete frame in order. A trailing
    /// partial frame stays buffered; a length prefix over the maximum is a
    /// protocol error and ends the connection.
    fn push_bytes(&mut self, bytes: &[u8]) -> Result<Vec<Vec<u8>>, FrameError> {
        self.buf.extend_from_slice(bytes);
        let mut frames = Vec::new();
        let mut consumed = 0usize;
        loop {
            let mut cursor = io::Cursor::new(&self.buf[consumed..]);
            match read_frame(&mut cursor) {
                Ok(Some(payload)) => {
                    consumed += cursor.position() as usize;
                    frames.push(payload);
                }
                Ok(None) | Err(FrameError::Truncated { .. }) => break,
                Err(error) => return Err(error),
            }
        }
        self.buf.drain(..consumed);
        Ok(frames)
    }
}

// ---------------------------------------------------------------------------
// Reader thread
// ---------------------------------------------------------------------------

/// The reader thread: owns the blocking read side of the transport and feeds
/// decoded frames to the run task. Sends `None` once, on exit. Runs on a
/// dedicated `std::thread` — never on a tokio worker (`03-cast-engine.md`
/// §7; Phase 3 lesson).
fn spawn_reader(
    transport: SharedTransport,
    frame_tx: mpsc::UnboundedSender<Option<Vec<u8>>>,
    shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    std::thread::Builder::new()
        .name("cast-reader".into())
        .spawn(move || reader_loop(transport, frame_tx, shutdown_rx))
        .map(|_| ())
}

fn reader_loop(
    transport: SharedTransport,
    frame_tx: mpsc::UnboundedSender<Option<Vec<u8>>>,
    shutdown_rx: watch::Receiver<bool>,
) {
    let mut accumulator = FrameAccumulator::new();
    let mut buffer = [0u8; READ_BUFFER_SIZE];

    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        let read_result = {
            let mut guard = lock_transport(&transport);
            guard.read(&mut buffer)
        };
        match read_result {
            // Clean EOF (close_notify) or connection reset: the session is
            // over; the run task decides whether to reconnect.
            Ok(0) => break,
            Ok(n) => match accumulator.push_bytes(&buffer[..n]) {
                Ok(frames) => {
                    for frame in frames {
                        if frame_tx.send(Some(frame)).is_err() {
                            return; // run task gone
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "protocol error while reading frames; closing connection");
                    break;
                }
            },
            // Read timeout / would-block: poll shutdown state and retry.
            //
            // Sleep before re-locking: the reader re-acquires the transport
            // mutex within microseconds of an idle poll, which starves
            // concurrent writers (mutex barging) — a blocked writer loses
            // the race to the instantly re-locking reader every cycle. A
            // short sleep opens a scheduling window the writer always wins.
            // Writers are commands/PINGs (human-scale or 5s cadence), so the
            // added latency is irrelevant; reads stay bounded by the socket
            // timeout.
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut =>
            {
                std::thread::sleep(IDLE_READ_BACKOFF);
            }
            Err(error) => {
                tracing::debug!(%error, "transport read ended");
                break;
            }
        }
    }

    let _ = frame_tx.send(None);
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

/// Write one framed payload on a `spawn_blocking` worker (blocking socket).
async fn send_payload(transport: &SharedTransport, payload: Vec<u8>) -> io::Result<()> {
    let transport = transport.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut guard = lock_transport(&transport);
        guard.write_all(&encode_frame(&payload))
    })
    .await;
    match result {
        Ok(inner) => inner,
        Err(_) => Err(io::Error::other("frame writer worker panicked")),
    }
}

// ---------------------------------------------------------------------------
// Inbound routing
// ---------------------------------------------------------------------------

/// What the run task should do with a routed inbound message.
enum InboundAction {
    /// Heartbeat `PONG`: reset the watchdog.
    Pong,
    /// Emit these events to the backend.
    Events(Vec<ConnectionEvent>),
    /// Nothing to do.
    Ignore,
}

impl Session {
    /// Decode, parse and classify one inbound frame payload
    /// (`03-cast-engine.md` §6): `PONG`, `RECEIVER_STATUS`, `MEDIA_STATUS`.
    /// Unknown or malformed messages are logged and ignored, never fatal.
    fn route_inbound(&mut self, payload: Vec<u8>) -> InboundAction {
        let message = match decode_cast_message(&payload) {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(%error, "undecodable inbound frame");
                return InboundAction::Ignore;
            }
        };
        let text = message.payload_utf8;
        let value: Value = match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(error) => {
                tracing::debug!(%error, "non-JSON payload in cast message");
                return InboundAction::Ignore;
            }
        };
        let Some(msg_type) = value.get("type").and_then(Value::as_str) else {
            tracing::debug!("inbound cast message without a type");
            return InboundAction::Ignore;
        };

        match msg_type {
            "PONG" => InboundAction::Pong,
            "RECEIVER_STATUS" => {
                self.correlate(&value);
                match parse_receiver_status(&text) {
                    Some(status) => InboundAction::Events(self.apply_receiver_status(status)),
                    None => InboundAction::Ignore,
                }
            }
            "MEDIA_STATUS" => {
                self.correlate(&value);
                match parse_media_status(&text) {
                    Some(info) => InboundAction::Events(self.apply_media_status(info)),
                    None => InboundAction::Ignore,
                }
            }
            _ => {
                tracing::debug!(msg_type, namespace = %message.namespace, "unsolicited or unknown inbound message");
                InboundAction::Ignore
            }
        }
    }

    /// Resolve the pending request this response correlates to (FR-021).
    fn correlate(&mut self, value: &Value) {
        if let Some(request_id) = value
            .get("requestId")
            .and_then(Value::as_u64)
            .map(|id| id as u32)
        {
            tracing::debug!(
                request_id,
                hit = self.pending.resolve(request_id),
                "correlated inbound response"
            );
        }
    }

    fn apply_receiver_status(
        &mut self,
        status: crate::cast::namespaces::ReceiverStatus,
    ) -> Vec<ConnectionEvent> {
        let mut events = Vec::new();
        if let Some(transport_id) = status.transport_id {
            self.transport_id = Some(transport_id);
        }
        if let Some(session_id) = status.session_id {
            self.session_id = Some(session_id);
        }

        // (FR-009) `transportId` from RECEIVER_STATUS is used as the media
        // destination ID; the session becomes Ready for media commands.
        if self.phase == Phase::Launching {
            if let (Some(transport_id), Some(session_id)) = (&self.transport_id, &self.session_id) {
                self.phase = Phase::Ready;
                events.push(ConnectionEvent::Ready {
                    transport_id: transport_id.clone(),
                    session_id: session_id.clone(),
                });
            }
        }
        if let Some(volume) = status.volume {
            events.push(ConnectionEvent::Volume {
                level: volume.level,
                muted: volume.muted,
            });
        }
        events
    }

    fn apply_media_status(
        &mut self,
        info: crate::cast::namespaces::MediaStatusInfo,
    ) -> Vec<ConnectionEvent> {
        use crate::cast::namespaces::PlayerState;
        let playing = matches!(
            info.player_state,
            PlayerState::Playing | PlayerState::Buffering
        );
        let buffering = info.player_state == PlayerState::Buffering;
        vec![ConnectionEvent::MediaStatus { playing, buffering }]
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Dispatch one session command. Returns `Err` when the transport write
/// fails — the caller treats that as a lost connection.
async fn handle_command(session: &mut Session, command: Command) -> Result<(), io::Error> {
    match command {
        Command::LaunchDefaultReceiver => {
            if session.phase != Phase::Connected {
                tracing::debug!(phase = ?session.phase, "LAUNCH ignored outside Connected");
                return Ok(());
            }
            let id = session.next_request();
            let payload = encode_cast_message(SOURCE_ID, RECEIVER_ID, RECEIVER_NS, &launch(id));
            session.phase = Phase::Launching;
            send_payload(&session.transport, payload).await
        }
        Command::Load {
            content_id,
            content_type,
            stream_type,
        } => {
            match session.phase {
                Phase::Ready | Phase::Streaming => {
                    let id = session.next_request();
                    let destination = session.media_destination();
                    let payload = encode_cast_message(
                        SOURCE_ID,
                        &destination,
                        MEDIA_NS,
                        &load(id, &content_id, &content_type, stream_type),
                    );
                    session.phase = Phase::Streaming;
                    send_payload(&session.transport, payload).await
                }
                Phase::Connected => {
                    // Not launched yet: launch first and queue the LOAD so
                    // "select then play" works without extra round-trips.
                    let id = session.next_request();
                    let payload =
                        encode_cast_message(SOURCE_ID, RECEIVER_ID, RECEIVER_NS, &launch(id));
                    session.phase = Phase::Launching;
                    session.pending_command = Some(PendingCommand::Load {
                        content_id,
                        content_type,
                        stream_type,
                    });
                    send_payload(&session.transport, payload).await
                }
                phase => {
                    tracing::debug!(?phase, "LOAD ignored outside Ready/Streaming/Connected");
                    Ok(())
                }
            }
        }
        Command::Play | Command::Pause | Command::Stop => {
            if !matches!(session.phase, Phase::Ready | Phase::Streaming) {
                tracing::debug!(phase = ?session.phase, "transport command ignored outside Ready/Streaming");
                return Ok(());
            }
            let id = session.next_request();
            let destination = session.media_destination();
            let payload = encode_cast_message(
                SOURCE_ID,
                &destination,
                MEDIA_NS,
                &match command {
                    Command::Play => play(id),
                    Command::Pause => pause(id),
                    Command::Stop => stop(id),
                    _ => unreachable!(),
                },
            );
            if matches!(command, Command::Stop) {
                session.phase = Phase::Ready;
            }
            send_payload(&session.transport, payload).await
        }
        Command::SetVolume { level, muted } => {
            let id = session.next_request();
            let payload = encode_cast_message(
                SOURCE_ID,
                RECEIVER_ID,
                RECEIVER_NS,
                &set_volume(id, level, muted),
            );
            send_payload(&session.transport, payload).await
        }
        Command::Select(_) | Command::Shutdown => {
            unreachable!("Select/Shutdown are handled by the run loop")
        }
    }
}

impl Session {
    /// The media namespace destination: `transport-<sessionId>`
    /// (`03-cast-engine.md` §6.0). Falls back to the transport ID while the
    /// session is not yet launched.
    fn media_destination(&self) -> String {
        match self.transport_id.as_deref() {
            Some(transport_id) => media_destination_id(transport_id),
            None => TRANSPORT_ID.to_string(),
        }
    }
}

/// Dispatch a command queued while the session was `Connected`.
async fn dispatch_pending(session: &mut Session, pending: PendingCommand) -> Result<(), io::Error> {
    match pending {
        PendingCommand::Load {
            content_id,
            content_type,
            stream_type,
        } => {
            let id = session.next_request();
            let destination = session.media_destination();
            let payload = encode_cast_message(
                SOURCE_ID,
                &destination,
                MEDIA_NS,
                &load(id, &content_id, &content_type, stream_type),
            );
            session.phase = Phase::Streaming;
            send_payload(&session.transport, payload).await
        }
    }
}

// ---------------------------------------------------------------------------
// Teardown and reconnect
// ---------------------------------------------------------------------------

/// Graceful session teardown (`03-cast-engine.md` §7): media `STOP` →
/// receiver `STOP_APP` → `close_notify` → close socket. Best effort — a dead
/// peer logs and proceeds.
async fn teardown_session(state: &mut RunState, events: &mpsc::UnboundedSender<ConnectionEvent>) {
    let Some(mut session) = state.session.take() else {
        return;
    };

    if let Some(session_id) = session.session_id.clone() {
        if session.phase == Phase::Streaming {
            let id = session.next_request();
            let destination = session.media_destination();
            let payload = encode_cast_message(SOURCE_ID, &destination, MEDIA_NS, &stop(id));
            if let Err(error) = send_payload(&session.transport, payload).await {
                tracing::debug!(%error, "best-effort media STOP failed during teardown");
            }
        }
        let id = session.next_request();
        let payload = encode_cast_message(
            SOURCE_ID,
            RECEIVER_ID,
            RECEIVER_NS,
            &stop_app(id, &session_id),
        );
        if let Err(error) = send_payload(&session.transport, payload).await {
            tracing::debug!(%error, "best-effort STOP_APP failed during teardown");
        }
    }

    let transport = session.transport.clone();
    let closed = tokio::task::spawn_blocking(move || {
        let mut guard = lock_transport(&transport);
        guard.close();
    })
    .await;
    if closed.is_err() {
        tracing::warn!("teardown worker panicked");
    }

    // The reader thread exits once the socket is closed; dropping the
    // channel means its `None` signal is not needed.
    state.inbound = None;
    state.heartbeat = None;
    state.watchdog = None;

    let _ = events.send(ConnectionEvent::Disconnected(session.device));
}

// ---------------------------------------------------------------------------
// Run-loop state
// ---------------------------------------------------------------------------

/// All mutable state owned by the [`run`] loop, bundled so helpers take a
/// single reference instead of a long argument list. Fields are borrowed
/// disjointly by the `tokio::select!` branches.
struct RunState {
    session: Option<Session>,
    desired: Option<CastDevice>,
    inbound: Option<mpsc::UnboundedReceiver<Option<Vec<u8>>>>,
    heartbeat: Option<std::pin::Pin<Box<Sleep>>>,
    watchdog: Option<std::pin::Pin<Box<Sleep>>>,
    reconnect: Option<Reconnect>,
}

impl RunState {
    fn new() -> Self {
        Self {
            session: None,
            desired: None,
            inbound: None,
            heartbeat: None,
            watchdog: None,
            reconnect: None,
        }
    }
}

/// A reconnect attempt scheduled after a backoff delay
/// (`03-cast-engine.md` §7.1).
struct Reconnect {
    delay: std::pin::Pin<Box<Sleep>>,
    device: CastDevice,
    backoff: Backoff,
    /// Number of failed connect attempts so far.
    attempts: usize,
}

/// Handle a lost connection: tear down, then schedule reconnect attempts
/// with exponential backoff; surface [`ConnectionError::ReconnectExhausted`]
/// once the policy is exhausted and wait for the user to re-select.
async fn connection_lost(
    state: &mut RunState,
    events: &mpsc::UnboundedSender<ConnectionEvent>,
    config: &ConnectionConfig,
) {
    teardown_session(state, events).await;
    let Some(device) = state.desired.clone() else {
        return;
    };

    let mut backoff = config.backoff.clone();
    match backoff.next() {
        Some(delay) => {
            state.reconnect = Some(Reconnect {
                delay: Box::pin(tokio::time::sleep(delay)),
                device,
                backoff,
                attempts: 0,
            });
        }
        None => {
            let _ = events.send(ConnectionEvent::Error(
                ConnectionError::ReconnectExhausted {
                    name: device.name,
                    addr: device.addr,
                    attempts: 0,
                },
            ));
            state.desired = None;
        }
    }
}

// ---------------------------------------------------------------------------
// Connect flow
// ---------------------------------------------------------------------------

/// Establish a session to `device`: TLS connect → `CONNECT` → heartbeat →
/// emit `Connected`. `LAUNCH` is issued by
/// [`Command::LaunchDefaultReceiver`] or auto-issued by `LOAD`
/// (`03-cast-engine.md` §7).
async fn establish<C: Connector>(
    state: &mut RunState,
    device: CastDevice,
    connector: &C,
    events: &mpsc::UnboundedSender<ConnectionEvent>,
    config: &ConnectionConfig,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), ConnectionError> {
    let transport =
        connector
            .connect(device.addr)
            .await
            .map_err(|source| ConnectionError::Tls {
                addr: device.addr,
                source,
            })?;

    // CONNECT before the reader spawns so a failed write cannot leak a
    // blocked reader thread.
    let payload = encode_cast_message(SOURCE_ID, TRANSPORT_ID, CONNECTION_NS, &connect());
    if let Err(error) = send_payload(&transport, payload).await {
        return Err(ConnectionError::Tls {
            addr: device.addr,
            source: TlsError::Io(error),
        });
    }

    let (frame_tx, frame_rx) = mpsc::unbounded_channel();
    spawn_reader(transport.clone(), frame_tx, shutdown_rx)
        .map_err(|source| ConnectionError::ReaderThread { source })?;
    state.inbound = Some(frame_rx);

    let mut session_state = Session::new(device.clone(), transport, config.request_timeout);
    session_state.phase = Phase::Connected;
    state.session = Some(session_state);

    state.heartbeat = Some(Box::pin(tokio::time::sleep(config.heartbeat_interval)));
    state.watchdog = Some(Box::pin(tokio::time::sleep(config.watchdog_timeout)));

    let _ = events.send(ConnectionEvent::Connected(device));
    Ok(())
}

// ---------------------------------------------------------------------------
// Run loop
// ---------------------------------------------------------------------------

async fn inbound_recv(
    inbound: &mut Option<mpsc::UnboundedReceiver<Option<Vec<u8>>>>,
) -> Option<Option<Vec<u8>>> {
    match inbound {
        Some(receiver) => receiver.recv().await,
        None => None,
    }
}

async fn heartbeat_opt(heartbeat: &mut Option<std::pin::Pin<Box<Sleep>>>) {
    if let Some(heartbeat) = heartbeat {
        heartbeat.as_mut().await;
    }
}

async fn watchdog_opt(watchdog: &mut Option<std::pin::Pin<Box<Sleep>>>) {
    if let Some(watchdog) = watchdog {
        watchdog.as_mut().await;
    }
}

async fn reconnect_delay(reconnect: &mut Option<Reconnect>) {
    if let Some(reconnect) = reconnect {
        reconnect.delay.as_mut().await;
    }
}

/// The connection state machine (`03-cast-engine.md` §7). Runs until
/// [`Command::Shutdown`], the [`Shutdown`] token, or all command senders
/// drop.
pub async fn run<C: Connector>(
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::UnboundedSender<ConnectionEvent>,
    shutdown: Shutdown,
    connector: C,
    config: ConnectionConfig,
) {
    let mut state = RunState::new();
    let mut shutdown_rx = shutdown.subscribe();

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    None => break, // all command senders dropped
                    Some(Command::Shutdown) => {
                        teardown_session(&mut state, &events).await;
                        break;
                    }
                    Some(Command::Select(device)) => {
                        teardown_session(&mut state, &events).await;
                        state.reconnect = None;
                        state.desired = Some(device.clone());
                        if let Err(error) = establish(
                            &mut state,
                            device,
                            &connector,
                            &events,
                            &config,
                            shutdown.subscribe(),
                        ).await {
                            tracing::error!(%error, "initial connection failed");
                            let _ = events.send(ConnectionEvent::Error(error));
                            state.desired = None;
                        }
                    }
                    Some(command) => {
                        let handled = match state.session.as_mut() {
                            Some(session) => handle_command(session, command).await,
                            None => {
                                tracing::debug!("ignoring command while disconnected");
                                Ok(())
                            }
                        };
                        if let Err(error) = handled {
                            tracing::warn!(%error, "command write failed; treating connection as lost");
                            connection_lost(&mut state, &events, &config).await;
                        }
                    }
                }
            }
            frame = inbound_recv(&mut state.inbound), if state.inbound.is_some() => {
                match frame {
                    Some(Some(payload)) => {
                        let mut dispatch = false;
                        if let Some(session) = state.session.as_mut() {
                            match session.route_inbound(payload) {
                                InboundAction::Pong => {
                                    if let Some(watchdog) = state.watchdog.as_mut() {
                                        watchdog.as_mut().reset(Instant::now() + config.watchdog_timeout);
                                    }
                                }
                                InboundAction::Events(events_out) => {
                                    for event in events_out {
                                        let _ = events.send(event);
                                    }
                                }
                                InboundAction::Ignore => {}
                            }
                            dispatch = session.phase == Phase::Ready;
                        }
                        if dispatch {
                            let dispatched = match state.session.as_mut() {
                                Some(session) => match session.take_pending_command() {
                                    Some(pending) => dispatch_pending(session, pending).await,
                                    None => Ok(()),
                                },
                                None => Ok(()),
                            };
                            if let Err(error) = dispatched {
                                tracing::warn!(%error, "queued command write failed; treating connection as lost");
                                connection_lost(&mut state, &events, &config).await;
                            }
                        }
                    }
                    Some(None) | None => {
                        // The reader thread exited: connection lost (or our
                        // own teardown already took the session).
                        if state.session.is_some() {
                            tracing::warn!("reader exited; treating connection as lost");
                            connection_lost(&mut state, &events, &config).await;
                        } else {
                            state.inbound = None;
                        }
                    }
                }
            }
            _ = heartbeat_opt(&mut state.heartbeat), if state.heartbeat.is_some() => {
                    let transport = state.session.as_ref().map(|session| session.transport.clone());
                match transport {
                    Some(transport) => {
                        let payload = encode_cast_message(SOURCE_ID, TRANSPORT_ID, HEARTBEAT_NS, &ping());
                        // (FR-008) PING every heartbeat interval.
                        if let Err(error) = send_payload(&transport, payload).await {
                                        tracing::warn!(%error, "PING write failed; treating connection as lost");
                            connection_lost(&mut state, &events, &config).await;
                        } else {
                            state.heartbeat =
                                Some(Box::pin(tokio::time::sleep(config.heartbeat_interval)));
                        }
                    }
                    None => state.heartbeat = None,
                }
            }
            _ = watchdog_opt(&mut state.watchdog), if state.watchdog.is_some() => {
                if state.session.is_some() {
                    // (FR-008) No PONG within the watchdog window: teardown
                    // and reconnect.
                    tracing::warn!("heartbeat watchdog fired; no PONG received");
                    connection_lost(&mut state, &events, &config).await;
                } else {
                    state.watchdog = None;
                }
            }
            _ = reconnect_delay(&mut state.reconnect), if state.reconnect.is_some() => {
                if let Some(mut reconnect_state) = state.reconnect.take() {
                    let device = reconnect_state.device;
                    match establish(
                        &mut state,
                        device.clone(),
                        &connector,
                        &events,
                        &config,
                        shutdown.subscribe(),
                    ).await {
                        Ok(()) => {}
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                attempt = reconnect_state.attempts + 1,
                                "reconnect attempt failed",
                            );
                            match reconnect_state.backoff.next() {
                                Some(delay) => {
                                    state.reconnect = Some(Reconnect {
                                        delay: Box::pin(tokio::time::sleep(delay)),
                                        device,
                                        backoff: reconnect_state.backoff,
                                        attempts: reconnect_state.attempts + 1,
                                    });
                                }
                                None => {
                                    let _ = events.send(ConnectionEvent::Error(
                                        ConnectionError::ReconnectExhausted {
                                            name: device.name,
                                            addr: device.addr,
                                            attempts: reconnect_state.attempts + 1,
                                        },
                                    ));
                                    state.desired = None;
                                }
                            }
                        }
                    }
                }
            }
            _ = shutdown_rx.changed(), if !*shutdown_rx.borrow() => {
                tracing::info!("shutdown requested; tearing down connection");
                teardown_session(&mut state, &events).await;
                break;
            }
        }

        // Reap requests that exceeded their response timeout (§6.0).
        if let Some(session) = state.session.as_mut() {
            for request_id in session.pending.expire(Instant::now()) {
                tracing::warn!(request_id, "request timed out waiting for a response");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public facade
// ---------------------------------------------------------------------------

/// Handle for driving one connection task. All methods are non-blocking:
/// they enqueue a [`Command`] and return immediately.
#[derive(Debug, Clone)]
pub struct CastConnection {
    commands: mpsc::UnboundedSender<Command>,
}

impl CastConnection {
    /// Spawn the connection task on the current tokio runtime with default
    /// timers ([`ConnectionConfig::default`]).
    pub fn start(events: mpsc::UnboundedSender<ConnectionEvent>, shutdown: Shutdown) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(
            receiver,
            events,
            shutdown,
            TlsConnector,
            ConnectionConfig::default(),
        ));
        Self { commands }
    }

    /// Select (or re-select) a receiver; tears down any current session.
    pub fn select(&self, device: CastDevice) {
        let _ = self.commands.send(Command::Select(device));
    }

    /// `LAUNCH` the Default Media Receiver (FR-009).
    pub fn launch_default_receiver(&self) {
        let _ = self.commands.send(Command::LaunchDefaultReceiver);
    }

    /// `LOAD` a media URL (FR-020). If the session has not been launched
    /// yet, it is launched first and the `LOAD` is queued.
    ///
    /// `content_type` is required by the `LOAD` message
    /// (`03-cast-engine.md` §6.4) and comes from the media MIME map
    /// (Phase 7).
    pub fn load(&self, content_id: &str, content_type: &str, stream_type: StreamType) {
        let _ = self.commands.send(Command::Load {
            content_id: content_id.to_string(),
            content_type: content_type.to_string(),
            stream_type,
        });
    }

    /// `PLAY` (FR-018).
    pub fn play(&self) {
        let _ = self.commands.send(Command::Play);
    }

    /// `PAUSE` (FR-018).
    pub fn pause(&self) {
        let _ = self.commands.send(Command::Pause);
    }

    /// Media `STOP` (FR-018).
    pub fn stop(&self) {
        let _ = self.commands.send(Command::Stop);
    }

    /// `SET_VOLUME` (FR-018).
    pub fn set_volume(&self, level: f32, muted: bool) {
        let _ = self.commands.send(Command::SetVolume { level, muted });
    }

    /// Full teardown (`STOP → STOP_APP → close_notify`) and stop the
    /// connection task.
    pub fn shutdown(&self) {
        let _ = self.commands.send(Command::Shutdown);
    }
}

/// Test double plumbing shared by the in-module gate tests and
/// `tests/connection_tests.rs`. Test-only: integration tests link the
/// library without `cfg(test)`, so this module is always compiled (it is
/// dead weight in release builds, never referenced by production code).
#[doc(hidden)]
pub mod test_support {
    use super::{Connector, SharedTransport, TlsError, Transport};
    use std::io::{self, Read, Write};
    use std::net::SocketAddr;
    use std::sync::{Arc, Condvar, Mutex, MutexGuard};
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct MockState {
        incoming: Vec<u8>,
        outgoing: Vec<u8>,
        closed: bool,
    }

    struct MockCore {
        state: Mutex<MockState>,
        incoming: Condvar,
        outgoing: Condvar,
    }

    /// Byte pipe shared between a `MockTransport` (reader/writer side) and
    /// the test (control side).
    #[derive(Clone)]
    pub struct MockPipe {
        core: Arc<MockCore>,
    }

    impl MockPipe {
        pub fn new() -> Self {
            Self {
                core: Arc::new(MockCore {
                    state: Mutex::new(MockState::default()),
                    incoming: Condvar::new(),
                    outgoing: Condvar::new(),
                }),
            }
        }

        fn lock(&self) -> MutexGuard<'_, MockState> {
            match self.core.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }

        /// Feed bytes to the transport's reader.
        pub fn push_incoming(&self, bytes: &[u8]) {
            let mut state = self.lock();
            state.incoming.extend_from_slice(bytes);
            self.core.incoming.notify_all();
        }

        /// Block up to `timeout` for the transport to write something;
        /// returns and clears whatever is buffered (possibly empty on
        /// timeout/close).
        pub fn wait_outgoing(&self, timeout: Duration) -> Vec<u8> {
            let deadline = Instant::now() + timeout;
            let mut state = self.lock();
            loop {
                if !state.outgoing.is_empty() || state.closed {
                    return std::mem::take(&mut state.outgoing);
                }
                let now = Instant::now();
                if now >= deadline {
                    return std::mem::take(&mut state.outgoing);
                }
                let (new_state, timed_out) =
                    match self.core.outgoing.wait_timeout(state, deadline - now) {
                        Ok(result) => result,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                state = new_state;
                if timed_out.timed_out() {
                    return std::mem::take(&mut state.outgoing);
                }
            }
        }

        /// Non-blocking take of everything the transport has written so far.
        pub fn take_outgoing(&self) -> Vec<u8> {
            std::mem::take(&mut self.lock().outgoing)
        }

        /// Simulate socket shutdown; unblocks the transport's reader with
        /// clean EOF.
        pub fn close(&self) {
            let mut state = self.lock();
            state.closed = true;
            self.core.incoming.notify_all();
            self.core.outgoing.notify_all();
        }

        pub fn is_closed(&self) -> bool {
            self.lock().closed
        }
    }

    impl Default for MockPipe {
        fn default() -> Self {
            Self::new()
        }
    }

    /// `Read`/`Write`/`Transport` impl over a `MockPipe`. Reads block until
    /// data arrives or the pipe closes (mirrors blocking socket semantics).
    pub struct MockTransport {
        pipe: MockPipe,
    }

    impl Read for MockTransport {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            // Mirror the real socket's blocking-socket-with-timeout
            // semantics: wake on data, clean EOF on close, otherwise a
            // WouldBlock after a short poll interval (so the transport
            // mutex is never held indefinitely, exactly like the real
            // READ_POLL_INTERVAL).
            const POLL_INTERVAL: Duration = Duration::from_millis(100);
            let mut state = self.pipe.lock();
            loop {
                if !state.incoming.is_empty() {
                    let n = state.incoming.len().min(buf.len());
                    buf[..n].copy_from_slice(&state.incoming[..n]);
                    state.incoming.drain(..n);
                    return Ok(n);
                }
                if state.closed {
                    return Ok(0);
                }
                let (new_state, timed_out) =
                    match self.pipe.core.incoming.wait_timeout(state, POLL_INTERVAL) {
                        Ok(result) => result,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                state = new_state;
                if timed_out.timed_out() {
                    return Err(io::Error::new(io::ErrorKind::WouldBlock, "no data yet"));
                }
            }
        }
    }

    impl Write for MockTransport {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut state = self.pipe.lock();
            state.outgoing.extend_from_slice(buf);
            self.pipe.core.outgoing.notify_all();
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Transport for MockTransport {
        fn close(&mut self) {
            self.pipe.close();
        }

        fn shutdown(&self) {
            self.pipe.close();
        }
    }

    #[derive(Default)]
    struct MockConnectorState {
        fail_connections: usize,
        pipes: Vec<MockPipe>,
    }

    /// Succeeds by handing out a fresh `MockPipe`; can be armed to fail the
    /// next `n` connect attempts (for reconnect/backoff tests).
    #[derive(Clone, Default)]
    pub struct MockConnector {
        state: Arc<Mutex<MockConnectorState>>,
    }

    impl MockConnector {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn fail_next(&self, n: usize) {
            self.state
                .lock()
                .expect("no poisoned mock state")
                .fail_connections = n;
        }

        pub fn pipes(&self) -> Vec<MockPipe> {
            self.state
                .lock()
                .expect("no poisoned mock state")
                .pipes
                .clone()
        }

        pub fn last_pipe(&self) -> Option<MockPipe> {
            self.state
                .lock()
                .expect("no poisoned mock state")
                .pipes
                .last()
                .cloned()
        }
    }

    impl Connector for MockConnector {
        async fn connect(&self, addr: SocketAddr) -> Result<SharedTransport, TlsError> {
            let mut state = self.state.lock().expect("no poisoned mock state");
            if state.fail_connections > 0 {
                state.fail_connections -= 1;
                return Err(TlsError::Connect {
                    addr,
                    source: io::Error::new(io::ErrorKind::ConnectionRefused, "mock: refused"),
                });
            }
            let pipe = MockPipe::new();
            state.pipes.push(pipe.clone());
            Ok(Arc::new(Mutex::new(MockTransport { pipe })))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{MockConnector, MockPipe};
    use super::*;
    use crate::cast::framing::{FrameError, MAX_FRAME_SIZE, encode_frame, read_frame};
    use crate::cast::namespaces::{
        CONNECTION_NS, HEARTBEAT_NS, MEDIA_NS, RECEIVER_ID, RECEIVER_NS, SOURCE_ID, TRANSPORT_ID,
        media_destination_id,
    };
    use crate::cast::proto::{CastMessage, decode_cast_message};
    use crate::state::CastDevice;
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;

    fn test_device() -> CastDevice {
        CastDevice {
            id: "living-room".to_string(),
            name: "Living Room".to_string(),
            addr: "10.0.0.5:8009".parse().expect("valid test address"),
        }
    }

    fn test_config() -> ConnectionConfig {
        ConnectionConfig {
            heartbeat_interval: Duration::from_millis(50),
            watchdog_timeout: Duration::from_millis(200),
            request_timeout: Duration::from_secs(5),
            backoff: Backoff::with_params(Duration::from_millis(20), Duration::from_millis(20), 3),
        }
    }

    /// Config for tests whose wire traffic must stay noise-free: long
    /// heartbeat so PINGs don't pollute frame-sequence assertions.
    fn quiet_config() -> ConnectionConfig {
        ConnectionConfig {
            heartbeat_interval: Duration::from_secs(10),
            watchdog_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(5),
            backoff: Backoff::with_params(Duration::from_millis(20), Duration::from_millis(20), 3),
        }
    }

    fn cast_frame(source: &str, dest: &str, ns: &str, payload: &str) -> Vec<u8> {
        encode_frame(&encode_cast_message(source, dest, ns, payload))
    }

    fn receiver_status_frame(transport_id: &str, session_id: &str) -> Vec<u8> {
        cast_frame(
            RECEIVER_ID,
            SOURCE_ID,
            RECEIVER_NS,
            &format!(
                r#"{{"type":"RECEIVER_STATUS","requestId":0,"status":{{"applications":[{{"appId":"CC1AD845","sessionId":"{session_id}","transportId":"{transport_id}","statusText":"Ready"}}]}}}}"#,
            ),
        )
    }

    /// Drain everything the transport has written within `timeout`, parse it
    /// into frames, and return the message payloads in order.
    async fn drain_messages(pipe: &MockPipe, timeout: Duration) -> Vec<CastMessage> {
        let mut accumulated = Vec::new();
        let mut deadline = Instant::now() + timeout;
        loop {
            let chunk = pipe.wait_outgoing(deadline.saturating_duration_since(Instant::now()));
            if chunk.is_empty() {
                break;
            }
            accumulated.extend_from_slice(&chunk);
            deadline = Instant::now() + timeout;
        }
        let mut messages = Vec::new();
        let mut cursor = std::io::Cursor::new(&accumulated);
        while let Ok(Some(payload)) = read_frame(&mut cursor) {
            messages.push(decode_cast_message(&payload).expect("valid frame"));
        }
        messages
    }

    async fn expect_event(rx: &mut mpsc::UnboundedReceiver<ConnectionEvent>) -> ConnectionEvent {
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("event within timeout")
            .expect("sender still alive")
    }

    async fn expect_connected(rx: &mut mpsc::UnboundedReceiver<ConnectionEvent>) {
        match expect_event(rx).await {
            ConnectionEvent::Connected(device) => assert_eq!(device.id, "living-room"),
            other => panic!("expected Connected, got {other:?}"),
        }
    }

    #[test]
    fn frame_accumulator_handles_partial_and_multiple_frames() {
        let mut accumulator = FrameAccumulator::new();
        let frame_one = encode_frame(b"hello");
        let frame_two = encode_frame(b"world");

        let mut partial = Vec::new();
        partial.extend_from_slice(&frame_one[..3]);
        assert!(
            accumulator
                .push_bytes(&partial)
                .expect("no error")
                .is_empty()
        );

        let mut rest = Vec::new();
        rest.extend_from_slice(&frame_one[3..]);
        rest.extend_from_slice(&frame_two);
        let frames = accumulator.push_bytes(&rest).expect("no error");
        assert_eq!(frames, vec![b"hello".to_vec(), b"world".to_vec()]);

        assert!(accumulator.push_bytes(&[]).expect("no error").is_empty());
    }

    #[test]
    fn frame_accumulator_rejects_oversized_prefix() {
        let mut accumulator = FrameAccumulator::new();
        let mut oversized = Vec::new();
        oversized.extend_from_slice(&(MAX_FRAME_SIZE as u32 + 1).to_be_bytes());
        let error = accumulator
            .push_bytes(&oversized)
            .expect_err("size limit enforced");
        assert!(matches!(error, FrameError::FrameTooLarge(_)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lifecycle_state_transitions_with_mock_transport() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let shutdown = Shutdown::new();
        let connector = MockConnector::new();
        let config = quiet_config();
        let task = tokio::spawn(run(
            commands_rx,
            events_tx,
            shutdown,
            connector.clone(),
            config,
        ));

        let device = test_device();
        commands_tx.send(Command::Select(device.clone())).unwrap();

        expect_connected(&mut events_rx).await;
        let pipe = connector.last_pipe().expect("pipe created on connect");
        let messages = drain_messages(&pipe, Duration::from_millis(500)).await;
        assert_eq!(messages.len(), 1, "only CONNECT after establishing");
        assert_eq!(messages[0].namespace, CONNECTION_NS);
        assert_eq!(messages[0].destination_id, TRANSPORT_ID);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&messages[0].payload_utf8).unwrap()["type"],
            "CONNECT"
        );

        commands_tx.send(Command::LaunchDefaultReceiver).unwrap();
        let messages = drain_messages(&pipe, Duration::from_millis(500)).await;
        assert_eq!(messages.len(), 1, "only LAUNCH after launch command");
        assert_eq!(messages[0].destination_id, RECEIVER_ID);
        assert_eq!(messages[0].namespace, RECEIVER_NS);
        let launch = serde_json::from_str::<serde_json::Value>(&messages[0].payload_utf8).unwrap();
        assert_eq!(launch["type"], "LAUNCH");
        assert_eq!(launch["requestId"], 1);
        assert_eq!(launch["appId"], "CC1AD845");

        pipe.push_incoming(&receiver_status_frame("t-42", "s-7"));
        match expect_event(&mut events_rx).await {
            ConnectionEvent::Ready {
                transport_id,
                session_id,
            } => {
                assert_eq!(transport_id, "t-42");
                assert_eq!(session_id, "s-7");
            }
            other => panic!("expected Ready, got {other:?}"),
        }

        commands_tx
            .send(Command::Load {
                content_id: "http://10.0.0.5:8080/stream".to_string(),
                content_type: "video/mp4".to_string(),
                stream_type: StreamType::Buffered,
            })
            .unwrap();
        let messages = drain_messages(&pipe, Duration::from_millis(500)).await;
        assert_eq!(
            messages.len(),
            1,
            "LOAD is queued until Ready then dispatched"
        );
        assert_eq!(messages[0].destination_id, media_destination_id("t-42"));
        assert_eq!(messages[0].namespace, MEDIA_NS);
        let load = serde_json::from_str::<serde_json::Value>(&messages[0].payload_utf8).unwrap();
        assert_eq!(load["type"], "LOAD");
        assert_eq!(load["media"]["contentId"], "http://10.0.0.5:8080/stream");
        assert_eq!(load["media"]["streamType"], "BUFFERED");

        pipe.push_incoming(&cast_frame(
            TRANSPORT_ID,
            SOURCE_ID,
            MEDIA_NS,
            r#"{"type":"MEDIA_STATUS","requestId":2,"status":[{"playerState":"PLAYING","mediaSessionId":3}]}"#,
        ));
        match expect_event(&mut events_rx).await {
            ConnectionEvent::MediaStatus {
                playing: true,
                buffering: false,
            } => {}
            other => panic!("expected playing media status, got {other:?}"),
        }

        commands_tx.send(Command::Stop).unwrap();
        let messages = drain_messages(&pipe, Duration::from_millis(500)).await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].namespace, MEDIA_NS);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&messages[0].payload_utf8).unwrap()["type"],
            "STOP"
        );

        commands_tx.send(Command::Shutdown).unwrap();
        let messages = drain_messages(&pipe, Duration::from_millis(500)).await;
        assert_eq!(messages.len(), 1, "STOP_APP during teardown");
        assert_eq!(messages[0].namespace, RECEIVER_NS);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&messages[0].payload_utf8).unwrap()["type"],
            "STOP_APP"
        );
        match expect_event(&mut events_rx).await {
            ConnectionEvent::Disconnected(device) => assert_eq!(device.id, "living-room"),
            other => panic!("expected Disconnected, got {other:?}"),
        }

        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("connection task exits after shutdown")
            .expect("task did not panic");
        assert!(pipe.is_closed(), "socket closed after teardown");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_pings_on_interval() {
        let (events_tx, _events_rx) = mpsc::unbounded_channel();
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let shutdown = Shutdown::new();
        let connector = MockConnector::new();
        let config = ConnectionConfig {
            heartbeat_interval: Duration::from_millis(50),
            watchdog_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(5),
            backoff: Backoff::with_params(Duration::from_millis(20), Duration::from_millis(20), 3),
        };
        let task = tokio::spawn(run(
            commands_rx,
            events_tx,
            shutdown,
            connector.clone(),
            config,
        ));

        commands_tx.send(Command::Select(test_device())).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let pipe = connector.last_pipe().expect("pipe created on connect");

        let mut pings = 0usize;
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            let chunk = pipe.wait_outgoing(deadline.saturating_duration_since(Instant::now()));
            if chunk.is_empty() {
                continue;
            }
            let mut cursor = std::io::Cursor::new(&chunk);
            while let Ok(Some(payload)) = read_frame(&mut cursor) {
                let message = decode_cast_message(&payload).expect("valid frame");
                if message.namespace == HEARTBEAT_NS
                    && serde_json::from_str::<serde_json::Value>(&message.payload_utf8).unwrap()["type"]
                        == "PING"
                {
                    pings += 1;
                }
            }
        }
        // Writes serialize behind the reader's read-hold (≈100ms), so the
        // observed cadence is slower than the 50ms interval.
        assert!(
            pings >= 5,
            "expected several PINGs in 1s at 50ms interval, got {pings}"
        );

        pipe.close();
        commands_tx.send(Command::Shutdown).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("task exits after shutdown")
            .expect("task did not panic");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pong_resets_watchdog_and_events_flow() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let shutdown = Shutdown::new();
        let connector = MockConnector::new();
        let config = test_config();
        let task = tokio::spawn(run(
            commands_rx,
            events_tx,
            shutdown,
            connector.clone(),
            config,
        ));

        commands_tx.send(Command::Select(test_device())).unwrap();
        expect_connected(&mut events_rx).await;
        let pipe = connector.last_pipe().expect("pipe created on connect");

        // Keep PONGing (60ms < 200ms watchdog); connection must survive.
        // PONGs must keep flowing during the no-disconnect window: once
        // they stop, the watchdog deadline (last PONG + 200ms) can land
        // inside it and fire legitimately.
        let pipe_for_pongs = pipe.clone();
        let ponger = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(60)).await;
                pipe_for_pongs.push_incoming(&cast_frame(
                    TRANSPORT_ID,
                    SOURCE_ID,
                    HEARTBEAT_NS,
                    r#"{"type":"PONG"}"#,
                ));
            }
        });
        tokio::time::timeout(Duration::from_millis(600), events_rx.recv())
            .await
            .expect_err("no disconnect while PONGs keep arriving");
        ponger.abort();

        pipe.close();
        commands_tx.send(Command::Shutdown).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("task exits after shutdown")
            .expect("task did not panic");
    }
}
