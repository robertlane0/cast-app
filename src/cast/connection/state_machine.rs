// SPDX-License-Identifier: MIT OR Apache-2.0
//! Connection state machine (`03-cast-engine.md` §7): the lifecycle phases,
//! commands, events, inbound routing, command dispatch, heartbeat watchdog,
//! reconnect policy, connect flow and the [`run`] loop. Blocking TLS I/O
//! never runs here directly — it goes through [`super::writer::send_payload`]
//! (`spawn_blocking`) and [`super::reader::spawn_reader`] (dedicated
//! thread), per the Phase 3 lesson.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, Sleep};

use crate::cast::connection::reader::spawn_reader;
use crate::cast::connection::teardown::teardown_session;
use crate::cast::connection::transport::SharedTransport;
use crate::cast::connection::writer::send_payload;
use crate::cast::namespaces::{
    CONNECTION_NS, HEARTBEAT_NS, MEDIA_NS, RECEIVER_ID, RECEIVER_NS, SOURCE_ID, StreamType,
    TRANSPORT_ID, connect, launch, load, media_destination_id, parse_media_status,
    parse_receiver_status, pause, ping, play, set_volume, stop,
};
use crate::cast::proto::{decode_cast_message, encode_cast_message};
use crate::cast::request_id::{PendingMap, RequestId};
use crate::cast::tls::TlsError;
use crate::cast::tofu::{Fingerprint, PinCheck};
use crate::state::CastDevice;
use crate::util::retry::Backoff;
use crate::util::shutdown::Shutdown;

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
// Commands and events
// ---------------------------------------------------------------------------

/// Commands sent to the connection task. [`run`] consumes these; the GUI or
/// runtime dispatches through [`crate::cast::connection::CastConnection`].
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
    /// TOFU pin mismatch (`03-cast-engine.md` §3.1): the certificate the
    /// receiver presented differs from the one first seen. The connection
    /// proceeds; the payload carries both digests for the warning message.
    CertificateMismatch {
        device: CastDevice,
        previous: Fingerprint,
        current: Fingerprint,
    },
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
/// Fields needed by [`super::teardown::teardown_session`] are
/// `pub(super)`.
pub(super) struct Session {
    pub(super) phase: Phase,
    pub(super) device: CastDevice,
    pub(super) transport: SharedTransport,
    request_id: RequestId,
    pending: PendingMap,
    transport_id: Option<String>,
    pub(super) session_id: Option<String>,
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
    pub(super) fn next_request(&mut self) -> u32 {
        let id = self.request_id.allocate();
        self.pending.insert(id, Instant::now());
        id
    }

    fn take_pending_command(&mut self) -> Option<PendingCommand> {
        self.pending_command.take()
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
    pub(super) fn media_destination(&self) -> String {
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
// Run-loop state
// ---------------------------------------------------------------------------

/// All mutable state owned by the [`run`] loop, bundled so helpers take a
/// single reference instead of a long argument list. Fields are borrowed
/// disjointly by the `tokio::select!` branches. Fields needed by
/// [`super::teardown::teardown_session`] are `pub(super)`.
pub(super) struct RunState {
    pub(super) session: Option<Session>,
    desired: Option<CastDevice>,
    pub(super) inbound: Option<mpsc::UnboundedReceiver<Option<Vec<u8>>>>,
    pub(super) heartbeat: Option<std::pin::Pin<Box<Sleep>>>,
    pub(super) watchdog: Option<std::pin::Pin<Box<Sleep>>>,
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
async fn establish<C: crate::cast::connection::transport::Connector>(
    state: &mut RunState,
    device: CastDevice,
    connector: &C,
    events: &mpsc::UnboundedSender<ConnectionEvent>,
    config: &ConnectionConfig,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), ConnectionError> {
    let (transport, pin_check) =
        connector
            .connect(&device)
            .await
            .map_err(|source| ConnectionError::Tls {
                addr: device.addr,
                source,
            })?;

    // A TOFU mismatch never blocks the connection (`03-cast-engine.md`
    // §3.1), but it must reach the GUI as a warning.
    if let PinCheck::Mismatch { previous, current } = pin_check {
        let _ = events.send(ConnectionEvent::CertificateMismatch {
            device: device.clone(),
            previous,
            current,
        });
    }

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
pub async fn run<C: crate::cast::connection::transport::Connector>(
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
