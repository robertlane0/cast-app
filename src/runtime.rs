// SPDX-License-Identifier: MIT OR Apache-2.0
//! Tokio runtime construction and the backend supervisor (`06-concurrency.md`
//! §5): spawns Task A (mDNS), Task B (Cast), Task C (HTTP) and the screen
//! capture thread, routes GUI commands, aggregates every `BackendEvent` into
//! a single upward channel, and coordinates shutdown in the documented order
//! (HTTP stops accepting → Cast closes → mDNS stops → capture joins →
//! ffmpeg killed; AGENTS.md §10).

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::runtime::Runtime;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, watch};

use crate::cast::connection::{CastConnection, ConnectionEvent, Connector, TlsConnector};
use crate::cast::mdns;
use crate::cast::namespaces::StreamType;
use crate::media::lan_ip;
use crate::media::mime;
use crate::media::server::{DEFAULT_PORT, MediaServer};
use crate::media::source::ActiveSource;
use crate::screen::bridge::ScreenBridge;
use crate::state::{AppCommand, BackendEvent, SourceTab};
use crate::util::shutdown::Shutdown;

/// Deadline the runtime gives straggler tasks after the supervisor ends.
const RUNTIME_DRAIN: Duration = Duration::from_secs(5);

/// MIME type served by the screen pipeline (`04-media-proxy.md` §5).
const SCREEN_MIME: &str = "video/mp4";

/// The backend: owns the multi-threaded tokio runtime, the shutdown token
/// and the supervisor task. Created by [`Backend::start`], torn down with
/// [`Backend::shutdown`].
pub struct Backend {
    runtime: Runtime,
    shutdown: Shutdown,
    supervisor: Option<tokio::task::JoinHandle<()>>,
}

impl Backend {
    /// Build the runtime and spawn the backend supervisor with the real
    /// connectors (mDNS multicast socket + TLS Cast transport).
    ///
    /// Returns the `Backend` plus the GUI's channel endpoints
    /// (`02-gui.md` §3): commands down, events up.
    pub fn start() -> (
        Self,
        UnboundedSender<AppCommand>,
        UnboundedReceiver<BackendEvent>,
    ) {
        Self::start_with(mdns::bind_socket(), DEFAULT_PORT, TlsConnector)
    }

    /// Same as [`Backend::start`] with injectable discovery and connector
    /// plumbing (tests substitute a mock connector and a pre-bound socket).
    ///
    /// `discovery` is the result of binding the mDNS socket: an `Err` is the
    /// fatal-discovery path (`03-cast-engine.md` §2.5) surfaced to the GUI as
    /// a `ConnectionError` and retried via `AppCommand::Rescan`.
    /// `initial_port` is the media-server bind port (0 = ephemeral).
    pub fn start_with<C: Connector>(
        discovery: Result<std::net::UdpSocket, io::Error>,
        initial_port: u16,
        connector: C,
    ) -> (
        Self,
        UnboundedSender<AppCommand>,
        UnboundedReceiver<BackendEvent>,
    ) {
        let runtime = Runtime::new().expect("failed to build the tokio runtime");
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let shutdown = Shutdown::new();
        let supervisor = runtime.spawn(supervise(
            command_rx,
            event_tx.clone(),
            shutdown.clone(),
            connector,
            discovery,
            initial_port,
        ));
        (
            Self {
                runtime,
                shutdown,
                supervisor: Some(supervisor),
            },
            command_tx,
            event_rx,
        )
    }

    /// Coordinated shutdown: trigger the token, wait for the supervisor (all
    /// tasks joined, capture thread joined, `ffmpeg` reaped), then give
    /// straggler tasks a bounded drain window before the runtime drops.
    ///
    /// Blocks the calling thread; must not be called from inside a tokio
    /// runtime context.
    pub fn shutdown(mut self) {
        self.shutdown.trigger();
        if let Some(task) = self.supervisor.take() {
            let _ = self.runtime.block_on(task);
        }
        self.runtime.shutdown_timeout(RUNTIME_DRAIN);
    }
}

/// Mutable backend state owned by the supervisor task. Authoritative copies
/// of receiver list (mDNS task), connection state (Cast task) and active
/// source (media server task) live in their owning tasks; this struct keeps
/// only what the supervisor itself must route and mirror.
struct SupervisorState {
    events: UnboundedSender<BackendEvent>,
    cast: CastConnection,
    server: MediaServer,
    screen: Option<ScreenBridge>,
    /// The discovery task; `None` after a fatal socket setup, retried by
    /// `AppCommand::Rescan`.
    mdns_task: Option<tokio::task::JoinHandle<()>>,
    /// Bumped on `AppCommand::Rescan` for an immediate mDNS re-query.
    rescan: watch::Sender<u8>,
    /// LAN IP used for the advertised `/stream` URL (`04-media-proxy.md`
    /// §1.1); re-selected when the receiver selection changes.
    lan_ip: IpAddr,
    /// The configured proxy port (default `8080`; `0` = ephemeral in tests).
    /// Rebind targets use this port.
    proxy_port: u16,
    /// Whether the user has explicitly allowed the `0.0.0.0` wildcard bind
    /// (`04-media-proxy.md` §1.1). Latches for the session once granted;
    /// before that, every wildcard fallback asks via
    /// `BackendEvent::BindFallbackRequested`.
    wildcard_consented: bool,
    /// A consent request is outstanding; no new request is sent until the
    /// user answers (`AppCommand::BindFallback`).
    fallback_pending: bool,
    /// The source the next `Play` loads (`04-media-proxy.md` §1.2).
    current_source: Option<ActiveSource>,
    last_volume: f32,
    muted: bool,
}

impl SupervisorState {
    async fn handle_command(&mut self, command: AppCommand, shutdown: Shutdown) {
        match command {
            AppCommand::SelectReceiver(device) => {
                // LAN IP re-selection on receiver change (`04-media-proxy.md`
                // §1.1): the advertised URL must be reachable by the receiver,
                // and the listener rebinds to the resolved interface so the
                // exposure stays limited to the receiver's LAN segment.
                self.lan_ip = lan_ip::select_lan_ip(Some(device.addr.ip()));
                self.cast.select(device);
                self.rebind_to_lan_ip().await;
            }
            AppCommand::BindFallback(consent) => {
                self.fallback_pending = false;
                if consent {
                    self.wildcard_consented = true;
                    self.bind_wildcard().await;
                } else {
                    tracing::warn!(
                        "user declined the wildcard media-server bind; no listener until a receiver's interface binds"
                    );
                }
            }
            AppCommand::SelectSource(tab) => {
                // Tab switches are GUI-side enablement; re-enumerate displays
                // when the Display tab is opened so hotplug is picked up.
                if tab == SourceTab::Display {
                    self.refresh_displays().await;
                }
            }
            AppCommand::SelectDisplay(monitor) => {
                // Replace any running pipeline (the old one is stopped and
                // joined in the background; its encoder is torn down).
                if let Some(old) = self.screen.take() {
                    old.stop();
                    tokio::task::spawn_blocking(move || old.join());
                }
                self.current_source = Some(ActiveSource::Screen(monitor.clone()));
                self.server
                    .set_source(ActiveSource::Screen(monitor.clone()));
                // The pipeline start probes the display/portal service
                // (xcap, zbus round-trips) and must not run on the executor
                // task (AGENTS.md §12; on Wayland the portal dialog itself
                // lives on the controller thread, so this is quick).
                let events = self.events.clone();
                let server = self.server.clone();
                let spawn = tokio::task::spawn_blocking(move || {
                    ScreenBridge::start(monitor, server, events, shutdown)
                });
                match spawn.await {
                    Ok(Ok(bridge)) => self.screen = Some(bridge),
                    Ok(Err(error)) => {
                        tracing::error!(%error, "screen pipeline failed to start");
                        let _ = self.events.send(BackendEvent::StreamError(error));
                    }
                    Err(error) => {
                        tracing::error!(%error, "screen pipeline start task failed");
                        let _ = self.events.send(BackendEvent::StreamError(
                            "screen pipeline failed to start".to_string(),
                        ));
                    }
                }
            }
            AppCommand::SelectFile(path) => {
                self.current_source = Some(ActiveSource::File(path.clone()));
                self.server.set_source(ActiveSource::File(path));
            }
            AppCommand::SelectUrl(url) => {
                self.current_source = Some(ActiveSource::Url(url.clone()));
                self.server.set_source(ActiveSource::Url(url));
            }
            AppCommand::Play => {
                let Some(source) = self.current_source.clone() else {
                    tracing::warn!("Play without an active source; ignored");
                    return;
                };
                let port = self.server.bound_port();
                if port == 0 {
                    tracing::warn!("media server not bound yet; Play ignored");
                    return;
                }
                let content_id = format!("http://{}:{port}/stream", self.lan_ip);
                match source {
                    ActiveSource::File(path) => {
                        self.cast.load(
                            &content_id,
                            mime::mime_for_path(&path),
                            StreamType::Buffered,
                        );
                    }
                    ActiveSource::Url(raw) => {
                        self.cast.load(
                            &content_id,
                            content_type_for_url(&raw),
                            StreamType::Buffered,
                        );
                    }
                    ActiveSource::Screen(_) => {
                        self.cast.load(&content_id, SCREEN_MIME, StreamType::Live);
                    }
                }
            }
            AppCommand::Pause => self.cast.pause(),
            AppCommand::Stop => self.cast.stop(),
            AppCommand::SetVolume(level) => {
                self.last_volume = level;
                self.cast.set_volume(level, self.muted);
            }
            AppCommand::Mute(muted) => {
                self.muted = muted;
                self.cast.set_volume(self.last_volume, muted);
            }
            AppCommand::SetProxyPort(port) => {
                self.proxy_port = port;
                self.server.set_port(port);
            }
            AppCommand::Rescan => {
                // Immediate mDNS re-query (GUI Error-state retry, `02-gui.md`
                // §3.1). Multiple bumps coalesce inside the watch. Note:
                // `send_replace` (not `send`) because `send` would take the
                // value write-lock while the `Ref` from `borrow()` below is
                // still held — a self-deadlock on the watch RwLock.
                let next = self.rescan.borrow().wrapping_add(1);
                self.rescan.send_replace(next);
                // Discovery never started (fatal socket setup): retry now.
                if self.mdns_task.is_none() {
                    tracing::debug!("rescan: rebinding the mDNS socket");
                    match mdns::bind_socket() {
                        Ok(socket) => {
                            let (rescan_tx, rescan_rx) = watch::channel(0u8);
                            self.mdns_task = Some(tokio::spawn(mdns::run(
                                socket,
                                shutdown,
                                self.events.clone(),
                                rescan_rx,
                            )));
                            self.rescan = rescan_tx;
                            tracing::info!("mDNS discovery re-established");
                        }
                        Err(error) => {
                            tracing::warn!(%error, "mDNS rebind failed on rescan");
                        }
                    }
                }
            }
        }
    }

    /// Bind the listener to the interface address resolved for the current
    /// receiver (`04-media-proxy.md` §1.1), tightening exposure to the
    /// receiver's LAN segment. When that bind fails, fall back to `0.0.0.0`
    /// — directly if the user already consented this session, otherwise by
    /// asking via `BackendEvent::BindFallbackRequested`.
    async fn rebind_to_lan_ip(&mut self) {
        let addr = SocketAddr::new(self.lan_ip, self.proxy_port);
        if self.bind_addr(addr).await.is_ok() {
            return;
        }
        if self.wildcard_consented {
            self.bind_wildcard().await;
        } else {
            self.request_wildcard_fallback(format!(
                "Binding the media server to {addr} failed, so the Chromecast could not reach the stream. The app wants to fall back to binding all interfaces (0.0.0.0)."
            ));
        }
    }

    /// Bind the listener to the wildcard address on the configured port.
    async fn bind_wildcard(&mut self) {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.proxy_port);
        if let Err(error) = self.bind_addr(addr).await {
            tracing::error!(%error, "wildcard media-server bind failed");
        }
    }

    /// Send a `SetBindAddr` command and await the server task's ack.
    async fn bind_addr(&self, addr: SocketAddr) -> io::Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.server.set_bind_addr(addr, ack_tx);
        match ack_rx.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "media server task ended",
            )),
        }
    }

    /// Ask the user (GUI pop-up) for permission to bind the media server to
    /// `0.0.0.0`, explaining what failed and the exposure it creates
    /// (`04-media-proxy.md` §1.1). At most one request is outstanding.
    fn request_wildcard_fallback(&mut self, reason: String) {
        if self.fallback_pending {
            tracing::debug!("wildcard-fallback consent already requested; not asking again");
            return;
        }
        self.fallback_pending = true;
        tracing::warn!(%reason, "requesting user consent for the wildcard media-server bind");
        let _ = self
            .events
            .send(BackendEvent::BindFallbackRequested(reason));
    }

    async fn refresh_displays(&self) {
        // `monitor_names` probes the display backend (xcap) and, on Wayland,
        // the portal service over the session bus: keep it off the executor.
        let result = tokio::task::spawn_blocking(crate::screen::capture::monitor_names).await;
        match result {
            Ok(Ok(names)) => {
                let _ = self.events.send(BackendEvent::DisplaysUpdated(names));
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "display enumeration failed");
            }
            Err(error) => {
                tracing::warn!(%error, "display enumeration task failed");
            }
        }
    }
}

/// The supervisor task (`06-concurrency.md` §5): owns the shutdown token and
/// the task graph; routes commands; aggregates events; exits on the token,
/// on GUI disconnect, or on a dead child task, then performs the coordinated
/// shutdown sequence.
async fn supervise<C: Connector>(
    mut commands: UnboundedReceiver<AppCommand>,
    events: UnboundedSender<BackendEvent>,
    shutdown: Shutdown,
    connector: C,
    discovery: Result<std::net::UdpSocket, io::Error>,
    initial_port: u16,
) {
    // Displays are enumerated once at startup and again when the Display tab
    // is opened (see `handle_command`). The probe hits the display backend
    // (xcap, and on Wayland the portal service over the session bus), so it
    // runs on a blocking worker.
    let displays = tokio::task::spawn_blocking(crate::screen::capture::monitor_names).await;
    match displays {
        Ok(Ok(names)) => {
            let _ = events.send(BackendEvent::DisplaysUpdated(names));
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "display enumeration failed at startup");
        }
        Err(error) => {
            tracing::warn!(%error, "display enumeration task failed at startup");
        }
    }

    // Task A — mDNS discovery. A fatal setup failure surfaces as a
    // `ConnectionError` and discovery stays halted until `Rescan`.
    let (rescan_tx, rescan_rx) = watch::channel(0u8);
    let mdns_task = match discovery {
        Ok(socket) => {
            let handle = tokio::spawn(mdns::run(
                socket,
                shutdown.clone(),
                events.clone(),
                rescan_rx,
            ));
            Some(handle)
        }
        Err(error) => {
            tracing::error!(%error, "mDNS socket setup failed; discovery halted");
            let _ = events.send(BackendEvent::ConnectionError(format!(
                "mDNS discovery failed: {error}"
            )));
            None
        }
    };

    // Task B — Cast connection (owns its own reconnect policy).
    let (connection_tx, mut connection_rx) = mpsc::unbounded_channel();
    let (cast, cast_handle) =
        CastConnection::start_with_handle(connection_tx, shutdown.clone(), connector);

    // Task C — media server. Starts **unbound**: the interface to bind is
    // unknown until a receiver is selected, and the `0.0.0.0` wildcard
    // fallback is gated on explicit user consent (`04-media-proxy.md` §1.1).
    let (server, server_handle) = MediaServer::start_unbound_with_handle(shutdown.clone());

    // The supervisor mirrors the receiver's LAN IP and the server's bound
    // port so `Play` can build the advertised URL.
    let mut state = SupervisorState {
        events,
        cast,
        server,
        screen: None,
        mdns_task,
        rescan: rescan_tx,
        lan_ip: lan_ip::select_lan_ip(None),
        proxy_port: initial_port,
        wildcard_consented: false,
        fallback_pending: false,
        current_source: None,
        last_volume: 0.0,
        muted: false,
    };

    // No receiver is selected yet, so the interface to restrict the media
    // server to cannot be determined: ask the user before binding `0.0.0.0`
    // (`04-media-proxy.md` §1.1). The listener stays unbound until the
    // answer (or until a receiver selection resolves a specific interface).
    state.request_wildcard_fallback(
        "No Chromecast receiver is selected yet, so the app cannot determine which network interface the receiver is on. The app wants to fall back to binding all interfaces (0.0.0.0) so the media server is reachable once a receiver is chosen.".to_string(),
    );

    let mut port_rx = state.server.subscribe_port();
    let mut shutdown_rx = shutdown.subscribe();

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    break; // GUI dropped (application exit)
                };
                state.handle_command(command, shutdown.clone()).await;
            }
            event = connection_rx.recv() => {
                let Some(event) = event else {
                    break; // cast task ended
                };
                forward_connection_event(&state.events, event, &mut state.last_volume, &mut state.muted);
            }
            changed = port_rx.changed() => {
                if changed.is_err() {
                    break; // media server task ended
                }
            }
            _ = shutdown_rx.changed() => break,
        }
    }

    // Coordinated shutdown (`06-concurrency.md` §5; AGENTS.md §10): HTTP
    // stops accepting first so no new /stream connections arrive while the
    // Cast session tears down, then the Cast session closes, then mDNS
    // stops, then the capture thread joins and ffmpeg is killed.
    tracing::info!("backend supervisor exiting; coordinated shutdown");
    let SupervisorState {
        events,
        cast,
        server,
        screen,
        mdns_task,
        rescan: _rescan,
        lan_ip: _lan_ip,
        proxy_port: _proxy_port,
        wildcard_consented: _wildcard_consented,
        fallback_pending: _fallback_pending,
        current_source: _current_source,
        mut last_volume,
        mut muted,
    } = state;

    // 1. HTTP listener: stop accepting, release the port.
    server.shutdown();
    drop(server);
    let _ = server_handle.await;

    // 2. Cast session: STOP → STOP_APP → close_notify; drain the task's
    //    final events (e.g. Disconnected) until the task ends.
    cast.shutdown();
    drop(cast);
    while let Some(event) = connection_rx.recv().await {
        forward_connection_event(&events, event, &mut last_volume, &mut muted);
    }
    let _ = cast_handle.await;

    // 3. mDNS stops on the token.
    if let Some(task) = mdns_task {
        let _ = task.await;
    }

    // 4. Capture thread joins; the encoder child is killed and reaped
    //    (`screen::bridge::ScreenBridge::join`).
    if let Some(bridge) = screen {
        bridge.stop();
        let _ = tokio::task::spawn_blocking(move || bridge.join()).await;
    }
    tracing::info!("backend shutdown complete");
}

/// Aggregate one cast-task event into the single backend→GUI channel,
/// mirroring the receiver's last-known volume (`06-concurrency.md` §3).
fn forward_connection_event(
    events: &UnboundedSender<BackendEvent>,
    event: ConnectionEvent,
    last_volume: &mut f32,
    muted: &mut bool,
) {
    match event {
        ConnectionEvent::Connected(device) => {
            let _ = events.send(BackendEvent::ReceiverConnected(device));
        }
        ConnectionEvent::Disconnected(device) => {
            let _ = events.send(BackendEvent::ReceiverDisconnected(device));
        }
        ConnectionEvent::Error(error) => {
            // Fatal cast failure: surface to the GUI (`06-concurrency.md
            // §5). Reconnect exhaustion halts the session; the GUI shows the
            // error and the user re-selects.
            let _ = events.send(BackendEvent::ConnectionError(error.to_string()));
        }
        ConnectionEvent::Ready { .. } => {}
        ConnectionEvent::MediaStatus { playing, buffering } => {
            let _ = events.send(BackendEvent::MediaStatus { playing, buffering });
        }
        ConnectionEvent::Volume {
            level,
            muted: event_muted,
        } => {
            *last_volume = level;
            *muted = event_muted;
            let _ = events.send(BackendEvent::Volume {
                level,
                muted: event_muted,
            });
        }
    }
}

/// The `LOAD` contentType for a remote URL source: the MIME of the URL path
/// extension, defaulting to `application/octet-stream` when the path has no
/// usable extension.
fn content_type_for_url(raw: &str) -> &'static str {
    let extension = url::Url::parse(raw)
        .map(|parsed| {
            parsed
                .path()
                .rsplit('.')
                .next()
                .unwrap_or_default()
                .to_string()
        })
        .unwrap_or_default();
    mime::mime_for_extension(&extension)
}
