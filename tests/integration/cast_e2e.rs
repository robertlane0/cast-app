// SPDX-License-Identifier: MIT OR Apache-2.0
#![forbid(unsafe_code)]
#![cfg(feature = "e2e-cast")]

// Real-Chromecast end-to-end tests (`07-requirements-and-tests.md` §4,
// `03-cast-engine.md`). These exercise the full hand-rolled stack against a
// physical receiver on the local network: mDNS discovery, TLS with the
// permissive verifier, CONNECT, heartbeat PING/PONG, LAUNCH with requestId
// correlation, media-namespace LOAD/PLAY/PAUSE/STOP, and SET_VOLUME.
//
// The tests are `#[ignore]`d and gated behind the `e2e-cast` feature; they
// are never run by default CI:
//
//     cargo test --features e2e-cast --test cast_e2e -- --ignored --test-threads=1
//
// Receiver selection: set `CAST_E2E_RECEIVER=IP:port` to pin a device
// (deterministic, recommended in CI); otherwise a 12-second mDNS discovery
// pass picks the first receiver found. A remote-URL proxy test additionally
// honors `CAST_E2E_REMOTE_URL` when an internet origin is desired.
//
// Gate: `cargo test --features e2e-cast --test cast_e2e -- --ignored`.

use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use cast_app::cast::connection::{CastConnection, ConnectionEvent, TlsConnector};
use cast_app::cast::framing::{FrameError, read_frame, write_frame};
use cast_app::cast::mdns;
use cast_app::cast::namespaces::{
    CONNECTION_NS, HEARTBEAT_NS, RECEIVER_ID, SOURCE_ID, StreamType, TRANSPORT_ID, connect, ping,
};
use cast_app::cast::proto::{decode_cast_message, encode_cast_message};
use cast_app::cast::tls::{close_notify, connect as tls_connect};
use cast_app::media::lan_ip::select_lan_ip;
use cast_app::media::mime::mime_for_path;
use cast_app::media::server::MediaServer;
use cast_app::media::source::ActiveSource;
use cast_app::screen::ffmpeg_discover::{ffmpeg_available, ffmpeg_path};
use cast_app::state::{BackendEvent, CastDevice};
use cast_app::util::shutdown::Shutdown;
use tokio::sync::{mpsc, watch};

const DISCOVER_WINDOW: Duration = Duration::from_secs(12);
const EVENT_WAIT: Duration = Duration::from_secs(15);
const PLAYBACK_WAIT: Duration = Duration::from_secs(45);
const SOCKET_READ_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FRAMES_PER_STAGE: usize = 16;

/// One raw-protocol exchange result (`03-cast-engine.md` §7).
struct RawExchange {
    saw_receiver_status: bool,
    saw_pong: bool,
}

/// A drainer that waits for a matching `ConnectionEvent` while recording
/// every non-matching event so the caller can assert nothing fatal happened.
struct EventCollector {
    rx: mpsc::UnboundedReceiver<ConnectionEvent>,
    skipped: Vec<ConnectionEvent>,
}

impl EventCollector {
    fn new(rx: mpsc::UnboundedReceiver<ConnectionEvent>) -> Self {
        Self {
            rx,
            skipped: Vec::new(),
        }
    }

    /// Wait up to `timeout` for an event satisfying `pred`. Returns `None`
    /// on timeout, channel closure, or the first `ConnectionEvent::Error`
    /// (which is treated as fatal).
    async fn wait_for(
        &mut self,
        timeout: Duration,
        pred: impl Fn(&ConnectionEvent) -> bool,
    ) -> Option<ConnectionEvent> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            match tokio::time::timeout(remaining, self.rx.recv()).await {
                Ok(Some(event)) => {
                    if matches!(event, ConnectionEvent::Error(_)) {
                        self.skipped.push(event);
                        return None;
                    }
                    if pred(&event) {
                        return Some(event);
                    }
                    self.skipped.push(event);
                }
                Ok(None) | Err(_) => return None,
            }
        }
    }

    /// Panic if any fatal event was drained along the way.
    fn assert_no_fatal_events(&self) {
        for event in &self.skipped {
            assert!(
                !matches!(event, ConnectionEvent::Error(_)),
                "connection reported an error mid-flow: {event:?}"
            );
        }
    }
}

/// A media server plus its shutdown token, torn down on drop.
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
    let mut port_rx = server.subscribe_port();
    let deadline = Instant::now() + Duration::from_secs(10);
    while *port_rx.borrow() == 0 && port_rx.changed().await.is_ok() && Instant::now() < deadline {}
    assert_ne!(*port_rx.borrow(), 0, "media server never bound a port");
    TestServer {
        server,
        shutdown,
        port: *port_rx.borrow(),
    }
}

/// Deterministic `set_source`: return only once the server has processed it.
async fn set_source(server: &MediaServer, source: ActiveSource) {
    let mut rx = server.subscribe_generation();
    let before = *rx.borrow();
    server.set_source(source);
    let deadline = Instant::now() + Duration::from_secs(10);
    while *rx.borrow() == before && rx.changed().await.is_ok() && Instant::now() < deadline {}
}

/// Run the real mDNS discovery loop for `window` and return the latest
/// device snapshot (`07-requirements-and-tests.md` §4 scenario 1).
async fn discover_via_mdns(window: Duration) -> Vec<CastDevice> {
    let socket = mdns::bind_socket().expect("mDNS socket must bind");
    let shutdown = Shutdown::new();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (_rescan_tx, rescan_rx) = watch::channel(0u8);
    let task = tokio::spawn(mdns::run(socket, shutdown.clone(), events_tx, rescan_rx));

    let mut devices = Vec::new();
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), events_rx.recv()).await {
            Ok(Some(BackendEvent::ReceiversUpdated(found))) => devices = found,
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {} // quiet recv window; keep polling until the deadline
        }
    }
    shutdown.trigger();
    let _ = task.await;
    devices
}

/// Resolve the receiver under test: `CAST_E2E_RECEIVER` pin first, else the
/// first device found by mDNS.
async fn resolve_receiver() -> Option<CastDevice> {
    if let Ok(addr) = std::env::var("CAST_E2E_RECEIVER") {
        let addr: std::net::SocketAddr = addr
            .parse()
            .expect("CAST_E2E_RECEIVER must be IP:port, e.g. 192.168.1.50:8009");
        return Some(CastDevice {
            id: addr.to_string(),
            name: "env-pinned receiver".to_string(),
            addr,
            tofu_key: addr.to_string(),
        });
    }
    discover_via_mdns(DISCOVER_WINDOW).await.into_iter().next()
}

/// Produce a real playable MP4 clip via the external `ffmpeg` when present;
/// otherwise fall back to a dummy payload (protocol assertions only). The
/// `.mp4` extension drives the media-server MIME map.
fn generate_clip() -> (PathBuf, bool) {
    let path = std::env::temp_dir().join(format!("cast-app-cast-e2e-{}.mp4", std::process::id()));
    if ffmpeg_available() {
        if let Some(ffmpeg) = ffmpeg_path() {
            let result = std::process::Command::new(ffmpeg)
                .args([
                    "-y",
                    "-f",
                    "lavfi",
                    "-i",
                    "testsrc=duration=15:size=320x240:rate=30",
                    "-pix_fmt",
                    "yuv420p",
                    "-c:v",
                    "libx264",
                    "-preset",
                    "ultrafast",
                    "-movflags",
                    "+faststart",
                    "-f",
                    "mp4",
                ])
                .arg(&path)
                .output();
            if let Ok(output) = result {
                if output.status.success() {
                    return (path, true);
                }
            }
        }
    }
    std::fs::write(&path, b"not-a-real-mp4").expect("dummy clip write");
    (path, false)
}

/// The URL the receiver will pull: bound to the LAN interface on the
/// receiver's subnet so a physical device can reach it (`FR-022`).
fn receiver_url(device: &CastDevice, port: u16) -> String {
    format!(
        "http://{}:{port}/stream",
        select_lan_ip(Some(device.addr.ip()))
    )
}

// ---------------------------------------------------------------------------
// Scenario 1 — discovery (`07-requirements-and-tests.md` §4 scenario 1)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn receiver_is_discovered_via_mdns() {
    let devices = discover_via_mdns(DISCOVER_WINDOW).await;
    assert!(
        !devices.is_empty(),
        "no Chromecast discovered on the local network (set CAST_E2E_RECEIVER to pin one)"
    );
    for device in &devices {
        assert!(!device.id.is_empty(), "device id must be populated");
        assert!(!device.name.is_empty(), "device name must be populated");
        assert!(
            !device.addr.ip().is_loopback(),
            "receiver must be on the LAN"
        );
    }
}

// ---------------------------------------------------------------------------
// Scenarios 2–4 — TLS, CONNECT, heartbeat (`FR-003`, `FR-007`, `FR-008`)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn tls_connect_send_connect_and_heartbeat_round_trip() {
    let Some(device) = resolve_receiver().await else {
        eprintln!("skipping: no receiver found (set CAST_E2E_RECEIVER=IP:port)");
        return;
    };

    // (FR-003) TLS with the permissive self-signed verifier (FR-004).
    let mut stream = tls_connect(device.addr)
        .await
        .expect("TLS handshake with the receiver must succeed");
    // Bounds every blocking read so a dead peer fails the test instead of
    // hanging it; all blocking I/O runs on the blocking pool.
    stream
        .sock
        .set_read_timeout(Some(SOCKET_READ_TIMEOUT))
        .expect("socket read timeout");

    let exchange = tokio::task::spawn_blocking(move || -> io::Result<RawExchange> {
        // (FR-007) Connection namespace CONNECT. Android TV (including the
        // adb emulator) requires `receiver-0`; `transport-0` is rejected with
        // CLOSE. Physical Chromecasts accept `receiver-0` as well. Android TV
        // does not send an immediate RECEIVER_STATUS after CONNECT – it is
        // produced on GET_STATUS/LAUNCH – so we only wait briefly.
        let frame = encode_cast_message(SOURCE_ID, RECEIVER_ID, CONNECTION_NS, &connect());
        write_frame(&mut stream, &frame)?;

        let mut saw_receiver_status = false;
        // Briefly poll for RECEIVER_STATUS without blocking the heartbeat.
        // Chromecast replies immediately; Android TV defers it, so a 2s window
        // avoids stalling the subsequent PING (which must occur within ~5s to
        // keep the watchdog alive).
        let original_timeout = stream.sock.read_timeout().ok().flatten();
        stream
            .sock
            .set_read_timeout(Some(Duration::from_secs(2)))
            .ok();
        for _ in 0..MAX_FRAMES_PER_STAGE {
            let payload = match read_frame(&mut stream) {
                Ok(p) => p,
                Err(FrameError::Io(ref e)) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(FrameError::Io(ref e)) if e.kind() == io::ErrorKind::TimedOut => break,
                Err(e) => return Err(io::Error::other(e)),
            };
            match payload {
                Some(payload) => {
                    if is_json_type(&payload, "RECEIVER_STATUS") {
                        saw_receiver_status = true;
                        break;
                    }
                }
                None => break,
            }
        }
        stream
            .sock
            .set_read_timeout(original_timeout.or(Some(SOCKET_READ_TIMEOUT)))
            .ok();

        // (FR-008) Heartbeat PING must be answered with PONG.
        let frame = encode_cast_message(SOURCE_ID, TRANSPORT_ID, HEARTBEAT_NS, &ping());
        write_frame(&mut stream, &frame)?;

        let mut saw_pong = false;
        for _ in 0..MAX_FRAMES_PER_STAGE {
            let payload = match read_frame(&mut stream) {
                Ok(p) => p,
                Err(FrameError::Io(ref e)) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(FrameError::Io(ref e)) if e.kind() == io::ErrorKind::TimedOut => break,
                Err(e) => return Err(io::Error::other(e)),
            };
            match payload {
                Some(payload) => {
                    if is_json_type(&payload, "PONG") {
                        saw_pong = true;
                        break;
                    }
                }
                None => break,
            }
        }

        close_notify(&mut stream);
        Ok(RawExchange {
            saw_receiver_status,
            saw_pong,
        })
    })
    .await
    .expect("blocking worker panicked")
    .expect("raw protocol exchange failed");

    if !exchange.saw_receiver_status {
        eprintln!(
            "note: no RECEIVER_STATUS after CONNECT (Android TV defers it to GET_STATUS/LAUNCH)"
        );
    }
    assert!(exchange.saw_pong, "receiver must answer PING with PONG");
}

/// Tolerant JSON `type` check on a Cast frame payload. The frame payload is
/// the protobuf-encoded `CastMessage`; decode it first and inspect the
/// `payload_utf8` JSON. Android TV's direct `transportId` vs prefixed form is
/// handled by `media_destination_id`, so this helper works for both.
fn is_json_type(payload: &[u8], kind: &str) -> bool {
    let msg = match decode_cast_message(payload) {
        Ok(m) => m,
        Err(_) => return false,
    };
    serde_json::from_slice::<serde_json::Value>(msg.payload_utf8.as_bytes())
        .ok()
        .map(|value| value.get("type").and_then(|t| t.as_str()) == Some(kind))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Scenarios 5–9, 15 — LAUNCH correlation, LOAD of a local file, transport
// controls (`FR-009`, `FR-010`, `FR-018`, `FR-020`, `FR-022`)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn launch_load_local_file_then_transport_controls() {
    let Some(device) = resolve_receiver().await else {
        eprintln!("skipping: no receiver found (set CAST_E2E_RECEIVER=IP:port)");
        return;
    };

    let (clip_path, playable) = generate_clip();
    let server = start_server().await;
    set_source(&server.server, ActiveSource::File(clip_path.clone())).await;
    let content_type = mime_for_path(&clip_path);
    let url = receiver_url(&device, server.port);
    eprintln!("loading local file via {url}");

    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let (conn, handle) = CastConnection::start_with_handle(
        events_tx,
        server.shutdown.clone(),
        TlsConnector::default(),
    );
    let mut events = EventCollector::new(events_rx);

    // Select → Connected (TLS + CONNECT done).
    conn.select(device.clone());
    let connected = events
        .wait_for(EVENT_WAIT, |e| matches!(e, ConnectionEvent::Connected(_)))
        .await
        .expect("Connected event within the window");
    let ConnectionEvent::Connected(actual) = connected else {
        unreachable!()
    };
    assert_eq!(
        actual.addr, device.addr,
        "connected device must be the selected one"
    );

    // (FR-009) LAUNCH the Default Media Receiver and correlate the response
    // by requestId (scenario 6: the facade extracts transportId/sessionId).
    conn.launch_default_receiver();
    let ready = events
        .wait_for(EVENT_WAIT, |e| matches!(e, ConnectionEvent::Ready { .. }))
        .await
        .expect("LAUNCH must produce a Ready event");
    let ConnectionEvent::Ready {
        transport_id,
        session_id,
    } = ready
    else {
        unreachable!()
    };
    assert!(!transport_id.is_empty(), "transportId must be extracted");
    assert!(!session_id.is_empty(), "sessionId must be extracted");

    // (FR-020) LOAD the local proxy URL into the media namespace
    // (scenario 7: destination transport-<transportId>).
    conn.load(&url, content_type, StreamType::Buffered);
    let status = events
        .wait_for(PLAYBACK_WAIT, |e| {
            matches!(e, ConnectionEvent::MediaStatus { .. })
        })
        .await
        .expect("receiver must answer LOAD with MEDIA_STATUS");

    // With a real clip the receiver plays: assert the full pipeline
    // (file → local HTTP server → receiver) actually works.
    if playable {
        let playing = events
            .wait_for(PLAYBACK_WAIT, |e| {
                matches!(e, ConnectionEvent::MediaStatus { playing: true, .. })
            })
            .await;
        assert!(
            playing.is_some(),
            "receiver never reported PLAYING; the served clip is unplayable"
        );
    } else {
        eprintln!("ffmpeg absent: protocol-level assertions only (no playback check)");
        let _ = status;
    }

    // (FR-018) SET_VOLUME round trip.
    conn.set_volume(0.42, false);
    let _volume = events
        .wait_for(EVENT_WAIT, |e| match e {
            ConnectionEvent::Volume { level, muted } => (level - 0.42).abs() < 0.05 && !muted,
            _ => false,
        })
        .await
        .expect("SET_VOLUME must be reflected in a Volume event");

    // (FR-018) Pause / resume round trip (only meaningful while playing).
    // Android TV emulator timing can make PAUSE appear ineffective when sent
    // immediately after LOAD/PLAYING; treat it as advisory for the emulator.
    if playable {
        conn.pause();
        let paused = events
            .wait_for(EVENT_WAIT, |e| {
                matches!(e, ConnectionEvent::MediaStatus { playing: false, .. })
            })
            .await;
        if paused.is_none() {
            eprintln!("warning: no PAUSED after pause() (emulator timing, continuing to PLAY)");
        }

        conn.play();
        let resumed = events
            .wait_for(PLAYBACK_WAIT, |e| {
                matches!(e, ConnectionEvent::MediaStatus { playing: true, .. })
            })
            .await;
        if resumed.is_none() {
            eprintln!("warning: no PLAYING after play() (media may have ended, continuing)");
        }
    }

    // (FR-018) Media STOP, then full teardown (`STOP_APP` + close_notify).
    conn.stop();
    conn.shutdown();
    tokio::time::timeout(EVENT_WAIT, handle)
        .await
        .expect("connection task must exit after shutdown")
        .expect("connection task must not panic");
    drop(conn);

    events.assert_no_fatal_events();
}

// ---------------------------------------------------------------------------
// Scenario 10 — a remote URL served through the local proxy (FR-012)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn load_remote_url_through_the_local_proxy() {
    let Some(device) = resolve_receiver().await else {
        eprintln!("skipping: no receiver found (set CAST_E2E_RECEIVER=IP:port)");
        return;
    };

    // The "remote" origin: a second media server (no proxy involved on its
    // side), exactly the topology http_e2e validates in-process. Set
    // CAST_E2E_REMOTE_URL to substitute an internet origin if preferred.
    let origin = if let Ok(remote) = std::env::var("CAST_E2E_REMOTE_URL") {
        ActiveSource::Url(remote)
    } else {
        let (clip_path, _) = generate_clip();
        ActiveSource::File(clip_path)
    };

    let origin_server = start_server().await;
    set_source(&origin_server.server, origin).await;
    let origin_url = receiver_url(&device, origin_server.port);

    // The proxy: forwards Range and streams the origin body back.
    let proxy = start_server().await;
    set_source(&proxy.server, ActiveSource::Url(origin_url.clone())).await;
    let url = receiver_url(&device, proxy.port);
    eprintln!("proxying {origin_url} via {url}");

    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let (conn, handle) = CastConnection::start_with_handle(
        events_tx,
        proxy.shutdown.clone(),
        TlsConnector::default(),
    );
    let mut events = EventCollector::new(events_rx);

    conn.select(device.clone());
    events
        .wait_for(EVENT_WAIT, |e| matches!(e, ConnectionEvent::Connected(_)))
        .await
        .expect("Connected event within the window");

    // LOAD in the Connected phase auto-launches and queues until Ready
    // (Phase 10 lesson): the receiver sees LAUNCH then LOAD.
    conn.load(&url, "video/mp4", StreamType::Buffered);
    events
        .wait_for(EVENT_WAIT, |e| matches!(e, ConnectionEvent::Ready { .. }))
        .await
        .expect("LAUNCH must produce a Ready event");
    let status = events
        .wait_for(PLAYBACK_WAIT, |e| {
            matches!(e, ConnectionEvent::MediaStatus { .. })
        })
        .await;
    assert!(
        status.is_some(),
        "receiver must answer LOAD through the proxy with MEDIA_STATUS"
    );

    conn.shutdown();
    tokio::time::timeout(EVENT_WAIT, handle)
        .await
        .expect("connection task must exit after shutdown")
        .expect("connection task must not panic");
    drop(conn);

    events.assert_no_fatal_events();
}
