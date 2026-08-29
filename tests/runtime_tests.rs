// SPDX-License-Identifier: MIT OR Apache-2.0
#![forbid(unsafe_code)]

//! Backend supervisor tests (`06-concurrency.md` §5): the whole task graph
//! runs in-process with a mock Cast transport — command routing, event
//! aggregation, fatal-discovery handling and the coordinated shutdown
//! ordering. Uses `Backend::start_with` with a pre-bound UDP socket, an
//! ephemeral media-server port, and `MockConnector` (which lives in
//! `cast::connection::test_support`).
//!
//! These tests are plain `#[test]`: `Backend::shutdown` drives the runtime
//! with `block_on` on the calling thread, which panics inside a tokio
//! runtime context.

use cast_app::cast::connection::test_support::{MockConnector, MockPipe};
use cast_app::cast::framing::{encode_frame, read_frame};
use cast_app::cast::namespaces::{MEDIA_NS, RECEIVER_ID, RECEIVER_NS, SOURCE_ID, TRANSPORT_ID};
use cast_app::cast::proto::{decode_cast_message, encode_cast_message};
use cast_app::runtime::Backend;
use cast_app::state::{AppCommand, BackendEvent, CastDevice};
use std::io::{self, Cursor};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

fn test_device() -> CastDevice {
    CastDevice {
        id: "living-room".to_string(),
        name: "Living Room".to_string(),
        addr: "10.0.0.5:8009".parse().expect("valid test address"),
        tofu_key: "Living Room+10.0.0.5".to_string(),
    }
}

/// A pre-bound, non-blocking socket for the discovery task; never actually
/// queried by these tests (nothing listens), it just exercises the healthy
/// path. Non-blocking is required: the mDNS task registers it with tokio via
/// `UdpSocket::from_std`, which panics on a blocking socket.
fn discovery_socket() -> io::Result<UdpSocket> {
    let socket = UdpSocket::bind(("0.0.0.0", 0))?;
    socket.set_nonblocking(true)?;
    Ok(socket)
}

fn cast_frame(source: &str, dest: &str, ns: &str, payload: &str) -> Vec<u8> {
    encode_frame(&encode_cast_message(source, dest, ns, payload))
}

fn receiver_status_frame(transport_id: &str, session_id: &str, level: f64, muted: bool) -> Vec<u8> {
    cast_frame(
        RECEIVER_ID,
        SOURCE_ID,
        RECEIVER_NS,
        &format!(
            r#"{{"type":"RECEIVER_STATUS","requestId":0,"status":{{"applications":[{{"appId":"CC1AD845","sessionId":"{session_id}","transportId":"{transport_id}"}}],"volume":{{"level":{level},"muted":{muted}}}}}}}"#,
        ),
    )
}

fn media_status_frame(player_state: &str) -> Vec<u8> {
    cast_frame(
        TRANSPORT_ID,
        SOURCE_ID,
        MEDIA_NS,
        &format!(
            r#"{{"type":"MEDIA_STATUS","requestId":0,"status":[{{"playerState":"{player_state}","mediaSessionId":3}}]}}"#
        ),
    )
}

/// Poll the GUI event channel on the test thread until an event arrives.
fn expect_event(rx: &mut mpsc::UnboundedReceiver<BackendEvent>) -> BackendEvent {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match rx.try_recv() {
            Ok(event) => return event,
            Err(mpsc::error::TryRecvError::Empty) => {
                assert!(Instant::now() < deadline, "event within timeout");
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                panic!("backend event channel closed unexpectedly");
            }
        }
    }
}

/// Poll until an event matching `predicate` arrives, draining earlier events.
/// Startup events such as `DisplaysUpdated` are platform-dependent (a headless
/// Linux CI runner emits none, a Windows desktop emits one per monitor), so
/// tests asserting on a specific event must not require it to be the first.
fn expect_event_matching(
    rx: &mut mpsc::UnboundedReceiver<BackendEvent>,
    mut predicate: impl FnMut(&BackendEvent) -> bool,
) -> BackendEvent {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match rx.try_recv() {
            Ok(event) => {
                if predicate(&event) {
                    return event;
                }
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                assert!(Instant::now() < deadline, "event within timeout");
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                panic!("backend event channel closed unexpectedly");
            }
        }
    }
}

/// Consume whatever the supervisor emitted at startup (e.g. `DisplaysUpdated`
/// when enumeration succeeds, `ConnectionError` when the discovery socket
/// could not bind). Keeps the deterministic assertions below aligned with the
/// first event of the scenario under test.
fn drain_startup_events(rx: &mut mpsc::UnboundedReceiver<BackendEvent>) {
    let deadline = Instant::now() + Duration::from_millis(300);
    loop {
        match rx.try_recv() {
            Ok(_) => {}
            Err(mpsc::error::TryRecvError::Empty) => {
                if Instant::now() > deadline {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                panic!("backend event channel closed unexpectedly");
            }
        }
    }
}

/// Collect everything written to `pipe` within `window` and parse it into
/// `(namespace, json)` messages (mirrors the connection_tests helpers).
fn drain_wire(pipe: &MockPipe, window: Duration) -> Vec<(String, serde_json::Value)> {
    let mut accumulated = Vec::new();
    let mut deadline = Instant::now() + window;
    loop {
        let chunk = pipe.wait_outgoing(deadline.saturating_duration_since(Instant::now()));
        if chunk.is_empty() {
            break;
        }
        accumulated.extend_from_slice(&chunk);
        deadline = Instant::now() + window;
    }
    let mut out = Vec::new();
    let mut cursor = Cursor::new(&accumulated);
    while let Ok(Some(payload)) = read_frame(&mut cursor) {
        let message = decode_cast_message(&payload).expect("valid frame");
        let json = serde_json::from_str(&message.payload_utf8).unwrap_or(serde_json::Value::Null);
        out.push((message.namespace, json));
    }
    out
}

/// Poll the cast wire until a frame matching `predicate` arrives, draining
/// earlier frames along the way. Unlike `drain_wire` the deadline is absolute
/// (5s, mirroring `expect_event_matching`), so a cold CI runner cannot starve
/// the assertion by delaying the frame past a fixed window.
fn expect_wire_frame(
    pipe: &MockPipe,
    message: &str,
    mut predicate: impl FnMut(&str, &serde_json::Value) -> bool,
) -> (String, serde_json::Value) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut accumulated: Vec<u8> = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "{message}");
        let chunk = pipe.wait_outgoing(remaining.min(Duration::from_millis(50)));
        if !chunk.is_empty() {
            accumulated.extend_from_slice(&chunk);
        }
        if chunk.is_empty() && pipe.is_closed() {
            panic!("{message}");
        }

        // Parse complete frames from the front; a partial trailing frame is
        // kept for the next poll.
        let mut cursor = Cursor::new(&accumulated);
        let mut consumed = 0usize;
        loop {
            let before = cursor.position() as usize;
            let payload = match read_frame(&mut cursor) {
                Ok(Some(payload)) => payload,
                Ok(None) | Err(_) => {
                    cursor.set_position(before as u64);
                    break;
                }
            };
            let message = decode_cast_message(&payload).expect("valid frame");
            let json =
                serde_json::from_str(&message.payload_utf8).unwrap_or(serde_json::Value::Null);
            consumed = cursor.position() as usize;
            if predicate(&message.namespace, &json) {
                // Re-inject anything that arrived after the matched frame so a
                // later poll can still see it (`wait_outgoing` already drained
                // the pipe buffer into `accumulated`).
                let trailing = accumulated[consumed..].to_vec();
                if !trailing.is_empty() {
                    pipe.push_outgoing(&trailing);
                }
                return (message.namespace, json);
            }
        }
        accumulated.drain(..consumed);
    }
}

/// Poll until `n` frames matching `predicate` have arrived (each via
/// [`expect_wire_frame`]); fails the test if any is missing before the
/// deadline.
fn expect_n_wire_frames(
    pipe: &MockPipe,
    n: usize,
    message: &str,
    mut predicate: impl FnMut(&str, &serde_json::Value) -> bool,
) {
    for _ in 0..n {
        expect_wire_frame(pipe, message, &mut predicate);
    }
}

/// The supervisor aggregates events from every task into the single GUI
/// channel and routes commands to the owning task.
#[test]
fn events_aggregate_and_commands_route() {
    let connector = MockConnector::new();
    let (backend, command_tx, mut event_rx) =
        Backend::start_with(Ok(discovery_socket().unwrap()), 0, connector.clone());
    drain_startup_events(&mut event_rx);

    let device = test_device();
    command_tx
        .send(AppCommand::SelectReceiver(device.clone()))
        .unwrap();
    assert_eq!(
        expect_event(&mut event_rx),
        BackendEvent::ReceiverConnected(device)
    );
    let pipe = connector.last_pipe().expect("pipe created on connect");
    drain_wire(&pipe, Duration::from_millis(200)); // CONNECT

    // RECEIVER_STATUS carries volume → forwarded to the GUI.
    pipe.push_incoming(&receiver_status_frame("t-9", "s-9", 0.5, false));
    assert_eq!(
        expect_event(&mut event_rx),
        BackendEvent::Volume {
            level: 0.5,
            muted: false
        }
    );

    // MEDIA_STATUS → playback-state event.
    pipe.push_incoming(&media_status_frame("PLAYING"));
    assert_eq!(
        expect_event(&mut event_rx),
        BackendEvent::MediaStatus {
            playing: true,
            buffering: false
        }
    );

    // Volume + mute route to the cast task.
    command_tx.send(AppCommand::SetVolume(0.25)).unwrap();
    command_tx.send(AppCommand::Mute(true)).unwrap();
    expect_n_wire_frames(
        &pipe,
        2,
        "SetVolume and Mute each send SET_VOLUME",
        |ns, json| ns == RECEIVER_NS && json["type"] == "SET_VOLUME",
    );

    // Rescan must not disturb routing (mDNS task stays alive).
    command_tx.send(AppCommand::Rescan).unwrap();
    command_tx.send(AppCommand::SetVolume(0.5)).unwrap();
    expect_wire_frame(&pipe, "SET_VOLUME routed after Rescan", |ns, json| {
        ns == RECEIVER_NS && json["type"] == "SET_VOLUME"
    });

    // Unresolvable monitor → StreamError (ffmpeg-missing or
    // monitor-missing: both fail deterministically). On a Wayland session
    // with the portal usable, the pipeline routes to the PipeWire portal
    // for the virtual "Screen" and does not validate the monitor name, so
    // an unknown name does not deterministically fail. The test is
    // therefore Wayland-aware: it only asserts the error on X11 or when
    // the portal path is unavailable, matching the implementation's
    // documented thread boundaries (AGENTS.md Phase 8).
    let is_wayland = cast_app::screen::capture::is_wayland_session();
    #[cfg(target_os = "linux")]
    let portal_usable = cast_app::screen::ffmpeg_discover::ffmpeg_available()
        && cast_app::screen::portal::portal_available();
    #[cfg(not(target_os = "linux"))]
    let portal_usable = false;
    if is_wayland && portal_usable {
        eprintln!("skipping unresolvable-monitor StreamError on Wayland portal");
    } else {
        command_tx
            .send(AppCommand::SelectDisplay("no-such-monitor".to_string()))
            .unwrap();
        match expect_event(&mut event_rx) {
            BackendEvent::StreamError(message) => assert!(!message.is_empty()),
            other => panic!("expected StreamError, got {other:?}"),
        }
    }

    backend.shutdown();
}

/// A fatal mDNS socket-setup failure is surfaced as a `ConnectionError`
/// (GUI Error state) and the backend keeps serving the other tasks until a
/// `Rescan` re-establishes discovery.
#[test]
fn fatal_mdns_setup_surfaces_error_and_rescan_revives() {
    let connector = MockConnector::new();
    let (backend, command_tx, mut event_rx) = Backend::start_with(
        Err(io::Error::other("test: no multicast")),
        0,
        connector.clone(),
    );

    assert_eq!(
        expect_event_matching(&mut event_rx, |event| matches!(
            event,
            BackendEvent::ConnectionError(_)
        )),
        BackendEvent::ConnectionError("mDNS discovery failed: test: no multicast".to_string())
    );

    // The Cast task is unaffected by the discovery failure.
    let device = test_device();
    command_tx
        .send(AppCommand::SelectReceiver(device.clone()))
        .unwrap();
    assert_eq!(
        expect_event_matching(&mut event_rx, |event| matches!(
            event,
            BackendEvent::ReceiverConnected(_)
        )),
        BackendEvent::ReceiverConnected(device)
    );
    let pipe = connector.last_pipe().expect("pipe created on connect");
    drain_wire(&pipe, Duration::from_millis(200));

    // Rescan retries the bind; in the test environment it succeeds, so the
    // discovery task restarts. Either way routing must keep working.
    command_tx.send(AppCommand::Rescan).unwrap();
    command_tx.send(AppCommand::SetVolume(1.0)).unwrap();
    expect_wire_frame(&pipe, "SET_VOLUME routed after Rescan", |ns, json| {
        ns == RECEIVER_NS && json["type"] == "SET_VOLUME"
    });

    backend.shutdown();
}

/// The `0.0.0.0` wildcard media-server bind is gated on explicit user
/// consent (`04-media-proxy.md` §1.1): the startup request is surfaced as
/// `BindFallbackRequested`, a decline keeps the wildcard unbound (receiver
/// selection still binds the resolved interface — the restrictive path needs
/// no consent), and a later consent re-enables Play's advertised port.
#[test]
fn wildcard_bind_consent_round_trip() {
    let connector = MockConnector::new();
    let (backend, command_tx, mut event_rx) =
        Backend::start_with(Ok(discovery_socket().unwrap()), 0, connector.clone());

    // Startup: no receiver selected yet → the app asks before binding
    // 0.0.0.0. The reason explains why and names the wildcard address.
    match expect_event_matching(&mut event_rx, |event| {
        matches!(event, BackendEvent::BindFallbackRequested(_))
    }) {
        BackendEvent::BindFallbackRequested(reason) => {
            assert!(
                reason.contains("0.0.0.0"),
                "reason names the wildcard bind: {reason}"
            );
        }
        other => panic!("expected BindFallbackRequested, got {other:?}"),
    }

    // Decline: the wildcard stays unbound, but selecting a receiver binds
    // the resolved interface — the restrictive path needs no consent.
    command_tx.send(AppCommand::BindFallback(false)).unwrap();
    let device = test_device();
    command_tx
        .send(AppCommand::SelectReceiver(device.clone()))
        .unwrap();
    match expect_event_matching(&mut event_rx, |event| {
        matches!(event, BackendEvent::ReceiverConnected(_))
    }) {
        BackendEvent::ReceiverConnected(connected) => assert_eq!(connected, device),
        other => panic!("expected ReceiverConnected, got {other:?}"),
    }
    let pipe = connector.last_pipe().expect("pipe created on connect");
    drain_wire(&pipe, Duration::from_millis(200)); // CONNECT

    command_tx
        .send(AppCommand::SelectFile(PathBuf::from(
            "/tmp/consent-test.mp4",
        )))
        .unwrap();
    command_tx.send(AppCommand::Play).unwrap();
    expect_wire_frame(
        &pipe,
        "Play launches with the interface-bound listener",
        |ns, json| ns == RECEIVER_NS && json["type"] == "LAUNCH",
    );
    pipe.push_incoming(&receiver_status_frame("t-c", "s-c", 0.5, false));
    expect_wire_frame(&pipe, "LOAD after Ready", |ns, json| {
        ns == MEDIA_NS && json["type"] == "LOAD"
    });

    // Late consent: answering yes permits the wildcard bind; a subsequent
    // Play still advertises a reachable port.
    command_tx.send(AppCommand::BindFallback(true)).unwrap();
    command_tx.send(AppCommand::Play).unwrap();
    let load = expect_wire_frame(&pipe, "LOAD after consent", |ns, json| {
        ns == MEDIA_NS && json["type"] == "LOAD"
    });
    let content_id = load.1["media"]["contentId"]
        .as_str()
        .expect("contentId is a string");
    let url = url::Url::parse(content_id).expect("contentId parses as URL");
    assert!(
        url.port().is_some(),
        "LOAD advertises a bound port after consent"
    );

    backend.shutdown();
}

/// The coordinated shutdown: HTTP listener stops accepting, the Cast session
/// closes with its final events drained to the GUI, and the GUI channel
/// closes once every task has ended — all within a bounded budget.
#[test]
fn coordinated_shutdown_order() {
    let connector = MockConnector::new();
    let (backend, command_tx, mut event_rx) =
        Backend::start_with(Ok(discovery_socket().unwrap()), 0, connector.clone());
    drain_startup_events(&mut event_rx);

    let device = test_device();
    command_tx.send(AppCommand::BindFallback(true)).unwrap();
    command_tx
        .send(AppCommand::SelectReceiver(device.clone()))
        .unwrap();
    assert_eq!(
        expect_event(&mut event_rx),
        BackendEvent::ReceiverConnected(device)
    );
    let pipe = connector.last_pipe().expect("pipe created on connect");
    drain_wire(&pipe, Duration::from_millis(200)); // CONNECT

    // Local file + Play → LAUNCH (the cast task auto-launches the default
    // receiver), then after the receiver confirms, the queued LOAD goes out
    // advertising the media server's bound port.
    command_tx
        .send(AppCommand::SelectFile(PathBuf::from(
            "/tmp/cast-app-runtime-test.mp4",
        )))
        .unwrap();
    command_tx.send(AppCommand::Play).unwrap();
    expect_wire_frame(
        &pipe,
        "Play auto-launches the default receiver",
        |ns, json| ns == RECEIVER_NS && json["type"] == "LAUNCH",
    );
    pipe.push_incoming(&receiver_status_frame("t-9", "s-9", 0.5, false));
    assert_eq!(
        expect_event(&mut event_rx),
        BackendEvent::Volume {
            level: 0.5,
            muted: false
        }
    );
    let load = expect_wire_frame(&pipe, "queued LOAD sent once Ready", |ns, json| {
        ns == MEDIA_NS && json["type"] == "LOAD"
    });
    assert_eq!(load.1["media"]["streamType"], "BUFFERED");
    assert_eq!(load.1["media"]["contentType"], "video/mp4");
    let content_id = load.1["media"]["contentId"]
        .as_str()
        .expect("contentId is a string");
    let advertised = url::Url::parse(content_id).expect("contentId parses as URL");
    let host = advertised.host_str().expect("contentId has a host");
    let port = advertised.port().expect("contentId carries the bound port");
    assert_eq!(advertised.path(), "/stream");

    // The media server is reachable at the advertised address pre-shutdown.
    let stream_addr: SocketAddr = format!("{host}:{port}").parse().expect("addr parses");
    TcpStream::connect_timeout(&stream_addr, Duration::from_secs(1))
        .expect("media server reachable before shutdown");

    // Session events keep flowing right up to shutdown.
    pipe.push_incoming(&media_status_frame("BUFFERING"));
    assert_eq!(
        expect_event(&mut event_rx),
        BackendEvent::MediaStatus {
            playing: true,
            buffering: true
        }
    );

    let shutdown_start = Instant::now();
    backend.shutdown();
    assert!(
        shutdown_start.elapsed() < Duration::from_secs(15),
        "coordinated shutdown within budget"
    );

    // 1. HTTP listener released: no more accepts on the advertised port.
    assert!(
        TcpStream::connect_timeout(&stream_addr, Duration::from_secs(1)).is_err(),
        "HTTP listener stopped accepting after shutdown"
    );

    // 2. Cast session closed: socket torn down, final Disconnected drained
    //    into the GUI channel, then the channel closes (all senders gone).
    assert!(pipe.is_closed(), "cast session closed on teardown");
    let mut saw_disconnected = false;
    let drain_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match event_rx.try_recv() {
            Ok(BackendEvent::ReceiverDisconnected(_)) => saw_disconnected = true,
            Ok(_) => {}
            Err(mpsc::error::TryRecvError::Empty) => {
                assert!(Instant::now() < drain_deadline, "channel closes in time");
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    assert!(
        saw_disconnected,
        "final ReceiverDisconnected drained during shutdown"
    );
}

/// Play with a real screen source advertises a `LIVE` stream. Runs only
/// where a monitor and `ffmpeg` exist (same skip pattern as screen_e2e).
#[test]
fn screen_play_uses_live_stream_type() {
    let Ok(names) = cast_app::screen::capture::monitor_names() else {
        eprintln!("skipping: monitor enumeration unavailable (headless/Wayland)");
        return;
    };
    if !cast_app::screen::ffmpeg_discover::ffmpeg_available() {
        eprintln!("skipping: ffmpeg not on PATH");
        return;
    }
    let Some(monitor) = names.first().cloned() else {
        eprintln!("skipping: no monitors available");
        return;
    };

    let connector = MockConnector::new();
    let (backend, command_tx, mut event_rx) =
        Backend::start_with(Ok(discovery_socket().unwrap()), 0, connector.clone());
    drain_startup_events(&mut event_rx);

    let device = test_device();
    command_tx.send(AppCommand::BindFallback(true)).unwrap();
    command_tx
        .send(AppCommand::SelectReceiver(device.clone()))
        .unwrap();
    assert_eq!(
        expect_event(&mut event_rx),
        BackendEvent::ReceiverConnected(device)
    );
    let pipe = connector.last_pipe().expect("pipe created on connect");
    drain_wire(&pipe, Duration::from_millis(200)); // CONNECT

    command_tx.send(AppCommand::SelectDisplay(monitor)).unwrap();
    command_tx.send(AppCommand::Play).unwrap();
    let wire = drain_wire(&pipe, Duration::from_millis(500));
    assert!(
        wire.iter()
            .any(|(ns, json)| ns == RECEIVER_NS && json["type"] == "LAUNCH"),
        "Play auto-launches the default receiver"
    );
    pipe.push_incoming(&receiver_status_frame("t-9", "s-9", 0.5, false));
    match expect_event(&mut event_rx) {
        BackendEvent::Volume { .. } => {}
        other => panic!("expected Volume from status, got {other:?}"),
    }
    let wire = drain_wire(&pipe, Duration::from_millis(500));
    let load = wire
        .iter()
        .find(|(ns, json)| ns == MEDIA_NS && json["type"] == "LOAD")
        .expect("queued LOAD sent once Ready");
    assert_eq!(load.1["media"]["streamType"], "LIVE");
    assert_eq!(load.1["media"]["contentType"], "video/mp4");

    backend.shutdown();
}
