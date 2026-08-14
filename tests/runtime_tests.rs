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
    let wire = drain_wire(&pipe, Duration::from_millis(300));
    let set_volumes = wire
        .iter()
        .filter(|(ns, json)| ns == RECEIVER_NS && json["type"] == "SET_VOLUME")
        .count();
    assert_eq!(set_volumes, 2, "SetVolume and Mute each send SET_VOLUME");

    // Rescan must not disturb routing (mDNS task stays alive).
    command_tx.send(AppCommand::Rescan).unwrap();
    command_tx.send(AppCommand::SetVolume(0.5)).unwrap();
    let wire = drain_wire(&pipe, Duration::from_millis(300));
    assert!(
        wire.iter()
            .any(|(ns, json)| ns == RECEIVER_NS && json["type"] == "SET_VOLUME"),
        "SET_VOLUME routed after Rescan"
    );

    // Unresolvable monitor → StreamError (ffmpeg-missing or
    // monitor-missing: both fail deterministically).
    command_tx
        .send(AppCommand::SelectDisplay("no-such-monitor".to_string()))
        .unwrap();
    match expect_event(&mut event_rx) {
        BackendEvent::StreamError(message) => assert!(!message.is_empty()),
        other => panic!("expected StreamError, got {other:?}"),
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
    let wire = drain_wire(&pipe, Duration::from_millis(300));
    assert!(
        wire.iter()
            .any(|(ns, json)| ns == RECEIVER_NS && json["type"] == "SET_VOLUME"),
        "SET_VOLUME routed after Rescan"
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
    let wire = drain_wire(&pipe, Duration::from_millis(300));
    assert!(
        wire.iter()
            .any(|(ns, json)| ns == RECEIVER_NS && json["type"] == "LAUNCH"),
        "Play auto-launches the default receiver"
    );
    pipe.push_incoming(&receiver_status_frame("t-9", "s-9", 0.5, false));
    assert_eq!(
        expect_event(&mut event_rx),
        BackendEvent::Volume {
            level: 0.5,
            muted: false
        }
    );
    let wire = drain_wire(&pipe, Duration::from_millis(300));
    let load = wire
        .iter()
        .find(|(ns, json)| ns == MEDIA_NS && json["type"] == "LOAD")
        .expect("queued LOAD sent once Ready");
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
