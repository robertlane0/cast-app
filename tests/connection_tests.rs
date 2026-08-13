#![forbid(unsafe_code)]

//! End-to-end connection lifecycle tests against the mock transport
//! (`03-cast-engine.md` §7). The mock plumbing lives in
//! `cast::connection::test_support` and is compiled into the library under
//! `cfg(test)`.

use cast_app::cast::connection::test_support::{MockConnector, MockPipe};
use cast_app::cast::connection::{
    Command, ConnectionConfig, ConnectionError, ConnectionEvent, run,
};
use cast_app::cast::framing::{encode_frame, read_frame};
use cast_app::cast::namespaces::{MEDIA_NS, RECEIVER_ID, RECEIVER_NS, SOURCE_ID, TRANSPORT_ID};
use cast_app::cast::proto::{decode_cast_message, encode_cast_message};
use cast_app::state::CastDevice;
use cast_app::util::retry::Backoff;
use cast_app::util::shutdown::Shutdown;
use std::time::Duration;
use tokio::sync::mpsc;

fn test_device() -> CastDevice {
    CastDevice {
        id: "living-room".to_string(),
        name: "Living Room".to_string(),
        addr: "10.0.0.5:8009".parse().expect("valid test address"),
    }
}

/// Heartbeat 50ms, watchdog 150ms, backoff 20ms x3 — fast enough for
/// watchdog/reconnect tests to finish in real time.
fn fast_config() -> ConnectionConfig {
    ConnectionConfig {
        heartbeat_interval: Duration::from_millis(50),
        watchdog_timeout: Duration::from_millis(150),
        request_timeout: Duration::from_secs(5),
        backoff: Backoff::with_params(Duration::from_millis(20), Duration::from_millis(20), 3),
    }
}

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

fn pong_frame() -> Vec<u8> {
    cast_frame(
        TRANSPORT_ID,
        SOURCE_ID,
        "urn:x-cast:com.google.cast.tp.heartbeat",
        r#"{"type":"PONG"}"#,
    )
}

async fn expect_event(rx: &mut mpsc::UnboundedReceiver<ConnectionEvent>) -> ConnectionEvent {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("event within timeout")
        .expect("sender still alive")
}

/// Collect everything written to `pipe` within `window` and parse it into
/// messages (namespace + JSON type).
async fn drain_wire(pipe: &MockPipe, window: Duration) -> Vec<String> {
    let mut accumulated = Vec::new();
    let mut deadline = std::time::Instant::now() + window;
    loop {
        let chunk =
            pipe.wait_outgoing(deadline.saturating_duration_since(std::time::Instant::now()));
        if chunk.is_empty() {
            break;
        }
        accumulated.extend_from_slice(&chunk);
        deadline = std::time::Instant::now() + window;
    }
    let mut out = Vec::new();
    let mut cursor = std::io::Cursor::new(&accumulated);
    while let Ok(Some(payload)) = read_frame(&mut cursor) {
        let message = decode_cast_message(&payload).expect("valid frame");
        let msg_type = serde_json::from_str::<serde_json::Value>(&message.payload_utf8)
            .ok()
            .and_then(|v| v.get("type").cloned())
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_else(|| "<non-json>".to_string());
        out.push(format!("{}:{}", message.namespace, msg_type));
    }
    out
}

/// The heartbeat watchdog (no PONGs) must tear the session down and drive
/// reconnect attempts until the backoff is exhausted, surfacing a
/// `ReconnectExhausted` error to the GUI.
#[tokio::test(flavor = "multi_thread")]
async fn watchdog_fires_and_reconnect_exhausts() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let shutdown = Shutdown::new();
    let connector = MockConnector::new();
    let task = tokio::spawn(run(
        commands_rx,
        events_tx,
        shutdown,
        connector.clone(),
        fast_config(),
    ));

    commands_tx.send(Command::Select(test_device())).unwrap();

    match expect_event(&mut events_rx).await {
        ConnectionEvent::Connected(device) => assert_eq!(device.id, "living-room"),
        other => panic!("expected Connected, got {other:?}"),
    }
    // From here on every reconnect attempt must fail: 3 attempts then
    // exhaustion. (Armed after the initial connect.)
    connector.fail_next(3);
    let first_pipe = connector.last_pipe().expect("pipe created on connect");

    // No PONGs: watchdog fires after 150ms, then 3 reconnect attempts fail.
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Disconnected(device) => assert_eq!(device.id, "living-room"),
        other => panic!("expected Disconnected, got {other:?}"),
    }
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Error(ConnectionError::ReconnectExhausted { attempts, .. }) => {
            assert_eq!(attempts, 3);
        }
        other => panic!("expected ReconnectExhausted, got {other:?}"),
    }
    assert!(
        first_pipe.is_closed(),
        "session socket closed during teardown"
    );

    commands_tx.send(Command::Shutdown).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("task exits after shutdown")
        .expect("task did not panic");
}

/// A lost connection reconnects through the backoff once the receiver is
/// reachable again; the new session speaks on a fresh pipe.
#[tokio::test(flavor = "multi_thread")]
async fn reconnect_succeeds_after_transient_failures() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let shutdown = Shutdown::new();
    let connector = MockConnector::new();
    let task = tokio::spawn(run(
        commands_rx,
        events_tx,
        shutdown,
        connector.clone(),
        fast_config(),
    ));

    commands_tx.send(Command::Select(test_device())).unwrap();
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Connected(_) => {}
        other => panic!("expected Connected, got {other:?}"),
    }
    connector.fail_next(2); // armed after the initial connect
    let first_pipe = connector.last_pipe().expect("pipe created on connect");
    first_pipe.close(); // simulate the receiver dropping the connection

    // Two failed reconnect attempts, then success on a new pipe.
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Disconnected(_) => {}
        other => panic!("expected Disconnected, got {other:?}"),
    }
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Connected(_) => {}
        other => panic!("expected second Connected, got {other:?}"),
    }
    assert_eq!(
        connector.pipes().len(),
        2,
        "one initial + one successful reconnect"
    );
    let second_pipe = connector.last_pipe().expect("second pipe exists");
    assert!(!second_pipe.is_closed(), "new session socket is live");
    let wire = drain_wire(&second_pipe, Duration::from_millis(300)).await;
    assert_eq!(
        wire.first().map(String::as_str),
        Some("urn:x-cast:com.google.cast.tp.connection:CONNECT"),
        "new session starts with CONNECT"
    );

    commands_tx.send(Command::Shutdown).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("task exits after shutdown")
        .expect("task did not panic");
}

/// Teardown ordering with a live media session: media `STOP` → receiver
/// `STOP_APP` → `close_notify`/socket close → `Disconnected`.
#[tokio::test(flavor = "multi_thread")]
async fn teardown_ordering_with_active_session() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let shutdown = Shutdown::new();
    let connector = MockConnector::new();
    let task = tokio::spawn(run(
        commands_rx,
        events_tx,
        shutdown,
        connector.clone(),
        quiet_config(),
    ));

    let device = test_device();
    commands_tx.send(Command::Select(device.clone())).unwrap();
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Connected(_) => {}
        other => panic!("expected Connected, got {other:?}"),
    }
    let pipe = connector.last_pipe().expect("pipe created on connect");
    drain_wire(&pipe, Duration::from_millis(200)).await; // CONNECT

    commands_tx.send(Command::LaunchDefaultReceiver).unwrap();
    drain_wire(&pipe, Duration::from_millis(200)).await; // LAUNCH

    pipe.push_incoming(&receiver_status_frame("t-9", "s-9", 0.5, false));
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Ready { .. } => {}
        other => panic!("expected Ready, got {other:?}"),
    }
    // The status also carried a volume: drain it before the teardown assert.
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Volume { .. } => {}
        other => panic!("expected Volume from status, got {other:?}"),
    }

    commands_tx
        .send(Command::Load {
            content_id: "http://10.0.0.5:8080/stream".to_string(),
            content_type: "video/mp4".to_string(),
            stream_type: cast_app::cast::namespaces::StreamType::Buffered,
        })
        .unwrap();
    drain_wire(&pipe, Duration::from_millis(200)).await; // LOAD

    commands_tx.send(Command::Shutdown).unwrap();
    let wire = drain_wire(&pipe, Duration::from_millis(500)).await;
    let types: Vec<&str> = wire.iter().map(|s| s.as_str()).collect();
    assert_eq!(
        types,
        vec![
            "urn:x-cast:com.google.cast.media:STOP",
            "urn:x-cast:com.google.cast.receiver:STOP_APP",
        ],
        "teardown sends media STOP before receiver STOP_APP"
    );
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Disconnected(_) => {}
        other => panic!("expected Disconnected, got {other:?}"),
    }
    assert!(pipe.is_closed(), "socket closed after teardown");

    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("task exits after shutdown")
        .expect("task did not panic");
}

/// Commands sent while disconnected are ignored — no writes, no panics —
/// and the connection stays available for a later `Select`.
#[tokio::test(flavor = "multi_thread")]
async fn commands_ignored_while_disconnected() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let shutdown = Shutdown::new();
    let connector = MockConnector::new();
    let task = tokio::spawn(run(
        commands_rx,
        events_tx,
        shutdown,
        connector.clone(),
        quiet_config(),
    ));

    commands_tx.send(Command::Play).unwrap();
    commands_tx.send(Command::Pause).unwrap();
    commands_tx.send(Command::Stop).unwrap();
    commands_tx
        .send(Command::SetVolume {
            level: 0.5,
            muted: false,
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        connector.pipes().is_empty(),
        "no connection attempted without Select"
    );

    commands_tx.send(Command::Select(test_device())).unwrap();
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Connected(_) => {}
        other => panic!("expected Connected, got {other:?}"),
    }
    let pipe = connector.last_pipe().expect("pipe created on connect");
    assert!(
        drain_wire(&pipe, Duration::from_millis(300)).await.len() == 1,
        "only CONNECT on the wire after re-select"
    );

    commands_tx.send(Command::Shutdown).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("task exits after shutdown")
        .expect("task did not panic");
}

/// `SET_VOLUME` goes to the receiver and the reply is surfaced as a
/// `Volume` event (GUI slider correction path).
#[tokio::test(flavor = "multi_thread")]
async fn volume_command_and_event_roundtrip() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let shutdown = Shutdown::new();
    let connector = MockConnector::new();
    let task = tokio::spawn(run(
        commands_rx,
        events_tx,
        shutdown,
        connector.clone(),
        quiet_config(),
    ));

    commands_tx.send(Command::Select(test_device())).unwrap();
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Connected(_) => {}
        other => panic!("expected Connected, got {other:?}"),
    }
    let pipe = connector.last_pipe().expect("pipe created on connect");
    drain_wire(&pipe, Duration::from_millis(200)).await;

    commands_tx.send(Command::LaunchDefaultReceiver).unwrap();
    drain_wire(&pipe, Duration::from_millis(200)).await;
    pipe.push_incoming(&receiver_status_frame("t-1", "s-1", 0.3, false));
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Ready { .. } => {}
        other => panic!("expected Ready, got {other:?}"),
    }
    // The status also carried the initial volume (0.3): drain it.
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Volume { level, .. } => assert!((level - 0.3).abs() < 0.001),
        other => panic!("expected Volume from status, got {other:?}"),
    }

    commands_tx
        .send(Command::SetVolume {
            level: 0.8,
            muted: false,
        })
        .unwrap();
    let wire = drain_wire(&pipe, Duration::from_millis(300)).await;
    assert_eq!(
        wire.first().map(String::as_str),
        Some("urn:x-cast:com.google.cast.receiver:SET_VOLUME"),
        "SET_VOLUME sent to the receiver namespace"
    );

    pipe.push_incoming(&receiver_status_frame("t-1", "s-1", 0.8, false));
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Volume { level, muted } => {
            assert!((level - 0.8).abs() < 0.001);
            assert!(!muted);
        }
        other => panic!("expected Volume event, got {other:?}"),
    }

    commands_tx.send(Command::Shutdown).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("task exits after shutdown")
        .expect("task did not panic");
}

/// PONGs keep the session alive past the watchdog window; MEDIA_STATUS
/// flows through as `MediaStatus` events.
#[tokio::test(flavor = "multi_thread")]
async fn pong_keepalive_and_media_status_events() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let shutdown = Shutdown::new();
    let connector = MockConnector::new();
    let task = tokio::spawn(run(
        commands_rx,
        events_tx,
        shutdown,
        connector.clone(),
        fast_config(),
    ));

    commands_tx.send(Command::Select(test_device())).unwrap();
    match expect_event(&mut events_rx).await {
        ConnectionEvent::Connected(_) => {}
        other => panic!("expected Connected, got {other:?}"),
    }
    let pipe = connector.last_pipe().expect("pipe created on connect");

    // Keep PONGs flowing for the whole assertion window: the reader thread
    // polls the mock transport at ~100 ms intervals, so a bounded burst of
    // PONGs leaves the watchdog deadline dangerously close to the end of the
    // no-disconnect window under load. A continuous 30 ms cadence guarantees
    // every poll finds a fresh PONG, so the watchdog is always reset well
    // inside its 150 ms window.
    let keepalive = tokio::spawn({
        let pipe = pipe.clone();
        async move {
            loop {
                pipe.push_incoming(&pong_frame());
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        }
    });
    tokio::time::timeout(Duration::from_millis(300), events_rx.recv())
        .await
        .expect_err("no disconnect while PONGs keep arriving");

    // The MEDIA_STATUS frame may take one reader poll (~100 ms) to arrive,
    // so keep PONGs flowing until the event is observed — stopping the
    // keepalive first would let the 150 ms watchdog fire and race the
    // assertion with a Disconnected event.
    pipe.push_incoming(&media_status_frame("PLAYING"));
    match expect_event(&mut events_rx).await {
        ConnectionEvent::MediaStatus {
            playing: true,
            buffering: false,
        } => {}
        other => panic!("expected playing MediaStatus, got {other:?}"),
    }
    keepalive.abort();

    commands_tx.send(Command::Shutdown).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("task exits after shutdown")
        .expect("task did not panic");
}
