#![forbid(unsafe_code)]

//! Screen-pipeline integration tests (`05-screen-capture.md` §4–§6) driven
//! through the real `ScreenBridge` with fake encoder scripts: EOF-honored
//! teardown, resolution-change restart, unexpected-exit error surfacing,
//! drop-oldest backpressure, and client-disconnect teardown.
//! Gate: `cargo test --test screen_pipeline_tests`.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use cast_app::media::server::MediaServer;
use cast_app::media::source::ActiveSource;
use cast_app::screen::bridge::ScreenBridge;
use cast_app::state::BackendEvent;
use cast_app::util::shutdown::Shutdown;
use futures_util::StreamExt;
use tokio::sync::mpsc;

const WAIT: Duration = Duration::from_secs(10);

fn sh() -> PathBuf {
    PathBuf::from("sh")
}

fn scratch(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "cast-app-screen-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// A fake encoder that consumes stdin until EOF and touches `marker`.
fn cat_script(marker: &Path) -> String {
    format!("cat >/dev/null; touch '{}'", marker.display())
}

/// A fake encoder that appends a "started" line to `log` on every launch.
fn restart_script(log: &Path) -> String {
    format!("echo started >> '{}'; cat >/dev/null", log.display())
}

fn read_log(log: &PathBuf) -> Vec<String> {
    std::fs::read_to_string(log)
        .map(|content| content.lines().map(str::to_string).collect())
        .unwrap_or_default()
}

async fn start_server() -> (MediaServer, Shutdown, u16) {
    let shutdown = Shutdown::new();
    let server = MediaServer::start(shutdown.clone(), 0);
    let mut rx = server.subscribe_port();
    if *rx.borrow() == 0 {
        let deadline = Instant::now() + WAIT;
        while *rx.borrow() == 0 && rx.changed().await.is_ok() && Instant::now() < deadline {}
    }
    let port = *rx.borrow();
    assert_ne!(port, 0, "server must bind an ephemeral port");
    (server, shutdown, port)
}

async fn wait_for_generation(server: &MediaServer, at_least: u64) {
    let mut rx = server.subscribe_generation();
    let deadline = Instant::now() + WAIT;
    while *rx.borrow() < at_least {
        if Instant::now() >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
            break;
        }
    }
    assert!(*rx.borrow() >= at_least, "server must process the attach");
}

fn wait_until(what: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !predicate() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(predicate(), "{what} never became true");
}

fn drain_events(events: &mut mpsc::UnboundedReceiver<BackendEvent>) -> Vec<BackendEvent> {
    let mut out = Vec::new();
    while let Ok(event) = events.try_recv() {
        out.push(event);
    }
    out
}

/// EOF is honored before the fake encoder exits; no error is surfaced.
#[tokio::test(flavor = "multi_thread")]
async fn stop_sends_eof_and_waits_for_graceful_exit() {
    let (server, shutdown, _port) = start_server().await;
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let marker = scratch("eof");
    let _ = std::fs::remove_file(&marker);
    let script = cat_script(&marker);

    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (8, 8),
        Some((sh(), vec!["-c".into(), script])),
    )
    .expect("bridge must start");
    wait_for_generation(&server, 1).await;

    for _ in 0..10 {
        bridge.push_frame(vec![0u8; 16 * 1024]);
    }
    thread::sleep(Duration::from_millis(100));

    let error = bridge.last_error();
    bridge.stop();
    bridge.join();

    assert!(
        marker.exists(),
        "fake encoder must observe stdin EOF before exit"
    );
    assert_eq!(error, None, "clean teardown must not report an error");
    assert!(
        drain_events(&mut events_rx).is_empty(),
        "no backend events expected on clean teardown"
    );
    let _ = std::fs::remove_file(&marker);
}

/// A resolution change restarts the encoder subprocess (spec §3.2).
#[tokio::test(flavor = "multi_thread")]
async fn resolution_change_restarts_the_encoder() {
    let (server, shutdown, _port) = start_server().await;
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let log = scratch("restart");
    let _ = std::fs::remove_file(&log);
    let script = restart_script(&log);

    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (8, 8),
        Some((sh(), vec!["-c".into(), script])),
    )
    .expect("bridge must start");
    wait_for_generation(&server, 1).await;
    wait_until("first encoder start", || read_log(&log).len() == 1);

    bridge.request_resolution(16, 16);
    wait_until("encoder restart", || read_log(&log).len() == 2);

    let error = bridge.last_error();
    bridge.stop();
    bridge.join();

    assert_eq!(error, None, "restart must not be an error");
    assert!(drain_events(&mut events_rx).is_empty());
    let _ = std::fs::remove_file(&log);
}

/// A non-zero encoder exit stops the pipeline and surfaces `StreamError`.
#[tokio::test(flavor = "multi_thread")]
async fn unexpected_encoder_exit_stops_pipeline_and_reports() {
    let (server, shutdown, _port) = start_server().await;
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();

    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (8, 8),
        Some((sh(), vec!["-c".into(), "exit 3".into()])),
    )
    .expect("bridge must start");

    wait_until("pipeline fails", || !bridge.is_running());
    let error = bridge.last_error().expect("an error must be recorded");
    assert!(
        error.contains("exited unexpectedly"),
        "unexpected error: {error}"
    );
    let events = drain_events(&mut events_rx);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, BackendEvent::StreamError(_))),
        "a StreamError must be emitted: {events:?}"
    );

    bridge.stop();
    bridge.join();
}

/// The cap-2 frame queue drops the oldest frame under overload rather than
/// blocking the producer (spec §5; AGENTS.md §7).
#[tokio::test(flavor = "multi_thread")]
async fn frame_queue_drops_oldest_when_encoder_is_slow() {
    let (server, shutdown, _port) = start_server().await;
    let (events_tx, _events_rx) = mpsc::unbounded_channel();

    // `sleep 1` never reads stdin: the 64 KiB pipe fills after 4 frames and
    // the controller blocks, so the queue must evict the oldest frames and
    // keep only the newest 2 (spec §6 drop-oldest backpressure).
    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (8, 8),
        Some((sh(), vec!["-c".into(), "sleep 1".into()])),
    )
    .expect("bridge must start");

    // Push at a pace that keeps the queue full while the controller blocks
    // on the full pipe (a single instant burst would be absorbed entirely
    // by producer-side eviction).
    for _ in 0..50 {
        bridge.push_frame(vec![0u8; 16 * 1024]);
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        bridge.frames_len(),
        2,
        "the queue must keep only the newest 2 frames"
    );
    // The sleep exits after 1 s, the blocked write unblocks and the pipeline
    // finishes its teardown; join must return.
    bridge.stop();
    bridge.join();
}

/// A closed HTTP consumer (client disconnect / source switch) tears down the
/// encoder session (spec §4.2).
#[tokio::test(flavor = "multi_thread")]
async fn closed_http_consumer_tears_down_the_session() {
    let (server, shutdown, port) = start_server().await;
    let (events_tx, _events_rx) = mpsc::unbounded_channel();

    // Fake encoder: emits output forever (so the only way the HTTP stream
    // ends is the client's dropped socket) while draining stdin. The
    // background emitter is killed when the stdin drainer hits EOF, so no
    // orphan process keeps the output pipes open after teardown.
    let script = "cat >/dev/null & CPID=$!; while :; do head -c 65536 /dev/zero; done & EPID=$!; wait $CPID; kill $EPID".to_string();
    // The media server only serves /stream for an active Screen source
    // (same ordering the Phase 10 runtime uses).
    server.set_source(ActiveSource::Screen("test-monitor".to_string()));
    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (8, 8),
        Some((sh(), vec!["-c".into(), script])),
    )
    .expect("bridge must start");
    wait_for_generation(&server, 2).await;

    // Connect, read one chunk, and drop the connection.
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/stream");
    let response = tokio::time::timeout(WAIT, client.get(&url).send())
        .await
        .expect("GET /stream timed out")
        .expect("GET /stream must succeed");
    assert_eq!(response.status(), 200);
    let mut stream = response.bytes_stream();
    let chunk = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("first stream chunk timed out")
        .expect("stream must yield a chunk")
        .expect("stream chunk must not error");
    assert!(!chunk.is_empty());
    drop(stream);

    // The server's write to the dropped socket fails, the live channel
    // closes, and the bridge tears the encoder down.
    wait_until("pipeline stops after client disconnect", || {
        !bridge.is_running()
    });

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let joiner = thread::spawn(move || {
        bridge.join();
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("bridge join must complete within the grace period");
    let _ = joiner.join();
}
