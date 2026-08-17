// SPDX-License-Identifier: MIT OR Apache-2.0
#![forbid(unsafe_code)]

//! Screen-pipeline integration tests (`05-screen-capture.md` §4–§6) driven
//! through the real `ScreenBridge` with a compiled fake-encoder binary
//! (`tests/support/fake_encoder.rs`, located via `CARGO_BIN_EXE_fake-encoder`):
//! EOF-honored teardown, resolution-change restart, unexpected-exit error
//! surfacing, drop-oldest backpressure, and client-disconnect teardown. The
//! binary replaces the former `/bin/sh` fake-encoder scripts so the tests run
//! on Windows as well as Unix (ISS-012).
//! Gate: `cargo test --test screen_pipeline_tests`.

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

fn fake_encoder() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_fake-encoder"))
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

/// `fake-encoder cat MARKER`: consumes stdin until EOF, then creates the
/// marker file (the test asserts the encoder observed stdin EOF before exit).
fn cat_args(marker: &Path) -> Vec<String> {
    vec!["cat".into(), marker.display().to_string()]
}

/// `fake-encoder restart LOG`: appends a "started" line to `log` on every
/// launch, then consumes stdin until EOF.
fn restart_args(log: &Path) -> Vec<String> {
    vec!["restart".into(), log.display().to_string()]
}

fn read_log(log: &PathBuf) -> Vec<String> {
    std::fs::read_to_string(log)
        .map(|content| content.lines().map(str::to_string).collect())
        .unwrap_or_default()
}

/// `started <pid>` lines recorded by the `fmp4` fake-encoder mode, one per
/// encoder generation.
fn pids_in_log(log: &Path) -> Vec<u32> {
    read_log(&log.to_path_buf())
        .iter()
        .filter_map(|line| {
            let (_, pid) = line.split_once(' ')?;
            pid.parse().ok()
        })
        .collect()
}

/// Walk the ISO-BMFF box structure of `bytes`: every box header must be
/// fully present and its declared size satisfied. A single truncated box is
/// allowed as the stream tail (the test stops reading at an arbitrary
/// point, so the final box may be cut by the test, never by the bridge).
/// Returns the complete boxes as `(offset, fourcc)` pairs plus the offset of
/// the truncated tail (== `bytes.len()` when nothing was cut).
fn walk_boxes(bytes: &[u8]) -> (Vec<(usize, [u8; 4])>, usize) {
    let mut offset = 0;
    let mut boxes = Vec::new();
    while offset + 8 <= bytes.len() {
        let size = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        let kind: [u8; 4] = bytes[offset + 4..offset + 8].try_into().unwrap();
        assert!(size >= 8, "corrupt box header at {offset}: size {size}");
        if offset + size > bytes.len() {
            break;
        }
        boxes.push((offset, kind));
        offset += size;
    }
    (boxes, offset)
}

/// Assert that `bytes` is a structurally sound fMP4 stream: it begins with
/// the `ftyp` box, every box is whole (except the test-truncated tail), and
/// every `moof` is immediately followed by its `mdat` (whole fragments).
/// The final `moof` may lack its `mdat` only when the stream was truncated
/// mid-fragment by the test itself (the truncated tail then begins with the
/// `mdat` header).
fn assert_whole_fmp4_stream(bytes: &[u8]) -> Vec<(usize, [u8; 4])> {
    assert!(bytes.len() >= 8, "stream too short: {} bytes", bytes.len());
    assert_eq!(
        &bytes[4..8],
        b"ftyp",
        "stream must begin with the ftyp box, got {:?}",
        &bytes[..8]
    );
    let (boxes, tail_offset) = walk_boxes(bytes);
    assert!(!boxes.is_empty(), "stream must contain at least one box");
    for (index, (_, kind)) in boxes.iter().enumerate() {
        if kind == b"moof" {
            if let Some(next) = boxes.get(index + 1) {
                assert_eq!(
                    &next.1, b"mdat",
                    "a moof must be followed immediately by its mdat"
                );
            } else {
                assert!(
                    tail_offset < bytes.len(),
                    "moof box without its mdat at index {index}"
                );
                let tail = &bytes[tail_offset..];
                if tail.len() >= 8 {
                    assert_eq!(
                        &tail[4..8],
                        b"mdat",
                        "the moof at index {index} is followed by a non-mdat tail"
                    );
                }
            }
        }
    }
    boxes
}

/// Read chunks from a live-stream response for `window`, appending them to
/// `received`.
async fn read_window<S>(stream: &mut S, received: &mut Vec<u8>, window: Duration)
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin,
{
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(Some(Ok(chunk))) => received.extend_from_slice(&chunk),
            Ok(Some(Err(error))) => panic!("stream error: {error}"),
            Ok(None) | Err(_) => break,
        }
    }
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

    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (8, 8),
        Some((fake_encoder(), cat_args(&marker))),
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

    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (8, 8),
        Some((fake_encoder(), restart_args(&log))),
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
        Some((fake_encoder(), vec!["exit".into(), "3".into()])),
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
        Some((fake_encoder(), vec!["sleep".into(), "1".into()])),
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

    // `stream` mode: drains stdin (holding it open) while emitting 64 KiB
    // chunks on stdout forever, so the only way the HTTP stream ends is the
    // client's dropped socket. Exiting on stdin EOF kills the emitter thread,
    // so no orphan process keeps the output pipes open after teardown.
    // The media server only serves /stream for an active Screen source
    // (same ordering the Phase 10 runtime uses).
    server.set_source(ActiveSource::Screen("test-monitor".to_string()));
    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (8, 8),
        Some((fake_encoder(), vec!["stream".into()])),
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

/// A sustained slow consumer causes only whole-segment loss: the bridge
/// drops complete `moof`+`mdat` fragments, never arbitrary byte slices, so
/// the stream the consumer receives remains a structurally sound fMP4
/// stream after overflow.
#[tokio::test(flavor = "multi_thread")]
async fn slow_consumer_receives_only_whole_fmp4_segments() {
    let (server, shutdown, port) = start_server().await;
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    server.set_source(ActiveSource::Screen("test-monitor".to_string()));
    let log = scratch("fmp4-slow");
    let _ = std::fs::remove_file(&log);

    // `fmp4` emits a structured fMP4 stream (init + moof/mdat fragments).
    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (8, 8),
        Some((
            fake_encoder(),
            vec!["fmp4".into(), log.display().to_string()],
        )),
    )
    .expect("bridge must start");
    wait_for_generation(&server, 2).await;

    // Read slowly (one chunk per 200 ms): the encoder outpaces the consumer,
    // the bridge's queues overflow, and drop-oldest must discard whole
    // segments.
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/stream");
    let response = tokio::time::timeout(WAIT, client.get(&url).send())
        .await
        .expect("GET /stream timed out")
        .expect("GET /stream must succeed");
    assert_eq!(response.status(), 200);
    let mut stream = response.bytes_stream();
    let mut received: Vec<u8> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(6);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(Some(Ok(chunk))) => received.extend_from_slice(&chunk),
            Ok(Some(Err(error))) => panic!("stream error: {error}"),
            Ok(None) | Err(_) => break,
        }
        thread::sleep(Duration::from_millis(200));
    }

    assert!(
        bridge.dropped_segments() > 0,
        "the slow consumer must have caused segment drops"
    );
    // The consumer reads one ~8 KiB chunk per 200 ms sleep, so it physically
    // receives only a fraction of what the encoder emits; assert that a few
    // whole segments flowed (not a single fragment's worth).
    assert!(
        received.len() > 64 * 1024,
        "expected a substantial stream, got {} bytes",
        received.len()
    );

    // The received stream is still a valid fMP4 structure: whole boxes only,
    // every moof immediately followed by its mdat, nothing truncated except
    // (possibly) the tail cut by the test stopping.
    assert_whole_fmp4_stream(&received);

    drop(stream);
    let error = bridge.last_error();
    bridge.stop();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let joiner = thread::spawn(move || {
        bridge.join();
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("bridge join must complete within the grace period");
    let _ = joiner.join();
    assert_eq!(error, None, "no pipeline error expected: {error:?}");
    let _ = std::fs::remove_file(&log);
}

/// An encoder restart (resolution change) produces a fresh, valid
/// initialization segment: the old generation's buffered bytes are cleared
/// and the new init leads the restarted stream, with no stale generation-1
/// bytes after it.
#[tokio::test(flavor = "multi_thread")]
async fn encoder_restart_emits_a_fresh_valid_init_segment() {
    let (server, shutdown, port) = start_server().await;
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    server.set_source(ActiveSource::Screen("test-monitor".to_string()));
    let log = scratch("fmp4-restart");
    let _ = std::fs::remove_file(&log);

    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (8, 8),
        Some((
            fake_encoder(),
            vec!["fmp4".into(), log.display().to_string()],
        )),
    )
    .expect("bridge must start");
    wait_for_generation(&server, 2).await;
    wait_until("first encoder start", || read_log(&log).len() == 1);

    // Fast consumer: read generation 1 for a moment.
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/stream");
    let response = tokio::time::timeout(WAIT, client.get(&url).send())
        .await
        .expect("GET /stream timed out")
        .expect("GET /stream must succeed");
    assert_eq!(response.status(), 200);
    let mut stream = response.bytes_stream();
    let mut received: Vec<u8> = Vec::new();
    read_window(&mut stream, &mut received, Duration::from_secs(1)).await;
    assert!(!received.is_empty(), "generation 1 must deliver bytes");

    // Restart the encoder mid-stream (resolution change).
    bridge.request_resolution(16, 16);
    wait_until("encoder restart", || read_log(&log).len() == 2);
    read_window(&mut stream, &mut received, Duration::from_secs(2)).await;

    let pids = pids_in_log(&log);
    assert_eq!(
        pids.len(),
        2,
        "two encoder generations must be logged: {pids:?}"
    );
    let pattern1 = pids[0].to_le_bytes();
    let pattern2 = pids[1].to_le_bytes();
    assert_ne!(pattern1, pattern2, "generations must be distinguishable");

    let boxes = assert_whole_fmp4_stream(&received);
    let ftyp_count = boxes.iter().filter(|(_, kind)| *kind == *b"ftyp").count();
    assert_eq!(
        ftyp_count, 2,
        "each encoder generation must lead with its own init segment"
    );
    let second_init = boxes
        .iter()
        .rev()
        .find(|(_, kind)| *kind == *b"ftyp")
        .map(|(offset, _)| *offset)
        .unwrap_or(0);
    let after_second_init = &received[second_init..];
    assert!(
        !after_second_init
            .windows(4)
            .any(|window| window == pattern1),
        "stale generation-1 bytes must not follow the new init segment"
    );
    assert!(
        after_second_init
            .windows(4)
            .any(|window| window == pattern2),
        "the new init segment must carry generation 2's pattern"
    );

    drop(stream);
    let error = bridge.last_error();
    bridge.stop();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let joiner = thread::spawn(move || {
        bridge.join();
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("bridge join must complete within the grace period");
    let _ = joiner.join();
    assert_eq!(error, None, "no pipeline error expected: {error:?}");
    let _ = std::fs::remove_file(&log);
}
