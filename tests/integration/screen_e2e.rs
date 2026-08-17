// SPDX-License-Identifier: MIT OR Apache-2.0
#![forbid(unsafe_code)]

//! Screen pipeline end-to-end test (`05-screen-capture.md` §6): a dummy
//! rawvideo feeder drives the real `ffmpeg` encoder through the bridge, and
//! the encoded fMP4 stream is consumed over the local HTTP server. Skipped
//! when `ffmpeg` is absent from PATH.
//! Gate: `cargo test --test screen_e2e`.

#![cfg(unix)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use cast_app::media::server::MediaServer;
use cast_app::media::source::ActiveSource;
use cast_app::screen::bridge::{FrameFeeder, ScreenBridge};
use cast_app::state::BackendEvent;
use cast_app::util::shutdown::Shutdown;
use futures_util::StreamExt;
use tokio::sync::mpsc;

const WAIT: Duration = Duration::from_secs(30);
const WIDTH: u32 = 320;
const HEIGHT: u32 = 240;

fn gradient_frame(index: u32) -> Vec<u8> {
    let mut frame = vec![0u8; (WIDTH * HEIGHT * 4) as usize];
    for (pixel, bytes) in frame.chunks_exact_mut(4).enumerate() {
        bytes[0] = (pixel as u8).wrapping_add(index as u8);
        bytes[1] = 0x40;
        bytes[2] = 0x80;
        bytes[3] = 0xff;
    }
    frame
}

/// The dummy rawvideo producer: pushes frames at ~30 fps (spec §3: fixed
/// rate) until `stop_feed` is set, so the encoder sees an endless live input
/// stream exactly like the real capture thread.
fn feeder_thread(feeder: FrameFeeder, stop_feed: Arc<AtomicBool>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let interval = Duration::from_secs_f64(1.0 / 30.0);
        let mut index = 0u32;
        while !stop_feed.load(Ordering::Relaxed) {
            let started = Instant::now();
            feeder.push_frame(gradient_frame(index));
            index = index.wrapping_add(1);
            if let Some(remaining) = interval.checked_sub(started.elapsed()) {
                thread::sleep(remaining);
            }
        }
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn rawvideo_frames_are_encoded_and_streamed_over_http() {
    if !cast_app::screen::ffmpeg_discover::ffmpeg_available() {
        eprintln!("skipping: ffmpeg not found on PATH");
        return;
    }

    let shutdown = Shutdown::new();
    let server = MediaServer::start(shutdown.clone(), 0);
    let mut port_rx = server.subscribe_port();
    let deadline = Instant::now() + WAIT;
    while *port_rx.borrow() == 0 && port_rx.changed().await.is_ok() && Instant::now() < deadline {}
    let port = *port_rx.borrow();
    assert_ne!(port, 0);

    let (events_tx, events_rx) = mpsc::unbounded_channel();
    // The media server only serves /stream for an active Screen source
    // (same ordering the Phase 10 runtime uses).
    server.set_source(ActiveSource::Screen("test-monitor".to_string()));
    let bridge = ScreenBridge::start_with_encoder(
        server.clone(),
        events_tx,
        shutdown,
        (WIDTH, HEIGHT),
        None,
    )
    .expect("bridge with real ffmpeg must start");

    // Wait for the live stream channel to be attached before connecting
    // (generation bumps once for the source switch, once for the attach).
    let mut generation = server.subscribe_generation();
    let deadline = Instant::now() + WAIT;
    while *generation.borrow() < 2
        && generation.changed().await.is_ok()
        && Instant::now() < deadline
    {}
    assert!(*generation.borrow() >= 2, "screen stream must be attached");

    let stop_feed = Arc::new(AtomicBool::new(false));
    let feeder = feeder_thread(bridge.feeder(), Arc::clone(&stop_feed));

    // Consume the first ~200 KiB of the fMP4 stream.
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/stream");
    let response = tokio::time::timeout(WAIT, client.get(&url).send())
        .await
        .expect("GET /stream timed out")
        .expect("GET /stream must succeed");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("video/mp4")
    );

    let mut stream = response.bytes_stream();
    let mut received: Vec<u8> = Vec::new();
    let deadline = Instant::now() + WAIT;
    while received.len() < 200 * 1024 && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(Some(Ok(chunk))) => received.extend_from_slice(&chunk),
            Ok(Some(Err(error))) => panic!("stream error: {error}"),
            Ok(None) | Err(_) => break,
        }
    }
    assert!(
        received.len() >= 4,
        "expected at least the fMP4 header, got {} bytes",
        received.len()
    );

    // fMP4 starts with the `ftyp` box (4-byte size prefix, then the fourcc),
    // and `empty_moov` writes `moov` up front (spec §4.3) so the receiver
    // can parse immediately.
    assert_eq!(
        &received[4..8],
        b"ftyp",
        "stream must start with the ftyp box, got {:?}",
        &received[..8]
    );
    assert!(
        received.windows(4).any(|window| window == b"moov"),
        "stream must contain the moov atom up-front"
    );
    assert!(
        received.len() > 100 * 1024,
        "expected a substantial encoded stream, got {} bytes",
        received.len()
    );

    drop(stream);
    // Stop the pipeline: the feeder notices, the controller drops stdin,
    // ffmpeg finishes and exits cleanly inside the teardown window (the
    // exit is silent because a stop was requested).
    let error = bridge.last_error();
    stop_feed.store(true, Ordering::Relaxed);
    bridge.stop();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let joiner = thread::spawn(move || {
        bridge.join();
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(WAIT)
        .expect("bridge teardown must complete");
    let _ = joiner.join();
    let _ = feeder.join();

    assert_eq!(error, None, "no pipeline error expected: {error:?}");
    let mut events = events_rx;
    let mut unexpected = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let BackendEvent::StreamError(message) = event {
            unexpected.push(message);
        }
    }
    assert!(
        unexpected.is_empty(),
        "unexpected StreamErrors: {unexpected:?}"
    );
}
