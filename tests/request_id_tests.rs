// SPDX-License-Identifier: MIT OR Apache-2.0
//! Request correlation and namespace message tests (`03-cast-engine.md` §6).
//! Monotonic IDs, correlation hit/miss, the 5-second timeout, byte-exact
//! JSON snapshots, and tolerant response parsers.
//! Gate: `cargo test --test request_id_tests`.

#![forbid(unsafe_code)]

use cast_app::cast::namespaces::{
    CONNECTION_NS, DEFAULT_MEDIA_RECEIVER_APP_ID, HEARTBEAT_NS, MEDIA_NS, PlayerState, RECEIVER_ID,
    RECEIVER_NS, SOURCE_ID, StreamType, TRANSPORT_ID, connect, get_status, is_pong, launch, load,
    media_destination_id, parse_media_status, parse_receiver_status, pause, ping, play, set_volume,
    stop, stop_app,
};
use cast_app::cast::request_id::{PendingMap, RequestId};
use tokio::time::Instant;

// ---------------------------------------------------------------------------
// requestId allocation and correlation (FR-021)
// ---------------------------------------------------------------------------

#[test]
fn request_ids_are_monotonic() {
    let mut ids = RequestId::new();
    let pending = PendingMap::with_default_timeout();
    let allocated: Vec<u32> = (0..100).map(|_| ids.allocate(&pending)).collect();
    assert_eq!(allocated, (1..=100).collect::<Vec<u32>>());
}

#[test]
fn correlation_hit_and_miss() {
    let mut map = PendingMap::with_default_timeout();
    let now = Instant::now();
    assert!(map.insert(42, now));

    // (FR-021) Responses are correlated to outstanding requests by
    // requestId: a hit resolves exactly once, then misses.
    assert!(map.resolve(42));
    assert!(!map.resolve(42));
    assert!(!map.resolve(1));
}

#[tokio::test(start_paused = true)]
async fn five_second_timeout_fires() {
    // (FR-021) A request with no response expires after 5 seconds.
    let mut map = PendingMap::with_default_timeout();
    let now = Instant::now();
    assert!(map.insert(7, now));

    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    assert_eq!(map.expire(Instant::now()), vec![7]);
    assert!(map.is_empty());
}

#[tokio::test(start_paused = true)]
async fn resolution_before_timeout_keeps_map_clean() {
    let mut map = PendingMap::with_default_timeout();
    let now = Instant::now();
    assert!(map.insert(1, now));
    assert!(map.insert(2, now));
    assert!(map.resolve(1));

    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    assert_eq!(
        map.expire(Instant::now()),
        vec![2],
        "resolved entry is gone"
    );
}

// ---------------------------------------------------------------------------
// Source/destination ID table (`03-cast-engine.md` §6.0)
// ---------------------------------------------------------------------------

#[test]
fn addressing_table_matches_spec() {
    assert_eq!(SOURCE_ID, "source-0");
    assert_eq!(TRANSPORT_ID, "transport-0");
    assert_eq!(RECEIVER_ID, "receiver-0");
    assert_eq!(media_destination_id("abc"), "transport-abc");
}

#[test]
fn namespace_urns_match_spec() {
    assert_eq!(CONNECTION_NS, "urn:x-cast:com.google.cast.tp.connection");
    assert_eq!(HEARTBEAT_NS, "urn:x-cast:com.google.cast.tp.heartbeat");
    assert_eq!(RECEIVER_NS, "urn:x-cast:com.google.cast.receiver");
    assert_eq!(MEDIA_NS, "urn:x-cast:com.google.cast.media");
}

// ---------------------------------------------------------------------------
// JSON builders produce exact bytes (snapshots from `03-cast-engine.md` §6)
// ---------------------------------------------------------------------------

#[test]
fn connect_builds_exact_bytes() {
    assert_eq!(
        connect(),
        concat!(
            r#"{"type":"CONNECT","origin":{},"userAgent":"cast-app/"#,
            env!("CARGO_PKG_VERSION"),
            r#"","connType":0,"senderInfo":{"sdkType":2,"version":""#,
            env!("CARGO_PKG_VERSION"),
            r#"","browserVersion":""#,
            env!("CARGO_PKG_VERSION"),
            r#"","platform":6,"connectionType":1}}"#
        )
    );
}

#[test]
fn ping_builds_exact_bytes() {
    assert_eq!(ping(), r#"{"type":"PING"}"#);
}

#[test]
fn launch_builds_exact_bytes() {
    assert_eq!(
        launch(1),
        r#"{"type":"LAUNCH","requestId":1,"appId":"CC1AD845"}"#,
    );
    assert_eq!(DEFAULT_MEDIA_RECEIVER_APP_ID, "CC1AD845");
}

#[test]
fn get_status_builds_exact_bytes() {
    assert_eq!(get_status(3), r#"{"type":"GET_STATUS","requestId":3}"#);
}

#[test]
fn set_volume_builds_exact_bytes() {
    assert_eq!(
        set_volume(4, 0.5, false),
        r#"{"type":"SET_VOLUME","requestId":4,"volume":{"level":0.5,"muted":false}}"#,
    );
    assert_eq!(
        set_volume(5, 1.0, true),
        r#"{"type":"SET_VOLUME","requestId":5,"volume":{"level":1.0,"muted":true}}"#,
    );
}

#[test]
fn stop_app_builds_exact_bytes() {
    assert_eq!(
        stop_app(6, "session-abc"),
        r#"{"type":"STOP_APP","requestId":6,"sessionId":"session-abc"}"#,
    );
}

#[test]
fn load_builds_exact_bytes() {
    // (FR-020) The spec §6.4 example, including streamType BUFFERED.
    assert_eq!(
        load(
            2,
            "session-abc",
            "http://192.168.1.42:8080/stream",
            "video/mp4",
            StreamType::Buffered,
        ),
        r#"{"type":"LOAD","requestId":2,"media":{"contentId":"http://192.168.1.42:8080/stream","contentType":"video/mp4","streamType":"BUFFERED"},"autoplay":true,"currentTime":0,"sessionId":"session-abc"}"#,
    );
}

#[test]
fn load_uses_live_stream_type_for_screen_capture() {
    // `streamType` SHALL be LIVE for the screen-capture source (§6.4).
    assert!(
        load(
            2,
            "session-abc",
            "http://192.168.1.42:8080/stream",
            "video/mp4",
            StreamType::Live
        )
        .contains(r#""streamType":"LIVE""#)
    );
}

#[test]
fn transport_controls_build_exact_bytes() {
    assert_eq!(play(7), r#"{"type":"PLAY","requestId":7}"#);
    assert_eq!(pause(8), r#"{"type":"PAUSE","requestId":8}"#);
    assert_eq!(stop(9), r#"{"type":"STOP","requestId":9}"#);
}

// ---------------------------------------------------------------------------
// Response parsers tolerate extra fields (`03-cast-engine.md` §6.3, §6.4)
// ---------------------------------------------------------------------------

#[test]
fn receiver_status_extracts_ids_and_volume() {
    let payload = r#"{
        "type": "RECEIVER_STATUS",
        "requestId": 1,
        "status": {
            "applications": [
                {"appId": "CC1AD845", "transportId": "t-1", "sessionId": "s-1"}
            ],
            "volume": {"level": 0.5, "muted": false}
        },
        "extraFutureField": {"nested": [1, 2, 3]}
    }"#;
    let status = parse_receiver_status(payload).expect("parses");
    assert_eq!(status.transport_id.as_deref(), Some("t-1"));
    assert_eq!(status.session_id.as_deref(), Some("s-1"));
    let volume = status.volume.expect("volume present");
    assert!((volume.level - 0.5).abs() < f32::EPSILON);
    assert!(!volume.muted);
}

#[test]
fn receiver_status_prefers_matching_application() {
    // A non-Default-Media-Receiver application listed first must not shadow
    // the CC1AD845 entry.
    let payload = r#"{
        "type": "RECEIVER_STATUS",
        "status": {
            "applications": [
                {"appId": "OTHER_APP", "transportId": "t-other", "sessionId": "s-other"},
                {"appId": "CC1AD845", "transportId": "t-1", "sessionId": "s-1"}
            ],
            "volume": {}
        }
    }"#;
    let status = parse_receiver_status(payload).expect("parses");
    assert_eq!(status.transport_id.as_deref(), Some("t-1"));
    assert_eq!(status.session_id.as_deref(), Some("s-1"));
}

#[test]
fn receiver_status_falls_back_to_first_application() {
    // Receivers that omit appId still yield usable IDs.
    let payload = r#"{
        "type": "RECEIVER_STATUS",
        "status": {
            "applications": [{"transportId": "t-1", "sessionId": "s-1"}],
            "volume": {"level": 0.25, "muted": true}
        }
    }"#;
    let status = parse_receiver_status(payload).expect("parses");
    assert_eq!(status.transport_id.as_deref(), Some("t-1"));
    assert_eq!(status.session_id.as_deref(), Some("s-1"));
    let volume = status.volume.expect("volume present");
    assert!((volume.level - 0.25).abs() < f32::EPSILON);
    assert!(volume.muted);
}

#[test]
fn receiver_status_without_application_entry_has_no_ids() {
    let payload = r#"{"type":"RECEIVER_STATUS","status":{"applications":[],"volume":{}}}"#;
    let status = parse_receiver_status(payload).expect("parses");
    assert_eq!(status.transport_id, None);
    assert_eq!(status.session_id, None);
    assert!(status.volume.is_some(), "volume defaults still parsed");
}

#[test]
fn media_status_extracts_player_state_and_idle_reason() {
    let payload = r#"{
        "type": "MEDIA_STATUS",
        "requestId": 2,
        "status": [
            {
                "mediaSessionId": 1,
                "playerState": "IDLE",
                "idleReason": "FINISHED",
                "extra": "tolerated"
            }
        ]
    }"#;
    let info = parse_media_status(payload).expect("parses");
    assert_eq!(info.player_state, PlayerState::Idle);
    assert_eq!(info.idle_reason.as_deref(), Some("FINISHED"));
}

#[test]
fn media_status_maps_all_known_player_states() {
    for (wire, expected) in [
        ("IDLE", PlayerState::Idle),
        ("PLAYING", PlayerState::Playing),
        ("PAUSED", PlayerState::Paused),
        ("BUFFERING", PlayerState::Buffering),
    ] {
        let payload = format!(r#"{{"type":"MEDIA_STATUS","status":[{{"playerState":"{wire}"}}]}}"#);
        let info = parse_media_status(&payload).expect("parses");
        assert_eq!(info.player_state, expected, "playerState {wire}");
        assert_eq!(info.idle_reason, None);
    }
}

#[test]
fn pong_is_detected_for_heartbeat_reset() {
    // (FR-008) A PONG resets the heartbeat timer.
    assert!(is_pong(r#"{"type":"PONG"}"#));
    assert!(!is_pong(r#"{"type":"PING"}"#));
    assert!(!is_pong(r#"{"type":"RECEIVER_STATUS"}"#));
}
