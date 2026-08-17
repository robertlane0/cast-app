// SPDX-License-Identifier: MIT OR Apache-2.0
//! Cast namespace JSON message builders and response parsers
//! (CONNECT, PING, LAUNCH, GET_STATUS, SET_VOLUME, STOP_APP, LOAD, PLAY,
//! PAUSE, STOP). Owned by `03-cast-engine.md` §6.

use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Message addressing (`03-cast-engine.md` §6.0)
// ---------------------------------------------------------------------------

/// Client source ID used for every message.
pub const SOURCE_ID: &str = "source-0";

/// Destination ID for the connection and heartbeat namespaces.
pub const TRANSPORT_ID: &str = "transport-0";

/// Destination ID for the receiver namespace.
pub const RECEIVER_ID: &str = "receiver-0";

/// Default Media Receiver App ID (`03-cast-engine.md` §6.3).
pub const DEFAULT_MEDIA_RECEIVER_APP_ID: &str = "CC1AD845";

/// Connection namespace URN (`03-cast-engine.md` §6.1).
pub const CONNECTION_NS: &str = "urn:x-cast:com.google.cast.tp.connection";

/// Heartbeat namespace URN (`03-cast-engine.md` §6.2).
pub const HEARTBEAT_NS: &str = "urn:x-cast:com.google.cast.tp.heartbeat";

/// Receiver namespace URN (`03-cast-engine.md` §6.3).
pub const RECEIVER_NS: &str = "urn:x-cast:com.google.cast.receiver";

/// Media namespace URN (`03-cast-engine.md` §6.4).
pub const MEDIA_NS: &str = "urn:x-cast:com.google.cast.media";

/// Media destination ID derived from the `transportId` in the
/// `RECEIVER_STATUS` response to `LAUNCH` (`03-cast-engine.md` §6.0).
pub fn media_destination_id(transport_id: &str) -> String {
    format!("transport-{transport_id}")
}

// ---------------------------------------------------------------------------
// streamType
// ---------------------------------------------------------------------------

/// `streamType` of a `LOAD` request (`03-cast-engine.md` §6.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    /// Local files and proxied URLs.
    Buffered,
    /// Screen-capture source.
    Live,
}

impl StreamType {
    /// The wire value, `"BUFFERED"` or `"LIVE"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "BUFFERED",
            Self::Live => "LIVE",
        }
    }
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

/// Render a JSON value to its exact wire bytes. `Value` cannot hold
/// non-finite floats, so this is infallible.
fn render(value: Value) -> String {
    value.to_string()
}

/// Connection-namespace `CONNECT` (`03-cast-engine.md` §6.1). Per the spec
/// this message carries no `requestId`.
///
/// (FR-007) Send Cast connection `CONNECT`.
pub fn connect() -> String {
    render(json!({"type": "CONNECT"}))
}

/// Heartbeat-namespace `PING` (`03-cast-engine.md` §6.2). Per the spec this
/// message carries no `requestId`.
///
/// (FR-008) Send heartbeat `PING` every 5 seconds.
pub fn ping() -> String {
    render(json!({"type": "PING"}))
}

/// Receiver-namespace `LAUNCH` of the Default Media Receiver
/// (`03-cast-engine.md` §6.3).
///
/// (FR-009) Launch Default Media Receiver app `CC1AD845`.
pub fn launch(request_id: u32) -> String {
    render(json!({
        "type": "LAUNCH",
        "requestId": request_id,
        "appId": DEFAULT_MEDIA_RECEIVER_APP_ID,
    }))
}

/// Receiver-namespace `GET_STATUS` (`03-cast-engine.md` §6.3).
pub fn get_status(request_id: u32) -> String {
    render(json!({"type": "GET_STATUS", "requestId": request_id}))
}

/// Receiver-namespace `SET_VOLUME` (`03-cast-engine.md` §6.3). `level` is
/// clamped to `0.0`–`1.0`; non-finite input is treated as `0.0`.
pub fn set_volume(request_id: u32, level: f32, muted: bool) -> String {
    let level = if level.is_finite() {
        level.clamp(0.0, 1.0) as f64
    } else {
        0.0
    };
    render(json!({
        "type": "SET_VOLUME",
        "requestId": request_id,
        "volume": {"level": level, "muted": muted},
    }))
}

/// Receiver-namespace `STOP_APP` (`03-cast-engine.md` §6.3).
pub fn stop_app(request_id: u32, session_id: &str) -> String {
    render(json!({
        "type": "STOP_APP",
        "requestId": request_id,
        "sessionId": session_id,
    }))
}

/// Media-namespace `LOAD` of `content_id` (`03-cast-engine.md` §6.4):
/// `autoplay: true`, `currentTime: 0`.
///
/// (FR-020) Send media-namespace `LOAD` with the local proxy URL.
pub fn load(
    request_id: u32,
    content_id: &str,
    content_type: &str,
    stream_type: StreamType,
) -> String {
    render(json!({
        "type": "LOAD",
        "requestId": request_id,
        "media": {
            "contentId": content_id,
            "contentType": content_type,
            "streamType": stream_type.as_str(),
        },
        "autoplay": true,
        "currentTime": 0,
    }))
}

/// Media-namespace `PLAY` (`03-cast-engine.md` §6.4).
pub fn play(request_id: u32) -> String {
    render(json!({"type": "PLAY", "requestId": request_id}))
}

/// Media-namespace `PAUSE` (`03-cast-engine.md` §6.4).
pub fn pause(request_id: u32) -> String {
    render(json!({"type": "PAUSE", "requestId": request_id}))
}

/// Media-namespace `STOP` (`03-cast-engine.md` §6.4).
pub fn stop(request_id: u32) -> String {
    render(json!({"type": "STOP", "requestId": request_id}))
}

// ---------------------------------------------------------------------------
// Parsers
// ---------------------------------------------------------------------------

/// Volume reported by a receiver (`03-cast-engine.md` §6.3).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VolumeInfo {
    /// Volume level in `0.0`–`1.0`.
    pub level: f32,
    /// Whether the receiver is muted.
    pub muted: bool,
}

/// Parsed `RECEIVER_STATUS` (`03-cast-engine.md` §6.3): the transport and
/// session IDs of the Default Media Receiver application plus the reported
/// volume. Fields the receiver omitted or left empty stay `None`.
#[derive(Debug, Clone, PartialEq)]
pub struct ReceiverStatus {
    /// `transportId` of the matching application entry.
    pub transport_id: Option<String>,
    /// `sessionId` of the matching application entry.
    pub session_id: Option<String>,
    /// `status.volume`, if present.
    pub volume: Option<VolumeInfo>,
}

/// `playerState` values from `MEDIA_STATUS` (`03-cast-engine.md` §6.4).
/// Unknown values are preserved verbatim rather than failing the parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlayerState {
    Idle,
    Playing,
    Paused,
    Buffering,
    /// Any other `playerState` string observed from the receiver.
    Other(String),
}

impl PlayerState {
    fn parse(value: &str) -> Self {
        match value {
            "IDLE" => Self::Idle,
            "PLAYING" => Self::Playing,
            "PAUSED" => Self::Paused,
            "BUFFERING" => Self::Buffering,
            other => Self::Other(other.to_string()),
        }
    }
}

/// Parsed `MEDIA_STATUS` (`03-cast-engine.md` §6.4).
#[derive(Debug, Clone, PartialEq)]
pub struct MediaStatusInfo {
    /// `playerState` of the first media status entry.
    pub player_state: PlayerState,
    /// `idleReason`, if reported.
    pub idle_reason: Option<String>,
}

/// Parse a `RECEIVER_STATUS` payload, tolerating extra fields. Returns
/// `None` for anything that is not a `RECEIVER_STATUS` message.
///
/// (FR-009) The `transportId` is used as the media destination ID
/// (`03-cast-engine.md` §8).
pub fn parse_receiver_status(payload: &str) -> Option<ReceiverStatus> {
    let value: Value = serde_json::from_str(payload).ok()?;
    if value.get("type")?.as_str() != Some("RECEIVER_STATUS") {
        return None;
    }
    let status = value.get("status")?.as_object()?;

    let volume = status
        .get("volume")
        .and_then(Value::as_object)
        .map(|volume| {
            // `"volume": {}` is a valid receiver response; default to a full,
            // unmuted volume for the fields it omits.
            VolumeInfo {
                level: volume
                    .get("level")
                    .and_then(Value::as_f64)
                    .map(|level| level as f32)
                    .unwrap_or(1.0),
                muted: volume
                    .get("muted")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            }
        });

    // Prefer the application matching the Default Media Receiver App ID;
    // fall back to the first entry so receivers that omit `appId` still
    // yield usable IDs.
    let application = status
        .get("applications")
        .and_then(Value::as_array)
        .and_then(|applications| {
            applications
                .iter()
                .find(|application| {
                    application.get("appId").and_then(Value::as_str)
                        == Some(DEFAULT_MEDIA_RECEIVER_APP_ID)
                })
                .or_else(|| applications.first())
        });

    let transport_id = application
        .and_then(|application| application.get("transportId"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let session_id = application
        .and_then(|application| application.get("sessionId"))
        .and_then(Value::as_str)
        .map(str::to_string);

    Some(ReceiverStatus {
        transport_id,
        session_id,
        volume,
    })
}

/// Parse a `MEDIA_STATUS` payload, tolerating extra fields. Returns `None`
/// for anything that is not a `MEDIA_STATUS` message or that carries an
/// empty `status` array.
///
/// (FR-020) Extracts `playerState` and `idleReason`
/// (`03-cast-engine.md` §6.4).
pub fn parse_media_status(payload: &str) -> Option<MediaStatusInfo> {
    let value: Value = serde_json::from_str(payload).ok()?;
    if value.get("type")?.as_str() != Some("MEDIA_STATUS") {
        return None;
    }
    let status = value.get("status")?.as_array()?.first()?.as_object()?;

    let player_state = status
        .get("playerState")
        .and_then(Value::as_str)
        .map(PlayerState::parse)?;
    let idle_reason = status
        .get("idleReason")
        .and_then(Value::as_str)
        .map(str::to_string);

    Some(MediaStatusInfo {
        player_state,
        idle_reason,
    })
}

/// Whether `payload` is a heartbeat-namespace `PONG`
/// (`03-cast-engine.md` §6.2). A `PONG` resets the heartbeat watchdog.
pub fn is_pong(payload: &str) -> bool {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        == Some("PONG".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_type_wire_values() {
        assert_eq!(StreamType::Buffered.as_str(), "BUFFERED");
        assert_eq!(StreamType::Live.as_str(), "LIVE");
    }

    #[test]
    fn set_volume_clamps_and_rejects_non_finite_levels() {
        assert_eq!(
            set_volume(1, 1.5, false),
            r#"{"type":"SET_VOLUME","requestId":1,"volume":{"level":1.0,"muted":false}}"#
        );
        assert_eq!(
            set_volume(1, -0.5, false),
            r#"{"type":"SET_VOLUME","requestId":1,"volume":{"level":0.0,"muted":false}}"#
        );
        assert_eq!(
            set_volume(1, f32::NAN, true),
            r#"{"type":"SET_VOLUME","requestId":1,"volume":{"level":0.0,"muted":true}}"#
        );
        assert_eq!(
            set_volume(1, f32::INFINITY, false),
            r#"{"type":"SET_VOLUME","requestId":1,"volume":{"level":0.0,"muted":false}}"#
        );
    }

    #[test]
    fn media_destination_is_transport_prefixed() {
        assert_eq!(media_destination_id("abc-123"), "transport-abc-123");
    }

    #[test]
    fn ping_is_detected() {
        assert!(is_pong(r#"{"type":"PONG"}"#));
        assert!(is_pong(r#"{"type":"PONG","requestId":0}"#));
        assert!(!is_pong(r#"{"type":"PING"}"#));
        assert!(!is_pong("not json"));
    }

    #[test]
    fn receiver_status_defaults_volume_when_empty() {
        let status = parse_receiver_status(
            r#"{"type":"RECEIVER_STATUS","status":{"applications":[],"volume":{}}}"#,
        )
        .expect("parses");
        assert_eq!(status.transport_id, None);
        assert_eq!(status.session_id, None);
        assert_eq!(
            status.volume,
            Some(VolumeInfo {
                level: 1.0,
                muted: false
            })
        );
    }

    #[test]
    fn non_status_payloads_parse_to_none() {
        assert!(parse_receiver_status(r#"{"type":"LAUNCH","requestId":1}"#).is_none());
        assert!(parse_receiver_status("garbage").is_none());
        assert!(parse_media_status(r#"{"type":"RECEIVER_STATUS"}"#).is_none());
        assert!(parse_media_status(r#"{"type":"MEDIA_STATUS","status":[]}"#).is_none());
    }

    #[test]
    fn unknown_player_state_is_preserved() {
        let info = parse_media_status(
            r#"{"type":"MEDIA_STATUS","status":[{"playerState":"SOMETHING_NEW","idleReason":null}]}"#,
        )
        .expect("parses");
        assert_eq!(
            info.player_state,
            PlayerState::Other("SOMETHING_NEW".to_string())
        );
        assert_eq!(info.idle_reason, None);
    }
}
