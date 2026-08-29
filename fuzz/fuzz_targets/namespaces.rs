#![no_main]
use libfuzzer_sys::fuzz_target;

use cast_app::cast::namespaces::{
    is_pong, parse_media_status, parse_receiver_status, set_volume,
};

fuzz_target!(|data: &[u8]| {
    // Interpret fuzz input as a JSON payload candidate.
    let payload = String::from_utf8_lossy(data);

    // All namespace parsers must tolerate arbitrary strings without panicking.
    let _ = parse_receiver_status(&payload);
    let _ = parse_media_status(&payload);
    let _ = is_pong(&payload);

    // set_volume's level clamping must handle any f32 bits.
    if data.len() >= 4 {
        let bits = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let level = f32::from_bits(bits);
        let muted = data.len() > 4 && (data[4] & 1 == 1);
        let json = set_volume(1, level, muted);
        // The produced JSON must be parseable again.
        let _ = parse_receiver_status(&json);
        let _ = is_pong(&json);
        // Level in JSON must be finite and clamped to [0.0, 1.0].
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&json) {
            if let Some(volume) = value.get("volume").and_then(|v| v.get("level")) {
                if let Some(n) = volume.as_f64() {
                    assert!(n.is_finite());
                    assert!((0.0..=1.0).contains(&n), "level {n} out of range");
                }
            }
        }
    }

    // media_destination_id handles any transport-id string.
    let _ = cast_app::cast::namespaces::media_destination_id(&payload);
});
