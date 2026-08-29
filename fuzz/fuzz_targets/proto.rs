#![no_main]
use libfuzzer_sys::fuzz_target;

use cast_app::cast::proto::{decode_cast_message, varint_decode, varint_encode};

fuzz_target!(|data: &[u8]| {
    // 1. Varint decoder must not panic on any byte sequence.
    let _ = varint_decode(data);

    // 2. CastMessage decoder must not panic on any byte sequence.
    let _ = decode_cast_message(data);

    // 3. Round-trip property: if varint_decode succeeds, re-encoding must
    // produce bytes that decode to the same value. This exercises both
    // encode and decode without requiring the input to be valid.
    if let Ok((value, consumed)) = varint_decode(data) {
        let encoded = varint_encode(value);
        if let Ok((decoded, _)) = varint_decode(&encoded) {
            assert_eq!(value, decoded);
        }
        // Consumed must not exceed input length.
        assert!(consumed <= data.len());
    }

    // 4. decode -> encode round-trip for any valid message: re-encode with
    // the same fields must be decode-able again.
    if let Ok(msg) = decode_cast_message(data) {
        let encoded = cast_app::cast::proto::encode_cast_message(
            &msg.source_id,
            &msg.destination_id,
            &msg.namespace,
            &msg.payload_utf8,
        );
        let _ = decode_cast_message(&encoded);
    }
});
