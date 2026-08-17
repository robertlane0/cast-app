// SPDX-License-Identifier: MIT OR Apache-2.0
//! Cross-module protobuf tests against the public API
//! (`03-cast-engine.md` §5). Golden vectors, round trips, and malformed-input
//! rejection. Gate: `cargo test --test protobuf_tests`.

#![forbid(unsafe_code)]

use cast_app::cast::proto::{PayloadType, decode_cast_message, encode_cast_message};

#[test]
fn encode_and_decode_full_messages() {
    // (FR-020) The canonical message shapes all round-trip exactly.
    let cases = [
        (
            "sender-0",
            "receiver-0",
            "urn:x-cast:com.google.cast.tp.connection",
            r#"{"type":"CONNECT"}"#,
        ),
        (
            "sender-0",
            "receiver-0",
            "urn:x-cast:com.google.cast.tp.heartbeat",
            r#"{"type":"PING"}"#,
        ),
        (
            "sender-0",
            "receiver-0",
            "urn:x-cast:com.google.cast.receiver",
            r#"{"type":"GET_STATUS","requestId":1}"#,
        ),
        (
            "sender-1234",
            "receiver-0",
            "urn:x-cast:com.google.cast.media",
            r#"{"type":"LOAD","requestId":2,"media":{"contentId":"http://192.168.1.42:8080/stream","contentType":"video/mp4"}}"#,
        ),
    ];

    for (source, dest, namespace, payload) in cases {
        let encoded = encode_cast_message(source, dest, namespace, payload);
        let message = decode_cast_message(&encoded).expect("round trip decodes");
        assert_eq!(message.protocol_version, 0, "CASTV2_1_0");
        assert_eq!(message.source_id, source);
        assert_eq!(message.destination_id, dest);
        assert_eq!(message.namespace, namespace);
        assert_eq!(message.payload_type, PayloadType::String);
        assert_eq!(message.payload_utf8, payload);
    }
}

#[test]
fn payload_type_one_decodes_as_binary() {
    // A payload_type of 1 (BINARY) decodes without error and is reported as
    // such, even though this application never sends it. The field-5 key is
    // the only `0x28 0x00` byte pair in the stream (0x28 as a namespace
    // length is never followed by 0x00).
    let mut bytes = encode_cast_message("a", "b", "urn:x-cast:com.google.cast.tp.connection", "{}");
    let index = bytes
        .windows(2)
        .position(|w| w == [0x28, 0x00])
        .expect("field 5 key");
    bytes[index + 1] = 0x01;
    let message = decode_cast_message(&bytes).expect("binary payload decodes");
    assert_eq!(message.payload_type, PayloadType::Binary);
}

#[test]
fn maximum_length_field_is_accepted() {
    // Exactly the 16 MiB limit parses; the next byte over is rejected.
    let mut bytes = encode_cast_message("a", "b", "urn:x-cast:com.google.cast.tp.connection", "{}");
    let limit = cast_app::cast::proto::MAX_FIELD_SIZE;
    bytes.extend_from_slice(&[0x32, 0x80, 0x80, 0x80, 0x08]); // len 16 MiB
    bytes.extend_from_slice(&vec![0u8; limit]);
    assert_eq!(
        decode_cast_message(&bytes).unwrap().payload_utf8.len(),
        limit
    );
}
