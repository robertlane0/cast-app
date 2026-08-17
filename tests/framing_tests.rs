// SPDX-License-Identifier: MIT OR Apache-2.0
//! CastV2 framing tests against the public API (`03-cast-engine.md` §4).
//! Gate: `cargo test --test framing_tests`.

#![forbid(unsafe_code)]

use std::io::Read;

use cast_app::cast::framing::{MAX_FRAME_SIZE, encode_frame, read_frame, write_frame};
use cast_app::cast::proto::encode_cast_message;

/// The complete wire bytes of the canonical PING frame: a 4-byte BE length
/// followed by the hand-derived CastMessage protobuf payload (independent of
/// the code under test).
fn golden_ping_frame() -> Vec<u8> {
    let payload: Vec<u8> = vec![
        0x08, 0x00, // field 1: protocol_version = 0
        0x12, 0x08, b's', b'e', b'n', b'd', b'e', b'r', b'-', b'0', // field 2: sender-0
        0x1A, 0x0A, b'r', b'e', b'c', b'e', b'i', b'v', b'e', b'r', b'-', b'0', // field 3
        0x22, 0x27, // field 4, length 39
        b'u', b'r', b'n', b':', b'x', b'-', b'c', b'a', b's', b't', b':', b'c', b'o', b'm', b'.',
        b'g', b'o', b'o', b'g', b'l', b'e', b'.', b'c', b'a', b's', b't', b'.', b't', b'p', b'.',
        b'h', b'e', b'a', b'r', b't', b'b', b'e', b'a', b't', 0x28,
        0x00, // field 5: payload_type = 0 (STRING)
        0x32, 0x0F, // field 6, length 15
        b'{', b'"', b't', b'y', b'p', b'e', b'"', b':', b'"', b'P', b'I', b'N', b'G', b'"', b'}',
    ];
    let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&payload);
    frame
}

#[test]
fn full_frame_matches_golden_wire_bytes() {
    // (FR-017) encode_cast_message + encode_frame produce exactly the
    // hand-derived wire bytes for the canonical PING frame.
    let payload = encode_cast_message(
        "sender-0",
        "receiver-0",
        "urn:x-cast:com.google.cast.tp.heartbeat",
        r#"{"type":"PING"}"#,
    );
    let framed = encode_frame(&payload);
    assert_eq!(framed, golden_ping_frame());
}

#[test]
fn frame_round_trip_through_write_and_read() {
    // Write with `write_frame`, read back with `read_frame`, clean EOF after.
    let payload = encode_cast_message(
        "sender-0",
        "receiver-0",
        "urn:x-cast:com.google.cast.tp.receiver",
        r#"{"type":"GET_STATUS","requestId":7}"#,
    );

    let mut stream = Vec::new();
    write_frame(&mut stream, &payload).expect("write");
    // A second frame rides the same stream.
    write_frame(&mut stream, b"second").expect("write");

    let mut reader = stream.as_slice();
    assert_eq!(read_frame(&mut reader).expect("frame 1"), Some(payload));
    assert_eq!(
        read_frame(&mut reader).expect("frame 2"),
        Some(b"second".to_vec())
    );
    assert_eq!(read_frame(&mut reader).expect("eof"), None);
}

#[test]
fn maximum_length_frame_is_accepted_and_one_over_rejected() {
    let header = (MAX_FRAME_SIZE as u32).to_be_bytes();
    let mut ok_frame = Vec::new();
    ok_frame.extend_from_slice(&header);
    ok_frame.extend_from_slice(&vec![0u8; MAX_FRAME_SIZE]);
    assert_eq!(
        read_frame(&mut ok_frame.as_slice())
            .expect("max frame reads")
            .unwrap()
            .len(),
        MAX_FRAME_SIZE
    );

    let mut too_large = Vec::new();
    too_large.extend_from_slice(&(MAX_FRAME_SIZE as u32 + 1).to_be_bytes());
    assert!(read_frame(&mut too_large.as_slice()).is_err());
}

#[test]
fn zero_length_frame_reads_as_empty() {
    // A 0-length payload is legal and yields an empty vec, not EOF.
    let mut reader = [0u8, 0, 0, 0].as_slice();
    assert_eq!(read_frame(&mut reader).expect("empty frame"), Some(vec![]));
    assert_eq!(read_frame(&mut reader).expect("clean eof"), None);
}

#[test]
fn framing_works_over_a_raw_byte_reader() {
    // read_frame drives arbitrary Read impls (used later over the TLS stream).
    struct SliceReader<'a>(&'a [u8]);
    impl Read for SliceReader<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = buf.len().min(self.0.len());
            buf[..n].copy_from_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Ok(n)
        }
    }

    let frame = golden_ping_frame();
    let mut reader = SliceReader(&frame);
    let got = read_frame(&mut reader)
        .expect("frame via SliceReader")
        .unwrap();
    assert_eq!(got, &frame[4..]);
    assert_eq!(read_frame(&mut reader).expect("eof"), None);
}
