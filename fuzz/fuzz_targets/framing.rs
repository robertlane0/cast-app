#![no_main]
use std::io::Cursor;

use libfuzzer_sys::fuzz_target;

use cast_app::cast::framing::{MAX_FRAME_SIZE, encode_frame, read_frame, write_frame};

fuzz_target!(|data: &[u8]| {
    // read_frame must not panic on arbitrary bytes.
    let mut cursor = Cursor::new(data);
    let _ = read_frame(&mut cursor);

    // encode_frame -> read_frame round-trip on a truncated prefix of the input
    // (so the payload size stays reasonable for the fuzzer).
    let payload = if data.len() > 1024 { &data[..1024] } else { data };
    let framed = encode_frame(payload);
    let mut cursor = Cursor::new(&framed);
    match read_frame(&mut cursor) {
        Ok(Some(decoded)) => assert_eq!(decoded, payload),
        Ok(None) => panic!("encode_frame payload should be readable"),
        Err(_) => panic!("round-trip should not error for valid payload"),
    }

    // write_frame must produce the same bytes as encode_frame.
    let mut out = Vec::new();
    let _ = write_frame(&mut out, payload);
    assert_eq!(out, framed);

    // Payloads that would exceed MAX_FRAME_SIZE should be rejected on read.
    // Craft a fake header that claims MAX_FRAME_SIZE+1 bytes.
    let mut oversized = Vec::new();
    oversized.extend_from_slice(&((MAX_FRAME_SIZE as u32 + 1).to_be_bytes()));
    oversized.extend_from_slice(&[0u8; 8]);
    let mut cursor = Cursor::new(&oversized);
    let result = read_frame(&mut cursor);
    assert!(result.is_err(), "oversized frame must be rejected");
});
