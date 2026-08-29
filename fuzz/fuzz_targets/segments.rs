#![no_main]
use libfuzzer_sys::fuzz_target;

use cast_app::screen::segments::{EncodedSegment, Mp4Segmenter};

fuzz_target!(|data: &[u8]| {
    // Mp4Segmenter must not panic on any byte stream, regardless of chunking.
    let mut segmenter = Mp4Segmenter::new();

    // Feed in random chunk sizes to exercise boundary handling.
    let mut offset = 0;
    let mut produced = Vec::new();
    while offset < data.len() {
        // Use a byte from the input to determine next chunk length (1..32).
        let chunk_len = (data[offset] as usize % 32) + 1;
        let end = (offset + chunk_len).min(data.len());
        // Avoid infinite loop when chunk_len depends on the same bytes we're
        // consuming: advance at least 1 byte beyond header logic.
        let chunk = &data[offset..end];
        if chunk.is_empty() {
            break;
        }
        let segments = segmenter.feed(chunk);
        for seg in &segments {
            // Each segment is either Init or Fragment with non-zero intent.
            assert!(!seg.is_empty() || matches!(seg, EncodedSegment::Init(_)));
        }
        produced.extend(segments);
        if end == offset {
            break;
        }
        offset = end;
        // Prevent unbounded memory growth within the fuzzer.
        if produced.len() > 1024 {
            break;
        }
    }
    let tail = segmenter.finish();
    for seg in &tail {
        let _ = seg.bytes();
        let _ = seg.len();
    }

    // Also test the trivial case: single feed with all bytes then finish.
    let mut seg2 = Mp4Segmenter::new();
    let _ = seg2.feed(data);
    let _ = seg2.finish();

    // And byte-by-byte feeding.
    let mut seg3 = Mp4Segmenter::new();
    for byte in data {
        let _ = seg3.feed(std::slice::from_ref(byte));
    }
    let _ = seg3.finish();
});
