#![no_main]
use libfuzzer_sys::fuzz_target;

use cast_app::media::range::{content_range, parse_range, unsatisfiable_content_range};

fuzz_target!(|data: &[u8]| {
    // Split fuzz input into a header string and a file size.
    // Use first 2 bytes to synthesize a size, remainder as header bytes.
    let (size_bytes, header_bytes) = if data.len() >= 2 {
        data.split_at(2)
    } else {
        (data, &[][..])
    };
    let size = if size_bytes.len() == 2 {
        u64::from(u16::from_le_bytes([size_bytes[0], size_bytes[1]]))
    } else if size_bytes.len() == 1 {
        u64::from(size_bytes[0])
    } else {
        0
    };
    // Extend size to cover larger values occasionally without needing many bytes:
    // mix in length as well.
    let size = size.saturating_add(data.len() as u64 * 13);

    // Header as lossy UTF-8 (Range headers are ASCII, but the parser must
    // handle arbitrary bytes without panicking).
    let header_str = String::from_utf8_lossy(header_bytes);
    let header_opt = if header_bytes.is_empty() {
        None
    } else {
        Some(header_str.as_ref())
    };

    let decision = parse_range(header_opt, size);
    // Exercise the Content-Range builders for any valid range decision
    // so the fuzzer covers formatting without needing a real server.
    match decision {
        cast_app::media::range::RangeDecision::Partial { start, end } => {
            // Valid ranges must have start <= end and end < size (when size > 0).
            assert!(start <= end);
            if size > 0 {
                assert!(end < size);
            }
            let _ = content_range(start, end, size);
        }
        cast_app::media::range::RangeDecision::Unsatisfiable => {
            let _ = unsatisfiable_content_range(size);
        }
        cast_app::media::range::RangeDecision::Full => {}
    }

    // Also test with None header explicitly.
    let _ = parse_range(None, size);
});
