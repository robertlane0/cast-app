#![no_main]
use libfuzzer_sys::fuzz_target;

use cast_app::cast::mdns::parse_packet;

fuzz_target!(|data: &[u8]| {
    // The DNS parser must never panic on arbitrary input. Both success
    // and error are allowed; the fuzzer's oracle is absence of panics /
    // memory-safety violations.
    let _ = parse_packet(data);

    // Also exercise the lower-level correlators indirectly via the same entry point.
    // A non-empty packet that fails top-level parsing still exercises label
    // decoding, compression-pointer handling, and record correlation.
});
