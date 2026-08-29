# Fuzzing cast-app

`cargo fuzz` harnesses for the hand-rolled parsers and untrusted-input
boundaries. Each target fuzzes a pure function that handles data from the
network, the Chromecast, or user-supplied URLs; the oracle is "must not
panic, must not violate memory safety, and must uphold its documented
invariants".

## Prerequisites

```bash
cargo install cargo-fuzz
rustup toolchain install nightly
# Linux screen-capture deps are needed only for the integer build, the fuzz
# build does not link PipeWire at runtime, but its sys crate still needs
# headers to compile:
sudo apt-get install -y libpipewire-0.3-dev libgbm-dev
```

`cargo fuzz` always builds with `nightly` regardless of `rust-toolchain.toml`:

```bash
cargo +nightly fuzz --help
```

## Targets

| Target | What it fuzzes | File |
|---|---|---|
| `dns_parser` | `cast::mdns::parse_packet` — mDNS response parsing, label decompression, compression-pointer depth/cycle checks | `fuzz_targets/dns_parser.rs` |
| `proto` | `cast::proto::{varint_decode, decode_cast_message, encode_cast_message}` — LEB128, length-delimited fields, round-trip invariants | `fuzz_targets/proto.rs` |
| `framing` | `cast::framing::{encode_frame, read_frame, write_frame}` — 4-byte BE length prefix, MAX_FRAME_SIZE rejection, truncated-header handling | `fuzz_targets/framing.rs` |
| `range` | `media::range::parse_range` — `Range: bytes=` parsing, suffix/open-ended, 416 vs 206 decisions | `fuzz_targets/range.rs` |
| `namespaces` | `cast::namespaces::{parse_receiver_status, parse_media_status, is_pong, set_volume, media_destination_id}` — lenient JSON parsers, volume clamping to 0.0..=1.0 | `fuzz_targets/namespaces.rs` |
| `smb_url` | `media::smb_source::{SmbUrl::parse, is_smb_url}` + `media::mime::mime_for_extension` — anonymous-only `smb://` URL validation, no userinfo, percent-decoding | `fuzz_targets/smb_url.rs` |
| `segments` | `screen::segments::Mp4Segmenter::{feed, finish}` — fMP4 box boundary cutting, opaque-fallback, truncated-tail dropping, chunked feeding | `fuzz_targets/segments.rs` |

## Running

List targets:

```bash
cargo +nightly fuzz list
```

Build all targets without running (fast smoke check):

```bash
cargo +nightly fuzz build
```

Run one target (example: 60 s):

```bash
cargo +nightly fuzz run dns_parser -- -max_total_time=60
cargo +nightly fuzz run proto -- -max_total_time=60
cargo +nightly fuzz run framing -- -max_total_time=60
cargo +nightly fuzz run range -- -max_total_time=60
cargo +nightly fuzz run namespaces -- -max_total_time=60
cargo +nightly fuzz run smb_url -- -max_total_time=60
cargo +nightly fuzz run segments -- -max_total_time=60
```

Minimize corpora / test cases:

```bash
cargo +nightly fuzz cmin dns_parser
cargo +nightly fuzz tmin dns_parser fuzz/artifacts/dns_parser/crash-...
```

Coverage (requires `llvm-cov`):

```bash
cargo +nightly fuzz coverage dns_parser
```

## Adding a new target

```bash
cargo +nightly fuzz add my_target
# edit fuzz/Cargo.toml [[bin]] entry and fuzz/fuzz_targets/my_target.rs
```

New parsers that handle untrusted input (DNS, protobuf, HTTP, URLs) should
have a corresponding fuzz target. Prefer `&[u8]` entry points that call the
parser and assert documented invariants; the fuzzer will generate the corpus
automatically. Seed corpora can be checked in as `fuzz/corpus/<target>/`.
