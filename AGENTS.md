# AGENTS.md

> Canonical guide for any human or AI agent working in this repository. It captures
> the production-ready implementation plan derived from `OVERVIEW.md` and the
> `specs/01-07-*.md` specification set, and it dictates module layout, milestones,
> commands, conventions, and guardrails. When a spec and this document disagree,
> **the spec wins**; update this file to match.

---

## 1. Repository snapshot

| Item | Current state |
|---|---|
| `Cargo.toml` | `name = "cast-app"`, `edition = "2024"`, full dependency set (see §3.2), `[[bin]] fake-encoder`, `[[test]]` entries for the nested integration tests, workspace with `xtask` |
| `src/main.rs` | entrypoint: version banner, tracing init (console + file), tokio runtime + `Backend::start()`, eframe GUI launch, coordinated shutdown |
| `src/lib.rs` | crate root, `#![forbid(unsafe_code)]`, module declarations (`app`, `cast`, `media`, `runtime`, `screen`, `state`, `util`) |
| `OVERVIEW.md` | high-level architecture (three-domain split) |
| `specs/01-07-*.md` | full production specification set (architecture, GUI, cast engine, media proxy, screen capture, concurrency, requirements/tests); all acceptance-criteria checkboxes checked |
| `LICENSE-MIT`, `LICENSE-APACHE` | dual MIT/Apache-2.0, © 2026 Robert Lane |
| `.gitignore` | standard Rust |
| Implementation status | Phases 0–13 complete; only the manual per-OS verification runs (Phase 12, §10, §11) remain, as they require physical Chromecasts |

Target (achieved): a zero-unsafe Rust desktop app that discovers Chromecast
receivers, streams local files / remote URLs / anonymous network shares
(SMB, `04-media-proxy.md` §4.4) / captured displays to them, with a fully
hand-rolled Cast V2 stack and an external `ffmpeg` subprocess for video
encoding.

---

## 2. Non-negotiable hard constraints

These may never be relaxed by an agent without an explicit spec amendment:

1. **`#![forbid(unsafe_code)]`** in every crate root (`src/lib.rs`, `src/main.rs`),
   which covers all modules. Inner attributes are not repeated in module files.
2. **No `rust-cast`, `mdns`, `mdns-sd`, or `prost`** dependencies. The Cast
   stack (mDNS, TLS, CastV2 framing, Protobuf) is hand-rolled.
3. **No `ffmpeg-sys-next` or any C-FFI encoder binding.** Encoding lives in an
   external `ffmpeg` child process.
4. **`reqwest` must use `rustls-tls`**, never `native-tls`.
5. **GUI thread never blocks.** No `await`, no blocking I/O, no `recv()`; only
   `try_recv()` each frame.
6. **Screen capture runs on a dedicated `std::thread`.** Never on the GUI or
   Tokio worker threads.
7. **Stable Rust ≥ 1.85**, edition 2024, pinned via `rust-toolchain.toml`.
8. **`Cargo.lock` is committed.** Major-version anchors: Tokio `1.x`,
   `rustls` `0.23.x`, `reqwest` `0.12.x`, latest stable `eframe`/`egui`,
   `rfd`, `xcap`.

---

## 3. Toolchain & dependency manifest

### 3.1 `rust-toolchain.toml`

```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

### 3.2 `Cargo.toml` (as committed)

```toml
[package]
name = "cast-app"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
authors = ["Robert Lane"]
license = "MIT OR Apache-2.0"

[features]
# Real-Chromecast end-to-end tests (`tests/integration/cast_e2e.rs`): the test
# crate is empty without this feature. Run with:
#   cargo test --features e2e-cast -- --ignored --test-threads=1
e2e-cast = []
# Real-network-share end-to-end tests (`tests/integration/smb_e2e.rs`): empty
# without the feature. Run against a guest-accessible share with:
#   SMB_E2E_SERVER=nas:445 SMB_E2E_SHARE=media SMB_E2E_PATH=dir/video.mp4 \
#     cargo test --features e2e-smb --test smb_e2e -- --ignored --test-threads=1
e2e-smb = []

[dependencies]
eframe = "0.36"            # pin latest stable at impl start
egui   = "0.36"
rfd    = "0.17"
tokio  = { version = "1", features = ["rt-multi-thread", "macros", "net", "fs", "sync", "time", "io-util", "process"] }
rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "stream"] }
xcap   = "0.9"
serde  = { version = "1", features = ["derive"] }
# `preserve_order` keeps JSON object key order so namespace payloads are
# byte-exact against the spec examples (`03-cast-engine.md` §6).
serde_json = { version = "1", features = ["preserve_order"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "registry"] }
tracing-appender = "0.2"
thiserror = "1"
anyhow    = "1"
bytes     = "1"
http      = "1"
if-addrs = "0.15.0"
url = "2"
percent-encoding = "2"
# SHA-256 for trust-on-first-use (TOFU) certificate pinning (`03-cast-engine.md` §3.1):
# the first-seen receiver certificate's digest is stored per receiver key.
sha2 = "0.11"
# Pure-Rust SMB2/3 client for anonymous network-share streaming (`04-media-proxy.md` §4.4).
# Empty username/password = guest; the crate exposes ErrorKind::AuthRequired so
# authentication-required shares fail explicitly instead of prompting.
smb2 = "0.18"
# Stream utilities for the portal client's response wait (`screen/portal.rs`):
# zbus's `MessageStream` needs `TryStreamExt`, and the abort race needs
# `future::select`.
futures-util = "0.3"

# Linux-only Wayland screen capture (`05-screen-capture.md` §3.4): the
# xdg-desktop-portal ScreenCast D-Bus interface (pure-Rust zbus) plus the
# official freedesktop PipeWire client that consumes the portal's stream fd
# in-process — stock FFmpeg has no PipeWire input device, so frames are
# copied out of PipeWire buffers and fed to the `ffmpeg` child as rawvideo,
# exactly like the X11 path. async-io drives the zbus futures on the
# controller `std::thread` (`zbus::block_on` is `#[doc(hidden)]`).
[target.'cfg(target_os = "linux")'.dependencies]
pipewire = "0.9"
# 5.13.x is the last line with MSRV ≤ 1.85 (5.19 requires rustc 1.87).
# `p2p` (not in defaults) powers the fake-portal socket-pair tests.
zbus = { version = ">=5.13, <5.14", features = ["p2p"] }
# The proc-macro crate must stay in lockstep with the zbus lib: 5.19's macros
# emit code referencing `object_server::DispatchResult2`, which 5.13 lacks.
zbus_macros = ">=5.13, <5.14"
async-io = "2"

[dev-dependencies]
pretty_assertions = "1"
rcgen = "0.14.7"
tokio-test = "0.4"

# Cross-platform fake encoder for `tests/screen_pipeline_tests.rs` (ISS-012):
# a compiled binary replaces the Unix-only `/bin/sh` fake-encoder scripts so
# the screen-bridge tests also run on Windows CI. The tests locate it via the
# `CARGO_BIN_EXE_fake-encoder` environment variable.
[[bin]]
name = "fake-encoder"
path = "tests/support/fake_encoder.rs"

# Nested test files are not auto-discovered by cargo.
[[test]]
name = "http_e2e"
path = "tests/integration/http_e2e.rs"

[[test]]
name = "screen_e2e"
path = "tests/integration/screen_e2e.rs"

# Linux-only: portal client against a fake xdg-desktop-portal over a zbus
# p2p socket pair (`05-screen-capture.md` §3.4). Empty on other platforms.
[[test]]
name = "portal_tests"
path = "tests/portal_tests.rs"

# Feature-gated: empty unless `--features e2e-cast` (see `[features]`).
[[test]]
name = "cast_e2e"
path = "tests/integration/cast_e2e.rs"

# Feature-gated: empty unless `--features e2e-smb` (see `[features]`).
[[test]]
name = "smb_e2e"
path = "tests/integration/smb_e2e.rs"

[workspace]
members = ["xtask"]

[profile.release]
lto = "thin"
codegen-units = 1
strip = true
panic = "abort"
```

### 3.3 `deny.toml` (cargo-deny)

- Ban: `rust-cast`, `mdns`, `mdns-sd`, `prost`, `prost-build`, `ffmpeg-sys-next`,
  `libav`, the full `libav*-sys` family, `native-tls`, `tokio-native-tls`.
- `[bans] multiple-versions = "warn"` so duplicate transitive deps surface in
  `cargo deny check`.
- Allow: only MPL/Apache/MIT/BSD/Unicode-3.0 licenses, with per-crate exceptions
  for permissive licenses the pinned GUI/TLS stack pulls in transitively
  (BSL-1.0, ISC, Zlib, OFL-1.1, Ubuntu-font-1.0, CDLA-Permissive-2.0).
- `[advisories]`: a curated ignore list for known-unfixable advisories
  (currently `RUSTSEC-2026-0192`, unmaintained `ttf-parser` via the Linux
  GUI font theme).
- `[sources]`: `unknown-registry = "deny"`, `unknown-git = "deny"`.

---

## 4. Build, test, lint, audit commands

```bash
# Format
cargo fmt
cargo fmt --check

# Lint (must be clean)
cargo clippy --all-targets -- -D warnings

# Test
cargo test --all
cargo test --doc

# Build
cargo build
cargo build --release

# Safety & dependency audits
cargo run -p xtask                   # memory-safety scan of src/, tests/, xtask/
cargo deny check                      # license + ban list
cargo tree --duplicates               # review duplicate deps before merge

# Optional feature-gated end-to-end tests (require real Chromecast)
cargo test --features e2e-cast -- --ignored --test-threads=1

# Optional feature-gated end-to-end tests (require a guest-accessible SMB share)
SMB_E2E_SERVER=nas:445 SMB_E2E_SHARE=media SMB_E2E_PATH=dir/video.mp4 \
  cargo test --features e2e-smb --test smb_e2e -- --ignored --test-threads=1
```

CI gate (GitHub Actions matrix: `ubuntu-latest`, `windows-latest`, `macos-14`):
`fmt --check` → `clippy -D warnings` (default and `e2e-cast` features) →
`test` → `test --doc` → `e2e-cast` target compiles (`--no-run`) → `build` →
`build --release` → `cargo run -p xtask` →
`cargo deny check`, with the committed `Cargo.lock` uploaded as an artifact.

---

## 5. Target module layout

```
src/
  main.rs                # entrypoint: tracing init, runtime, eframe launch
  lib.rs                 # crate root, `#![forbid(unsafe_code)]`, re-exports
  state.rs               # CastDevice, SourceTab, AppCommand, BackendEvent
  app.rs                 # CastDashboard eframe::App impl + UI rendering
  runtime.rs             # tokio runtime, supervisor, task graph wiring
  util/
    mod.rs
    shutdown.rs          # Shutdown token (watch channel)
    retry.rs             # exponential backoff helper
    backpressure.rs      # bounded channel with drop-oldest semantics
  cast/
    mod.rs
    mdns.rs              # UDP multicast discovery + DNS parser
    tls.rs               # rustls ClientConfig + permissive verifier + peer fingerprint
    framing.rs           # 4-byte BE length-prefix encode/decode
    proto.rs             # hand-rolled CastMessage protobuf codec
    request_id.rs        # monotonic u32 + pending-request map w/ 5s timeout
    tofu.rs              # TOFU certificate-pin store: SHA-256 pins keyed by TXT id=/friendlyName+IP, persisted known_hosts.json
    namespaces.rs        # CONNECT, PING, LAUNCH, GET_STATUS, SET_VOLUME, STOP_APP, LOAD, PLAY, PAUSE, STOP
    connection/
    mod.rs             # facade: CastConnection handle, re-exports, test_support
    transport.rs       # Transport trait, SharedTransport, Connector/TlsConnector (TOFU-aware)
    reader.rs          # FrameAccumulator + dedicated reader thread
    writer.rs          # framed send_payload on spawn_blocking workers
    state_machine.rs   # Phase/Command/events, inbound routing, run loop, reconnect policy
    teardown.rs        # STOP → STOP_APP → close_notify ordering
media/
    mod.rs
    flush.rs             # FlushTracker: bounded byte/time flush cadence for streaming handlers
    server.rs            # tokio TcpListener bound to the receiver's resolved interface (wildcard 0.0.0.0 only after user consent); HTTP/1.1, /stream routing, SetBindAddr/SetPort rebinds
    range.rs             # Range parser + Content-Range builder
    mime.rs              # extension -> MIME map
    lan_ip.rs            # LAN IP selection (subnet -> default route -> loopback)
    local_file.rs        # 200/206/416 + 64 KiB chunked streaming
    url_proxy.rs         # reqwest GET with Range forwarding + 502 policy
    smb_source.rs        # smb:// URLs: SmbUrl parse (no userinfo), SmbConnector/SmbFile traits, Smb2Connector (guest), serve 200/206/416/400/401/403/404/502
    source.rs            # ActiveSource enum, switch-terminates-in-flight
  screen/
    mod.rs
    ffmpeg_discover.rs   # PATH lookup; bool result + cached
    bgra_rgba.rs         # safe BGRA -> RGBA conversion
    segments.rs          # Mp4Segmenter: parse ffmpeg stdout into whole fMP4 segments
    capture.rs           # xcap monitor selection + 30 fps capture thread + WAYLAND_SCREEN_ENTRY
    portal.rs            # xdg-desktop-portal ScreenCast client (zbus), AbortSignal, portal_available (Linux)
    pipewire.rs          # in-process PipeWire client: negotiated PwFormat/PixFmt, capture thread (Linux)
    ffmpeg.rs            # Command builder, lifecycle, EOF+kill policy
    bridge/              # split along the documented thread boundaries (cast::connection precedent)
      mod.rs             # ScreenBridge facade, PipelineInput, thread wiring, single-shot error reporter
      capture_link.rs    # capture -> encoder: cap-2 frame queue, FrameFeeder, capture-thread spawn
      controller.rs      # encoder lifecycle/restart (EncoderState generation bookkeeping), EOF->wait->kill; portal dance + teardown_portal (Linux)
      stdout_reader.rs   # per-generation stdout reader: segmentation (read_encoded), protected-init push policy, reader-handle reap
      forwarder.rs       # output -> HTTP: live-channel forwarder, whole-segment drops, init retry until room

tests/
  dns_parser_tests.rs
  protobuf_tests.rs
  framing_tests.rs
  flush_tests.rs         # FlushTracker byte/time flush cadence for streaming handlers
  range_tests.rs
  mime_tests.rs
  lan_ip_tests.rs
  request_id_tests.rs
  connection_tests.rs
  runtime_tests.rs
  tls_e2e.rs            # self-signed cert via rcgen (dev-dep)
  screen_pipeline_tests.rs
  portal_tests.rs       # Linux-only: real ZbusScreenCast vs fake portal over a zbus p2p socket pair
  smb_tests.rs          # smb:// parse + serve semantics via SmbConnector/SmbFile fakes
  gui_state_tests.rs
  support/
    fake_encoder.rs    # compiled by `[[bin]] fake-encoder` (ISS-012)
  integration/
    http_e2e.rs          # in-process server + reqwest client
    cast_e2e.rs          # #[ignore] real-device tests behind feature flag
    screen_e2e.rs        # dummy rawvideo producer -> ffmpeg -> HTTP
    smb_e2e.rs           # #[ignore] real-share tests behind feature flag (SMB_E2E_* env)

xtask/
  Cargo.toml
  forbid_unsafe.rs       # binary that scans src/, tests/, xtask/ for forbidden-keyword tokens

rust-toolchain.toml
deny.toml
Cargo.lock
```

---

## 6. Phased implementation plan

Each phase is independently mergeable. Do not start Phase N+1 until Phase N's
acceptance criteria pass.

### Phase 0 — Scaffolding
- [x] Add `rust-toolchain.toml`, populate `Cargo.toml` (§3.2), add `deny.toml`.
- [x] Replace `src/main.rs` with `#![forbid(unsafe_code)]` + tracing init + version banner.
- [x] Create `src/lib.rs` with `#![forbid(unsafe_code)]` and module declarations.
- [x] Add `xtask/forbid_unsafe.rs` (`cargo run -p xtask`): programmatic memory-safety scan of `src/`, `tests/`, `xtask/` (replaces the original `scripts/forbid-unsafe-check.sh` gate).
- [x] Create empty module files with `//!` doc comments referencing their owning spec.
- [x] **Gate:** `cargo build` clean, `cargo run -p xtask` passes, `cargo deny check` passes.

### Phase 1 — Foundation types (`state.rs`, `util/`)
- [x] `state.rs`: `CastDevice { id, name, addr }`, `SourceTab`, `AppCommand`, `BackendEvent` per `02-gui.md` §4.1.
- [x] `util/shutdown.rs`: `Shutdown` wrap of `tokio::sync::watch::<bool>` with `subscribe()`, `is_shutting_down()`, `trigger()`.
- [x] `util/retry.rs`: exponential backoff iterator (1s, 2s, 4s, ..., cap 30s, max 5).
- [x] `util/backpressure.rs`: `BoundedDropOldest<T>` over `mpsc::channel` with `try_send` + drain-then-send.
- [x] **Tests:** shutdown propagation; backpressure drops oldest; backoff sequence.
- [x] **Gate:** `cargo test util` green.

### Phase 2 — mDNS discovery (`cast/mdns.rs`) ← `03-cast-engine.md` §2
- [x] UDP bind `0.0.0.0:0`, join `224.0.0.251`.
- [x] Build PTR query for `_googlecast._tcp.local`, ID=0, no recursion flag.
- [x] 10-second requery loop with shutdown token.
- [x] DNS parser:
  - 12-byte header parse (QDCOUNT/ANCOUNT/NSCOUNT/ARCOUNT).
  - Question, answer, authority, additional sections.
  - Label decoding + compression pointers (max depth 4, cycle guard).
  - Skip unsupported record types.
  - Never panic on malformed packets; log + skip.
- [x] Record correlation: PTR instance name → SRV (port), A (IPv4), TXT (`fn=`).
- [x] De-dup by `(IP, port)`; expire after 3 missed cycles.
- [x] Push snapshots via `BackendEvent::ReceiversUpdated`.
- [x] Surface fatal setup errors via `ConnectionError`.
- [x] **Tests:** golden PTR/SRV/TXT/A packets; compression pointer chains; malformed packets; instance-name correlation; friendly-name fallback.
- [x] **Gate:** `cargo test --test dns_parser_tests` green.

### Phase 3 — TLS transport (`cast/tls.rs`) ← `03-cast-engine.md` §3
- [x] `rustls::ClientConfig` with `ring` provider, no ALPN, no SNI.
- [x] Custom `ServerCertVerifier` that accepts self-signed but still completes the handshake (signature verified, chain/hostname skipped).
- [x] `connect(addr) -> Result<rustls::StreamOwned<...>>` with 5-second handshake timeout (tokio time).
- [x] `close_notify` on shutdown.
- [x] **Tests:** unit-test the verifier decision logic; integration test against a local self-signed test server (rustls server config + rcgen for test cert, **dev-dep only**).
- [x] **Gate:** `cargo test cast::tls` green.

> **Lesson recorded (Phase 3):** `tokio::net::TcpStream::into_std()` leaves the
> socket non-blocking — call `set_nonblocking(false)` before driving the
> synchronous rustls handshake on a `spawn_blocking` worker. And synchronous
> TLS I/O (handshake, reads) must never run directly on a tokio executor
> task: on a single-thread runtime the blocking call freezes the runtime so
> timers and other tasks never advance (debugged via a hang that only
> reproduced with blocking sockets). Rules for Phase 6: all
> `CastTlsStream` I/O goes through `spawn_blocking`/dedicated threads. The
> handshake worker must also be deadline-bounded *on its own*: `spawn_blocking`
> cancellation is cooperative (a caller-side timeout does not stop the
> thread, so a stalled peer can strand it forever). `cast/tls.rs` re-arms
> per-op socket read/write timeouts (SO_RCVTIMEO/SO_SNDTIMEO, surfaced as
> `WouldBlock` on Linux and `TimedOut` on Windows) to the remaining handshake
> budget on every `complete_io` cycle, so the worker always exits and drops
> its socket at latest ~one op timeout after the deadline, independently of
> the caller. `complete_io`'s `Ok` must NOT be treated as success — check
> `conn.is_handshaking()` — because on a timed-out blocking socket it returns
> `Ok` with "no progress". An in-flight worker gauge (`IN_FLIGHT_HANDSHAKES`)
> plus tracing on worker start/finish and the caller-timeout path make
> stranded workers observable.

> **Lesson recorded (Phase 6):** three non-obvious concurrency bugs surfaced by
> the mock-transport tests. (1) **Mutex barging:** a reader thread that
> re-locks a shared transport `Mutex` microseconds after every WouldBlock
> poll starves a blocked writer indefinitely — the writer loses the
> unlock→re-lock race every cycle. Fixed by sleeping ~5ms (`IDLE_READ_BACKOFF`)
> after an idle poll, which opens a deterministic window for queued writers.
> (2) **Continuous-inbound starvation:** the 5ms idle backoff alone does not
> protect writers when inbound data never lets the reader idle-poll — with
> PONGs arriving continuously the reader's cycle is lock→read(instant)→re-lock
> and a blocked writer can lose every 5ms window for hundreds of milliseconds
> (observed: 350ms PING stalls on a 22-core box). Hardened by (a) yielding
> `IDLE_READ_BACKOFF` after *every* read cycle, and (b) making `send_payload`
> poll `try_lock` on a 1ms cadence (`WRITER_LOCK_RETRY`) instead of blocking
> on the mutex. (3) **Executor blocking:** `#[tokio::test]` defaults to a
> current-thread runtime; a test task blocked in a std `Condvar::wait`/`Mutex`
> freezes the executor, so `spawn_blocking` completions are never polled. Use
> `#[tokio::test(flavor = "multi_thread")]` whenever a test performs
> blocking I/O on its own task. Also: `cfg(test)` is NOT set when the lib is
> built for integration tests, so test doubles shared with `tests/` must not
> be `#[cfg(test)]`-gated.

### Phase 4 — Protobuf + framing (`cast/proto.rs`, `cast/framing.rs`) ← `03-cast-engine.md` §4, §5
- [x] Varint LEB128 encoder/decoder.
- [x] Field encoders: varint, length-delimited (string, bytes).
- [x] `encode_cast_message(source, dest, ns, payload) -> Vec<u8>` covering fields 1, 2, 3, 4, 5, 6.
- [x] `decode_cast_message(bytes) -> CastMessage` with unknown-field skipping by wire type.
- [x] `write_frame(writer, payload)`: single write of 4-byte BE length + payload.
- [x] `read_frame(reader) -> Vec<u8>`: read 4 BE bytes, then exactly N bytes; reject N > 16 MiB.
- [x] **Tests:** golden vectors for each field; round-trip; unknown-field tolerance; 16 MiB+1 rejection; varint edge cases (0, 127, 128, u32::MAX).
- [x] **Gate:** `cargo test --test protobuf_tests --test framing_tests` green.

### Phase 5 — Request correlation + namespaces (`cast/request_id.rs`, `cast/namespaces.rs`) ← `03-cast-engine.md` §6
- [x] `RequestId` counter; `PendingMap` keyed by `u32` with 5s timeout per entry.
- [x] JSON builders (using `serde_json::json!`):
  - Connection: `{"type":"CONNECT"}`
  - Heartbeat: `{"type":"PING"}`
  - Receiver: `LAUNCH {appId:"CC1AD845"}`, `GET_STATUS`, `SET_VOLUME {volume:{level,muted}}`, `STOP_APP {sessionId}`
  - Media: `LOAD {media:{contentId,contentType,streamType},autoplay,currentTime}`, `PLAY`, `PAUSE`, `STOP`
- [x] Response parsers: `RECEIVER_STATUS` → `(transportId, sessionId, volume)`, `MEDIA_STATUS` → `(playerState, idleReason)`, `PONG` → heartbeat reset.
- [x] Source/destination ID table per spec §6.0.
- [x] `streamType`: `BUFFERED` for file/URL, `LIVE` for screen.
- [x] **Tests:** monotonic IDs; correlation hit/miss; 5s timeout fires; JSON builders produce exact bytes (snapshot tests); parsers tolerate extra fields.
- [x] **Gate:** `cargo test --test request_id_tests` green.

### Phase 6 — Connection lifecycle (`cast/connection/`) ← `03-cast-engine.md` §7
- [x] State machine: `Disconnected → Connecting → Connected → Launching → Ready → Streaming → Teardown`.
- [x] Heartbeat task: PING every 5s; PONG watchdog 10s → teardown + reconnect.
- [x] Reconnect policy: exponential backoff per `util/retry.rs`, max 5 attempts; surface `ConnectionError` to GUI when exhausted.
- [x] Inbound JSON router: PONG, RECEIVER_STATUS, MEDIA_STATUS.
- [x] Public API: `select()`, `launch_default_receiver()`, `load(url, stream_type)`, `play()`, `pause()`, `stop()`, `set_volume(level, muted)`, `shutdown()`.
- [x] Teardown sequence: `STOP` → `STOP_APP` → `close_notify` → close socket.
- [x] **Tests:** state transitions with a mock TLS stream; heartbeat watchdog fires; reconnect backoff; teardown ordering.
- [x] **Gate:** `cargo test cast::connection` green (in-module gate tests) + `cargo test --test connection_tests` (integration: watchdog/exhaustion, reconnect, teardown ordering, volume round-trip, disconnected-command handling, PONG keep-alive).

### Phase 7 — Media proxy (`media/`) ← `04-media-proxy.md`
- [x] `mime.rs`: extension map (mp4/webm/mkv/mov/mp3/aac/m4a/flac/wav; default `application/octet-stream`).
- [x] `range.rs`: parse `bytes=a-b`, `bytes=a-`, `bytes=-suffix`; build `Content-Range`; classify as valid/invalid/multi/none.
- [x] `lan_ip.rs`: enumerate non-loopback IPv4 interfaces; match subnet containing receiver IP; fallback to default-route interface; fallback to `127.0.0.1` with `warn!`. Re-run on receiver change.
- [x] `server.rs`: tokio `TcpListener` bound to the interface address resolved for the selected receiver (`set_bind_addr`; rebind on receiver change, keeps the bind address on `SetProxyPort`); the app path starts **unbound** and the `0.0.0.0` wildcard is used only after an explicit user consent pop-up (`BindFallbackRequested`/`BindFallback`); the old listener is dropped before a rebind (same-port rebinds from the wildcard would otherwise hit `EADDRINUSE`); a failed bind is acked back and leaves the server unbound; HTTP/1.1 request line + headers; GET/HEAD only (else 405); route `/stream` only (else 404); `SetPort` is a no-op while unbound.
- [x] `local_file.rs`: open `tokio::fs::File`; 200 (full) / 206 (single range) / 416 (unsatisfiable); 64 KiB chunks; `Accept-Ranges`, `Content-Type`, `Content-Length`, `Cache-Control: no-cache`; HEAD = headers only.
- [x] `url_proxy.rs`: `reqwest::Client` with rustls-tls; reject userinfo URLs; forward `Range`; up to 5 redirects; 30s first-byte timeout; no overall timeout while streaming; pass through non-2xx status + body; 502 on connection failure.
- [x] `source.rs`: `ActiveSource { File(PathBuf) | Url(String) | Screen(monitor_name) }`; switching terminates in-flight connection via per-connection cancellation token.
- [x] **Tests:** MIME table; Range parser all cases; LAN IP selection (subnet/default/loopback); HTTP server end-to-end with reqwest client; 404/405/200/206/416; remote proxy 502 + Range forwarding; HEAD behavior; source switch cancels in-flight.
- [x] **Gate:** `cargo test --test range_tests --test mime_tests --test lan_ip_tests --test http_e2e` green (188 tests across the suite; `--test integration::http_e2e` is not a resolvable target — nested test files need a `[[test]]` entry in `Cargo.toml`, which names the target `http_e2e`).

### Phase 8 — Screen capture pipeline (`screen/`) ← `05-screen-capture.md`
- [x] `ffmpeg_discover.rs`: `PATH` scan via `std::env::var_os("PATH")`; cached result; `ffmpeg_available() -> bool`, `ffmpeg_path()`, `reset_cache()`; `#[cfg(test)] PATH_OVERRIDE` (no `set_var`).
- [x] `bgra_rgba.rs`: pure-safe BGRA→RGBA in-place byte shuffle; verified against pinned `xcap` 0.9.6 at impl time — **xcap already returns RGBA on Linux X11/macOS/Windows, so `XCAP_FRAMES_ARE_RGBA = true` and capture does not shuffle; `bgra_to_rgba` remains as a tested fallback**.
- [x] `capture.rs`:
  - `std::thread::spawn` capture loop.
  - **xcap 0.9.6 has no `Monitor::from_name`** → `Monitor::all()` + `name()` match (`monitor_by_name`, `monitor_resolution`); names enumerated for `DisplaysUpdated`.
  - 30 fps pacing (`std::thread::sleep`).
  - Resolution from xcap; restart signal on resolution change (controller restarts ffmpeg with new `-s WxH`).
  - Wayland detection (`XDG_SESSION_TYPE == "wayland"` or `WAYLAND_DISPLAY` set on Linux) → `monitor_names()` returns the virtual `["Screen"]` entry iff `ffmpeg_available() && portal_available()`, else Err; the capture thread refuses to start on Wayland (runtime path routes to `start_portal` instead).
  - 5 consecutive capture failures → stop + `StreamError`.
- [x] `portal.rs` (Linux): pure-zbus `ZbusScreenCast` (`create_session`/`select_sources`/`start`/`open_pipewire_remote`/`close`) with the `Request.Response` signal subscribed before each call and filtered by expected key (stale responses skipped); `AbortSignal` (stop flag + shutdown) interrupts a pending dialog; `portal_available()` via blocking `name_has_owner`.
- [x] `pipewire.rs` (Linux): `PwFormat`/`PixFmt` (4-byte formats only, mapped to `-pix_fmt`), `spawn_pipewire_capture` on a dedicated thread driving `Loop::iterate` (pollable), negotiated format reported exactly once over a status channel, later failures as `Err`.
- [x] `ffmpeg.rs`:
  - `Command` with spec §4 args (rawvideo, rgba, `-s WxH`, `-r 30`, libx264 ultrafast, zerolatency, fMP4, `pipe:1`) **+ recorded working-set addition `-g 30`** (1 s keyframe interval; x264's default keyint=250 would delay fMP4 fragments ~8 s and stall live output); `encoder_args` takes the platform's `pix_fmt` (`rgba` for xcap, negotiated `rgb0`/`bgr0`/`rgba`/`bgra` on Wayland) and `Ffmpeg::spawn_pipewire` drives the portal path.
  - stdin/stdout piped; stderr captured (50-line tail) for diagnostics.
  - Lifecycle: EOF → wait ≤5 s → kill → reap (`wait_graceful`); unexpected non-zero exit → error.
  - `-movflags frag_keyframe+empty_moov` baseline; `default_base_moof` fallback recorded pending real-receiver validation.
- [x] `bridge/` (split along the documented thread boundaries, following the `cast::connection` precedent: `mod.rs` facade + `capture_link.rs` + `controller.rs` + `stdout_reader.rs` + `forwarder.rs`):
  - `mod.rs`: `PipelineInput::{Frames{initial_resolution, capture_thread}, Portal{portal, capture}}`; `ScreenBridge::start` routes `is_wayland_session()` → `start_portal` (which also accepts a `custom_encoder` for tests); `start_with_encoder` never spawns the xcap thread (capture_thread: false — it must not trip the Wayland guard on dev machines); single-shot `report_once` error reporter.
  - `capture_link.rs`: `BoundedDropOldest` cap 2 capture→controller (drop-oldest; eviction is producer-side, so the queue stays ≤2 under bursts); `FrameFeeder`; capture-thread spawn wiring (`spawn_capture_thread`).
  - `controller.rs`: controller thread (owns `Ffmpeg`, writes stdin, restart on resolution change, EOF→wait→kill teardown); restart generation bookkeeping extracted as `EncoderState` (EOF old → graceful wait → join old readers → clear output → spawn next) with focused inline tests (restart bookkeeping, spawn-failure surfacing, live feed, closed-stdin error); portal dance + `teardown_portal` + `prepare_portal`/`wait_for_format` (Linux).
  - `stdout_reader.rs`: dedicated stdout reader thread per encoder generation — `read_encoded` cuts stdout into **whole fMP4 segments** (`Mp4Segmenter`, `screen/segments.rs`) and pushes them through a cap-8 drop-oldest queue that **never evicts the init segment**; `push_segment` protected/evictable selection and `register_reader` reap-before-push bookkeeping tested inline.
  - `forwarder.rs`: forwarder thread pushes whole segments into the media server's live channel (cap 8, drop-newest on overflow: the in-hand segment is discarded whole, the init retried until room) and tears down the session when the consumer closes; `forward_segment` policy tested inline (whole-fragment drop, init retry until room, init-retry stop, closed consumer).
  - Portal mode (Linux): `prepare_portal` runs the dance + spawns the PipeWire thread on the controller, `wait_for_format` polls the status channel against the teardown signals (100 ms ticks), any error cleans up (thread joined, session closed) before returning; `teardown_portal` stops the PipeWire thread → joins it → `portal.close(session)`; the controller surfaces format changes as encoder restarts (`EncoderTarget::Portal`) and PipeWire failures as `StreamError`.
  - **Lesson:** a fake encoder that forks background children survives SIGKILL of the shell and keeps the stdout/stderr pipes open forever — bridge `join()` hangs on the readers. Test fakes must kill their own children on stdin EOF.
  - **Lesson (ISS-009, byte-level drop corruption):** drop-oldest on *raw byte chunks* can slice a box mid-header — the receiver sees a truncated fMP4 and stalls or dies. Backpressure must operate on whole segments: the reader accumulates boxes (size≥8, largesize handled, 64 MiB cap, opaque fallback on a corrupt header, truncated tail at EOF dropped), so an overflow drop always discards an entire `moof`+`mdat` fragment and the wire never carries a partial box. A restart (resolution change) additionally clears the segment queue and joins the old readers *before* the new encoder starts, so the consumer sees exactly one init segment followed by complete fragments.
- **Lesson (Phase 12):** POSIX 2.9.3.1 gives an *asynchronous list* (`cmd &`) a `/dev/null` stdin whenever job control is off — so `cat >/dev/null &` EOFs instantly, `wait $CPID` returns, and the emitter is killed before any chunk reaches the pipe. A fake encoder that relied on that pattern failed ~50% of `screen_pipeline_tests` runs (the emitter raced the kill). Fix: `exec 3<&0; cat >/dev/null <&3 &` — an explicit fd redirect overrides the implicit `/dev/null` assignment and `cat` then genuinely holds the stdin pipe until the bridge closes it (EOF → the script kills its own emitter → clean exit).
- [x] **Tests:** bridge inline unit tests (14: `capture_link` frame-queue pattern; `controller` `EncoderState` restart bookkeeping, spawn-failure surfacing, live feed, closed-stdin error; `stdout_reader` segmentation via `read_encoded`, protected-init push policy, reader-handle reap; `forwarder` whole-fragment drop, init retry until room, init-retry stop, closed consumer) + `tests/screen_pipeline_tests.rs` (9: EOF-before-exit, resolution restart, unexpected exit → StreamError, drop-oldest under a full pipe, client-disconnect teardown, slow-consumer whole-segment drops, restart emits a fresh init, **plus 2 Wayland portal-mode tests** — fake `ScreenCast` + fake PipeWire spawner through the real bridge, including the negotiation-failure path) + `tests/portal_tests.rs` (6: real `ZbusScreenCast` vs a fake portal over a zbus p2p socket pair — full dance + fd passing + session close, stale-response skip, canceled, rejected, abort, unknown-session error) + `tests/integration/screen_e2e.rs` (real ffmpeg: rawvideo feeder → bridge → HTTP, ftyp/moov asserted, skip when ffmpeg absent).
- [x] **Gate:** `cargo test --test screen_pipeline_tests --test portal_tests --test screen_e2e` green.

### Phase 9 — GUI (`app.rs`) ← `02-gui.md`
- [x] `CastDashboard` struct per spec §4.2.
- [x] Left panel (~250 px): receiver list with `Scanning` / `No receivers found` / `Error+retry` states; row = name + IP:port.
- [x] Center panel: tabbed `Display` / `Local File` / `Web URL`.
  - Display: dropdown from `DisplaysUpdated`; disabled when no monitors or ffmpeg missing.
  - Local File: `rfd::AsyncFileDialog` with media-type filters.
  - Web URL: text input; Apply disabled until `http://` / `https://` absolute URL with host parses.
- [x] Bottom bar (~48 px): Play / Pause / Stop (disabled per spec rules); Volume slider 0..=100 → 0.0..=1.0; mute toggle; throttled to 1 message / 100 ms; corrected from `BackendEvent::Volume`.
- [x] Status strip: colored dot (amber/green/red); playback state; transient error banner with manual dismiss.
- [x] Settings modal: proxy port input validated `1024..=65535`; Save dispatches `SetProxyPort(u16)`.
- [x] Each frame: `try_recv` drain `event_rx` to exhaustion before rendering.
- [x] egui default dark theme; no custom skinning.
- [x] **Tests:** state transitions for all `AppCommand` variants; URL validation; volume throttle timing; status-indicator updates from synthetic `BackendEvent`s.
- [x] **Gate:** `cargo test --test gui_state_tests` green (33 tests); manual smoke test on each supported OS.

> **Lessons recorded (Phase 9):** (1) **Spec §3.1 requires a retry action but §4.1's enum had no rescan command** — added `AppCommand::Rescan` to `state.rs` and the spec enum; Phase 10 must map it to an immediate mDNS re-query. (2) **eframe 0.36 changed the `App` trait** — `update` is gone; implement `ui(&mut self, ui: &mut egui::Ui, frame: &mut eframe::Frame)`, and panels are the unified `egui::Panel::left/top/bottom` with `exact_size(...)` (no more `SidePanel`/`TopBottomPanel`, no `resizable` on top/bottom by default). (3) **`futures_util::poll!` is `async`-only** (gated behind the `async-await` feature) — cannot be used on the GUI thread; poll the `rfd::AsyncFileDialog` future manually with `std::task::Waker::noop()` + `Context::from_waker`, re-polling every frame (`Pin<Box<dyn Future + Send>>` is `Unpin`, so `Pin::new(&mut *fut).poll(&mut cx)` works). (4) **Discovery Error state is driven by `BackendEvent::ConnectionError`** while the receiver list is empty — the Phase 2 contract surfaces fatal mDNS setup errors via `ConnectionError`. (5) `futures-util` was a dev-dependency only until the Wayland portal work moved it to main deps (`screen/portal.rs` needs `StreamExt` + `future::select`).

### Phase 10 — Runtime & supervisor (`runtime.rs`) ← `06-concurrency.md`
- [x] Build `tokio::runtime::Runtime::new()` (multi-threaded).
- [x] Spawn Task A (mDNS), Task B (Cast), Task C (HTTP); spawn capture thread via `std::thread::spawn`.
- [x] Supervisor task owns `Shutdown` token; fatal mDNS or Cast failure → `ConnectionError` + halt dependents.
- [x] Aggregate backend → GUI through a single `UnboundedSender<BackendEvent>`.
- [x] On app exit: trigger shutdown → drop runtime → drop TLS socket, HTTP listener, ffmpeg child via `Drop`.
- [x] **Tests:** shutdown ordering (HTTP stops accepting → Cast closes → mDNS stops → capture thread joins → ffmpeg killed); event aggregation.
- [x] **Gate:** `cargo test runtime` green; manual: quit app, verify no orphan `ffmpeg` process.

> **Lessons recorded (Phase 10):** (1) **`watch::Sender::send` self-deadlocks when a `Ref` from `borrow()` is still alive** — `send` (via `send_replace`) takes the value write-lock while the outstanding `Ref` holds the read-lock on the same `RwLock`. `self.rescan.send(self.rescan.borrow() + 1)` froze the supervisor task for good (found via a `fatal_mdns` runtime test hang; masked earlier because test mDNS tasks panicked at spawn and dropped their receivers, and tokio only errors when `receiver_count() == 0`). Always drop the `Ref` before `send`/`send_replace`. (2) **tokio 1.53 `UdpSocket::from_std` panics on a blocking socket** — "Registering a blocking socket with the tokio runtime is unsupported" (tokio-rs/tokio#7172). Every socket injected into the backend (tests' discovery socket, the mdns sniffer socket) must be `set_nonblocking(true)` first. (3) **`Backend::shutdown` must never run inside a tokio runtime** — `runtime.block_on` panics; runtime tests are plain `#[test]`, and a panic that skips `backend.shutdown()` leaves the `Runtime::drop` blocking forever (drop waits for all tasks). (4) **The cast task auto-launches**: `LOAD` in `Phase::Connected` sends `LAUNCH` first and queues the LOAD until `Ready` — integration tests must push a `RECEIVER_STATUS` to see the LOAD on the wire.

### Phase 11 — Integration tests + CI
- [x] `tests/integration/http_e2e.rs`: spin server in-process, exercise all Range cases with `reqwest`.
- [x] `tests/integration/screen_e2e.rs`: dummy rawvideo stdin feeder → ffmpeg → HTTP → reqwest consumer; skip if `ffmpeg` absent.
- [x] `tests/integration/cast_e2e.rs`: `#[ignore]` tests requiring a real Chromecast; gated behind `--features e2e-cast`.
- [x] GitHub Actions matrix (ubuntu/windows/macos) running §4 CI gate (`.github/workflows/ci.yml`).
- [x] Upload `Cargo.lock` artifact on each run.

> **Phase 11 notes:** (1) `cast_e2e.rs` is feature-gated with `#![cfg(feature =
> "e2e-cast")]` plus `[[test]]` entry in `Cargo.toml`; CI only compiles it
> (`--no-run`). Run manually with
> `cargo test --features e2e-cast --test cast_e2e -- --ignored --test-threads=1`;
> pin a receiver via `CAST_E2E_RECEIVER=IP:port` or let mDNS discovery pick one.
> (2) `matches!` cannot express match guards — use a full `match` closure for
> value-asserting event predicates. (3) macos-13 runners were retired by GitHub;
> the matrix uses `macos-14` (spec `01-architecture.md` §8 allows "macOS 13+").

### Phase 12 — Production hardening
- [x] `tracing-subscriber` with `env-filter` (`CAST_APP_LOG=info` default; falls back to `RUST_LOG`, then `info`).
- [x] Audit every `unwrap`/`expect`/`panic` in non-init code; convert to `?` + typed error.
  - Remaining `expect`/`unreachable!` are documented init-only (`runtime.rs` runtime build) or provably-reachable-never match arms (`state_machine.rs` run-loop dispatch filters `Select`/`Shutdown` before `handle_command`; `capture.rs` join-handle taken once from a locally-created handle).
- [x] Backpressure tuning: capture channel cap 2, encoder channel cap 8.
- [x] Log file at platform log dir (Linux `$XDG_STATE_HOME`/`~/.local/state`, macOS `~/Library/Logs`, Windows `%LOCALAPPDATA%`; `std::env::temp_dir()` fallback).
- [x] Release profile: `lto = "thin"`, `codegen-units = 1`, `strip = true`, `panic = "abort"`.
- [x] Update `README.md` with per-platform build/run/ffmpeg-install instructions.
- [ ] Walk the manual verification script (§11 below) on each supported OS (requires physical Chromecasts + per-OS machines).
- [x] Tick every acceptance-criteria box in `specs/07-requirements-and-tests.md` (also `specs/03-cast-engine.md` §8 and `specs/06-concurrency.md` §6).

### Phase 13 — Anonymous SMB share streaming (`04-media-proxy.md` §4.4)
- [x] `smb://host[:port]/share/dir/file` accepted by the Web URL source; percent-decoded share/path; userinfo, query, fragment, and empty path segments rejected at parse (`SmbUrl` structurally carries no credentials).
- [x] `smb2 = "0.18"` pure-Rust client behind `SmbConnector<F>`/`SmbFile` traits; `Smb2Connector` uses empty username/password (guest), `auto_reconnect: false`, `dfs_enabled: false`; per-request anonymous session, handle closed when the response ends.
- [x] `/stream` serves SMB sources with the local-file Range policy (200/206/416, `HEAD` without body); the client `Range` is translated into positioned `read_at(offset, len)` reads (1 MiB chunks), never a whole-file refetch.
- [x] Error mapping: 400 invalid URL, 401 guest-logon rejected (permanent, no retry-with-credentials), 403 access denied, 404 missing, 502 transport; mid-stream read failure aborts the connection.
- [x] `tests/smb_tests.rs` (24 tests) via fakes; `tests/integration/smb_e2e.rs` behind `--features e2e-smb` (`SMB_E2E_SERVER`/`SHARE`/`PATH`, optional `SMB_E2E_AUTH_SERVER` for the 401 path).
- [x] **Gate:** `cargo test --test smb_tests` green; `cargo clippy --all-targets --features e2e-smb` clean; `smb_e2e` compiles `--no-run`.
- [x] Docs: `specs/04-media-proxy.md` §4.4 (+ §6 criteria), `specs/02-gui.md` §3.2, `specs/07-requirements-and-tests.md` (FR-033/FR-034, test matrix), `AGENTS.md`, CI (`e2e-smb` clippy/`--no-run` on the matrix).

> **Lessons recorded (Phase 13):** (1) **`url` 2.5.8 removed the `percent_encoding` re-export** — add `percent-encoding = "2"` directly. (2) **`smb2` pulls thiserror 2.x alongside our thiserror 1.x** (`cargo tree --duplicates` shows both); they are type-incompatible so never mix them in one module — `SmbError` stays a hand-rolled `thiserror 1` enum and wraps `smb2::Error` by value. (3) **`smb2::Error` is not `Clone`** (it holds `io::Error`), so `SmbError` cannot derive `Clone`; test fakes must not clone errors — the fake read-failure is a bool that builds a fresh error per read. (4) **Clippy's `question_mark` + inference:** `let result = match … { … Ok(()) }` then `result?;` is ambiguous under `-D warnings`; annotate `let result: io::Result<()> = match …`. (5) **`async fn` in trait impls is fine for RPITIT traits** — the lib desugars trait *declarations* to `impl Future + Send` (clippy `async_fn_in_trait`), but test *impls* may use plain `async fn` (clippy `manual_async_fn` demands it). (6) `split('\r\n')` yields a trailing empty element after the head terminator — response-head parsers in tests must filter empty lines or the last header panics.

> **Lessons recorded (Wayland portal phase):** (1) **`MatchRule::path_namespace` requires a valid `ObjectPath`** — a trailing slash (`…/desktop/request/`) is rejected with `InvalidObjectPath`; the namespace must be the prefix without the final `/`. (2) **Options dicts must be `HashMap<String, Value>`** — an array of `(str, Value)` tuples serializes as `a(sv)` and a real portal rejects the signature; only `HashMap` emits `a{sv}`. (3) **The session handle is an `o`, not an `s`** — `create_session` returns it as a string, but every ScreenCast method wants an object path; parse it (`ObjectPath::try_from`) at each call site. (4) **Never wait for a payload key alone in `call_and_wait`** — a canceled dialog replies code 1 with an *empty* `results` dict; waiting for the expected key hangs forever. Accept `code != 0 || expect(...)` and map the code to `Canceled`/`Rejected`. (5) **The zbus `#[interface]` macro derives member names with plain `pascal_case`** — `open_pipewire_remote` becomes `OpenpipewireRemote` (lowercase w), but the real portal's member is `OpenPipeWireRemote`; the fake must set `#[zbus(name = "OpenPipeWireRemote")]`. (6) **The fake portal tests caught four real production bugs** (the four above) — the p2p socket-pair harness pays for itself; a fake that is *signature-faithful* is the point. (7) **`pipewire` 0.9.x, not 0.10.x** — two `pipewire-sys` versions conflict on `links = "pipewire-0.3"` (xcap pulls `pipewire ^0.9`); 0.9.2 unifies them. 0.9.2 API deltas vs 0.10: `pw::init()` returns `()`, `Context::connect_fd(fd, None)` (no `connect_fd_rc`), `Loop::iterate(Duration)` and `Loop::enter/leave` is `unsafe`-gated and unneeded, `StreamBox::connect(direction, id, flags, params: &mut [&Pod])`, `StreamState::Error(String)`, `property!` `Choice, Range` rule takes **no** trailing comma (Enum/Id rules do). (8) **The zbus builder for p2p is `zbus::connection::Builder`** (no `Connection::builder`), and both ends of the socket pair must build **concurrently** (`futures_util::try_join!`) because the SASL handshake needs both sides live — zbus's own tests do the same. (9) **`start_with_encoder` must not spawn the xcap capture thread** — the harness feeds frames itself, and on a Wayland dev machine `start_capture`'s Wayland guard made every pipeline test fail; `PipelineInput::Frames` gained an explicit `capture_thread` flag (`false` for the harness, `true` from `ScreenBridge::start`). (10) **Keep the fake portal connection alive in the test scope** — the ObjectServer dispatcher runs on the async-io reactor, and dropping the `Connection` handle kills the socket (the fd-passing and session-close assertions silently stop working).

---

## 7. Coding conventions for agents

- **Every crate root carries `#![forbid(unsafe_code)]`** (`src/lib.rs`, `src/main.rs`); module files must not repeat the inner attribute.
- **Module-level `//!` docs** state purpose and link to the owning spec section.
- **Public functions carry `///` docs** referencing requirement IDs, e.g. `/// (FR-005) Encode a CastV2 frame.`
- **Errors:** `thiserror` for typed errors inside `cast::`, `media::`, `screen::`; `anyhow` only in `main.rs`/`runtime.rs` glue. No `unwrap`/`expect` outside init code.
- **Async signatures** take a `&Shutdown` (or owned `CancellationToken`) guard where long-running.
- **Channels:**
  - GUI ↔ backend: `tokio::sync::mpsc::unbounded_channel`.
  - Capture → bridge: bounded(2), drop-oldest.
  - Encoder → HTTP: bounded(8) **whole-segment** backpressure (`EncodedSegment` items, never raw bytes — a dropped item must be an entire fMP4 fragment so no partial box reaches the wire).
- **Logging:** `tracing::{info,warn,error,debug}`. No `println!` outside the pre-logging startup banner in `main.rs`.
- **Tests:** `#[tokio::test]` for async, `#[test]` for sync. Integration tests are separate files under `tests/`.
- **Commit style:** conventional commits — `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `refactor:`.
- **No silent panics:** every `.unwrap()` in PR review is a blocker unless justified in the diff.
- **JSON:** use `serde_json::json!` for outbound; `serde_json::Value` for inbound (tolerant parsing).

---

## 8. Spec cross-reference

| Module | Owning spec |
|---|---|
| `state.rs`, `app.rs` | `02-gui.md` |
| `cast/mdns.rs` | `03-cast-engine.md` §2 |
| `cast/tls.rs` | `03-cast-engine.md` §3 |
| `cast/tofu.rs` | `03-cast-engine.md` §3.1 |
| `cast/proto.rs`, `cast/framing.rs` | `03-cast-engine.md` §4–5 |
| `cast/request_id.rs`, `cast/namespaces.rs` | `03-cast-engine.md` §6 |
| `cast/connection/` (facade + transport/reader/writer/state_machine/teardown) | `03-cast-engine.md` §7 |
| `media/*` | `04-media-proxy.md` |
| `screen/*` | `05-screen-capture.md` (portal.rs/pipewire.rs: §3.4) |
| `runtime.rs`, `util/shutdown.rs` | `06-concurrency.md` |
| `rust-toolchain.toml`, `deny.toml`, CI | `01-architecture.md` §8–9 |
| Acceptance criteria & test matrix | `07-requirements-and-tests.md` |

---

## 9. Anti-patterns to refuse in review

Any of the following is an automatic PR block:

- `unsafe`, `unsafe impl`, `unsafe fn`, `#![allow(unsafe_code)]` anywhere.
- New dependency on `rust-cast`, `mdns`, `mdns-sd`, `prost`, `prost-build`, `ffmpeg-sys-next`, `libav-sys`, or any `*-tls-native` feature.
- `reqwest` with `default-features = true` (must disable default + use `rustls-tls`).
- `std::net::TcpListener::accept` blocking call inside a Tokio task (use `tokio::net`).
- `xcap::Monitor::capture_image` called from the GUI thread or a Tokio worker.
- `event_rx.recv().await` on the GUI thread (must be `try_recv`).
- Skipping `requestId` on any outbound Cast request.
- Forwarding `Range` to a remote origin without mirroring the 206/Content-Range back to the Chromecast.
- Closing a TLS connection without `close_notify`.
- Forwarding arbitrary client headers through the URL proxy (only `Range` is allowed).
- Spawning `ffmpeg` without `-movflags frag_keyframe+empty_moov` (or the validated working flag set).
- Bounded channel that uses `send().await` instead of drop-oldest in the capture/encoder pipelines.
- Any new `unwrap()`/`expect()` in `cast/`, `media/`, or `screen/`.

---

## 10. Production-readiness definition of done

A merge to `main` is production-ready when **all** of the following are true:

- [x] `#![forbid(unsafe_code)]` compiles on every supported target.
- [x] `cargo tree` shows zero banned crates (`cargo deny check` green).
- [x] Every acceptance-criteria checkbox in `specs/02-07-*.md` is checked, with the verifying test artifact referenced in the PR.
- [x] All seven spec files reviewed against final code; no silent deviations.
- [x] No `unwrap()`/`expect()` in hot paths; init-only usages documented.
- [x] No `await` or blocking I/O on the GUI thread (verified by code review + runtime check).
- [x] Heartbeat watchdog, reconnect backoff, and shutdown ordering covered by tests.
- [ ] HTTP Range correctness verified with `curl` and a real Chromecast on each OS.
- [ ] fMP4 `-movflags` working set validated against a real receiver; result recorded in `screen/ffmpeg.rs` doc comment.
- [ ] CI green on ubuntu-latest, windows-latest, macos-14.
- [x] `Cargo.lock` committed and reflects pinned versions from §3.2.
- [x] `ffmpeg` PATH discovery error UX is clear and actionable on each platform.
- [ ] Manual verification script (§11) executed successfully on at least one developer machine per OS.

---

## 11. Manual verification script (run before tagging release)

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all
cargo test --doc
cargo build --release
cargo run -p xtask
cargo deny check
```

Then, on a LAN with a real Chromecast and a machine with `ffmpeg` on `PATH`:

1. Launch `cast-app`.
2. Confirm a receiver appears in the left panel within ~10 s.
3. Select the receiver; verify the status dot turns green (`Connected <name>`).
4. **Local File** tab: pick an `.mp4`; verify playback; seek via the Chromecast's own UI; verify `206 Partial Content` lines in the tracing log.
5. **Web URL** tab: enter a public-domain media URL; verify playback.
6. **Display** tab: pick a monitor; verify mirrored playback; switch monitors; verify `ffmpeg` restarts with new `-s WxH`.
7. Adjust the volume slider; verify smooth updates and that `BackendEvent::Volume` corrects the slider after release.
8. Toggle mute; verify on the receiver.
9. Quit the app; verify no orphan `ffmpeg` process and no lingering listening socket (`lsof -i :8080` should be empty).
10. Re-launch, change the proxy port in Settings, verify the advertised URL updates and the listener rebinds.

---

## 12. Common pitfalls (read before touching each domain)

- **mDNS:** the receiver replies to the source port of your query socket; you do **not** need to bind `5353`. Binding `5353` will fail on most OSes without root.
- **DNS compression:** a pointer can target another pointer; cap depth at 4 and track visited offsets to break cycles.
- **TLS:** rustls 0.23 requires you to explicitly select a crypto provider (`rustls::crypto::ring::default_provider().install_default()` once at startup).
- **TOFU (`03-cast-engine.md` §3.1):** the pin check lives in the connector (`TlsConnector::record_pin`), keyed by `CastDevice::tofu_key` (TXT `id=` wins over `friendlyName+IP` — only the TXT id survives DHCP address changes). A mismatch must never block the connection and must never re-pin: keep the original pin (SSH semantics) and surface `BackendEvent::CertificateWarning`, which the GUI must NOT auto-dismiss on success events (a security notice blinking away mid-connect is worthless). The store persists atomically (tmp + rename) and must degrade to an empty in-memory store on any load/save failure — pinning is best-effort hardening, never a startup failure. `sha2` hashes the end-entity DER from `ClientConnection::peer_certificates()` *after* the handshake, so the handshake worker needs no changes.
- **Protobuf:** the length prefix is part of the **framing**, not the protobuf payload. Encode payload → measure → prepend 4-byte BE length.
- **Heartbeat:** PONG must reset the watchdog, not just be received. If you only set "received any message" you'll miss silent heartbeat failures.
- **HTTP Range:** `bytes=0-` is valid (200 OK or 206 from offset 0); `bytes=-0` is unsatisfiable (416); `bytes=1-0` is malformed (416). never flush per chunk in streaming handlers — it defeats the `BufWriter` and burns a syscall per chunk. Use `FlushTracker` with per-handler byte/time thresholds (`url_proxy.rs` `FLUSH_BYTES`/`FLUSH_INTERVAL` = 32 KiB/25 ms; `server.rs` live-screen = 64 KiB/50 ms), flush the response head immediately, and always flush the tail before a close-delimited stream ends.
- **URL proxy:** do **not** forward the Chromecast's `User-Agent` or `Host` headers upstream — they will break remote CDNs.
- **SMB (`04-media-proxy.md` §4.4):** anonymous-only by construction — the parsed `SmbUrl` type has no credential fields, so no code path can ever present credentials. `smb2`'s `Error` is not `Clone`, so never derive `Clone` on wrappers around it and never clone errors in fakes. A guest-logon rejection is `ErrorKind::AuthRequired` and is permanent (401), not a signal to retry with credentials. `split('\r\n')` on a response head yields a trailing empty line — filter it or the last header parse panics.
- **Screen capture:** the pinned `xcap` 0.9.6 was verified at implementation time to return **RGBA on Linux X11, macOS, and Windows** — no conversion runs in the capture loop (`XCAP_FRAMES_ARE_RGBA = true`). Do not trust that forever: re-verify against the pinned version when upgrading `xcap`, and keep the unit-tested `bgra_to_rgba` fallback (and `-pix_fmt rgba` in the ffmpeg args) as the safety net.
- **`xcap::Monitor` is `!Send` on Windows:** it wraps an `HMONITOR` (`*mut c_void`), so a `FrameSource` holding it cannot cross into a spawned thread. Construct the source *inside* the capture thread and move only the monitor *name* (`String`) across; `FrameSource` must not be `Send`-bounded. A second Windows-only trap: a `connect()` to a closed loopback port can report success at the socket layer with the refusal arriving later — tests must accept `ConnectTimeout` alongside `TlsError::Connect`.
- **fMP4:** if the receiver stalls at "Buffering" forever, add `default_base_moof` to `-movflags` and re-test. Record the working flag set in code.
- **Shutdown ordering:** drop the HTTP listener **first** so no new `/stream` connections arrive while the Cast connection is tearing down; otherwise the Chromecast may retry the URL mid-teardown.
- **Media-server binding:** the listener binds the receiver's resolved interface; rebinding from `0.0.0.0` to a specific address on the same port requires dropping the old listener **before** binding, or Linux returns `EADDRINUSE`. A failed interface bind leaves the server unbound (no stale listener) and is acked back so the runtime can request the user-consented wildcard fallback (`BindFallbackRequested`/`BindFallback`); the wildcard bind only ever happens after that consent (once per session). The GUI-to-backend event types for consent live in `state.rs`; runtime tests that exercise startup must answer the consent event (or the wildcard stays unbound and Play's advertised port is 0).
- **Tokio + std::thread:** the capture thread is `std::thread`, not a tokio task. Bridge into tokio via a `tokio::sync::mpsc::Sender` created with `unbounded_channel` (or bounded per §7) and `tokio::runtime::Handle::current()` captured before spawn.

---

## 13. Agent workflow checklist (run on every change)

1. Did I add or modify any `.rs` file? If it's a new crate root (`src/lib.rs`, `src/main.rs`), confirm `#![forbid(unsafe_code)]` is present; module files must not carry the inner attribute.
2. Did I add a dependency? Confirm it is not on the ban list and update `deny.toml` if needed.
3. Did I touch Cast protocol code? Add or update a golden-vector unit test.
4. Did I touch HTTP code? Add or update a Range / 404 / 405 / 502 test.
5. Did I touch screen pipeline? Verify backpressure still drops oldest; verify no `xcap` call on GUI/tokio threads.
6. Did I touch the GUI? Verify no new `await` or blocking call; verify `try_recv` drain remains non-blocking.
7. `cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test --all`.
8. `cargo run -p xtask`.
9. Update the relevant spec's acceptance-criteria checkboxes if behavior changed.
10. Conventional-commit message; reference requirement IDs in the body if applicable.

---

End of `AGENTS.md`. Treat it as the source of truth for *how* to build what the
specs describe. When in doubt, defer to the specs; when the specs are silent,
defer to this document.
