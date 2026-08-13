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
| `Cargo.toml` | `name = "cast-app"`, `edition = "2024"`, **no dependencies** |
| `src/main.rs` | stub `println!("Hello, world!")` |
| `OVERVIEW.md` | high-level architecture (three-domain split) |
| `specs/01-07-*.md` | full production specification set (architecture, GUI, cast engine, media proxy, screen capture, concurrency, requirements/tests) |
| `LICENSE` | MIT, © 2026 Robert Lane |
| `.gitignore` | standard Rust |

Target: a zero-unsafe Rust desktop app that discovers Chromecast receivers, streams
local files / remote URLs / captured displays to them, with a fully hand-rolled
Cast V2 stack and an external `ffmpeg` subprocess for video encoding.

---

## 2. Non-negotiable hard constraints

These may never be relaxed by an agent without an explicit spec amendment:

1. **`#![forbid(unsafe_code)]`** in every `.rs` file, root and module.
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

### 3.2 `Cargo.toml` (target shape)

```toml
[package]
name = "cast-app"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"

[dependencies]
eframe = "0.29"            # pin latest stable at impl start
egui   = "0.29"
rfd    = "0.15"
tokio  = { version = "1", features = ["rt-multi-thread", "macros", "net", "fs", "sync", "time", "io-util", "process"] }
rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "stream"] }
xcap   = "0.4"
serde  = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "1"
anyhow    = "1"
bytes     = "1"
http      = "1"

[dev-dependencies]
pretty_assertions = "1"
tokio-test = "0.4"

[profile.release]
lto = "thin"
codegen-units = 1
strip = true
panic = "abort"
```

### 3.3 `deny.toml` (cargo-deny)

- Ban: `rust-cast`, `mdns`, `mdns-sd`, `prost`, `prost-build`, `ffmpeg-sys-next`,
  `libav`, any crate whose name contains `native-tls`.
- Allow: only MPL/Apache/MIT/BSD/Unicode-3.0 licenses.

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
./scripts/forbid-unsafe-check.sh      # grep -rn 'unsafe' src/ tests/ xtask/
cargo deny check                      # license + ban list
cargo tree --duplicates               # review duplicate deps before merge

# Optional feature-gated end-to-end tests (require real Chromecast)
cargo test --features e2e-cast -- --ignored --test-threads=1
```

CI gate (GitHub Actions matrix: `ubuntu-latest`, `windows-latest`, `macos-13`):
`fmt --check` → `clippy -D warnings` → `test` → `build` → `forbid-unsafe-check.sh`
→ `cargo deny check`.

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
    tls.rs               # rustls ClientConfig + permissive verifier
    framing.rs           # 4-byte BE length-prefix encode/decode
    proto.rs             # hand-rolled CastMessage protobuf codec
    request_id.rs        # monotonic u32 + pending-request map w/ 5s timeout
    namespaces.rs        # CONNECT, PING, LAUNCH, GET_STATUS, SET_VOLUME, STOP_APP, LOAD, PLAY, PAUSE, STOP
    connection.rs        # full CastConnection lifecycle + reconnect policy
  media/
    mod.rs
    server.rs            # tokio TcpListener HTTP/1.1 server
    range.rs             # Range parser + Content-Range builder
    mime.rs              # extension -> MIME map
    lan_ip.rs            # LAN IP selection (subnet -> default route -> loopback)
    local_file.rs        # 200/206/416 + 64 KiB chunked streaming
    url_proxy.rs         # reqwest GET with Range forwarding + 502 policy
    source.rs            # ActiveSource enum, switch-terminates-in-flight
  screen/
    mod.rs
    ffmpeg_discover.rs   # PATH lookup; bool result + cached
    bgra_rgba.rs         # safe BGRA -> RGBA conversion
    capture.rs           # xcap monitor selection + 30 fps capture thread
    ffmpeg.rs            # Command builder, lifecycle, EOF+kill policy
    bridge.rs            # capture -> ffmpeg stdin pipe, stdout reader thread

tests/
  dns_parser_tests.rs
  protobuf_tests.rs
  framing_tests.rs
  range_tests.rs
  mime_tests.rs
  lan_ip_tests.rs
  request_id_tests.rs
  event_channel_tests.rs
  screen_pipeline_tests.rs
  gui_state_tests.rs
  integration/
    http_e2e.rs          # in-process server + reqwest client
    cast_e2e.rs          # #[ignore] real-device tests behind feature flag
    screen_e2e.rs        # dummy rawvideo producer -> ffmpeg -> HTTP

xtask/
  forbid_unsafe.rs       # binary that scans src/ for `unsafe` tokens

scripts/
  forbid-unsafe-check.sh
  dep-audit.sh
  ci.sh

rust-toolchain.toml
deny.toml
Cargo.lock
```

---

## 6. Phased implementation plan

Each phase is independently mergeable. Do not start Phase N+1 until Phase N's
acceptance criteria pass.

### Phase 0 — Scaffolding
- [ ] Add `rust-toolchain.toml`, populate `Cargo.toml` (§3.2), add `deny.toml`.
- [ ] Replace `src/main.rs` with `#![forbid(unsafe_code)]` + tracing init + version banner.
- [ ] Create `src/lib.rs` with `#![forbid(unsafe_code)]` and module declarations.
- [ ] Add `scripts/forbid-unsafe-check.sh` (grep -rn `unsafe` and fail on any hit).
- [ ] Add `xtask` binary to programmatically enforce the unsafe scan.
- [ ] Create empty module files with `//!` doc comments referencing their owning spec.
- [ ] **Gate:** `cargo build` clean, `forbid-unsafe-check.sh` passes, `cargo deny check` passes.

### Phase 1 — Foundation types (`state.rs`, `util/`)
- [ ] `state.rs`: `CastDevice { id, name, addr }`, `SourceTab`, `AppCommand`, `BackendEvent` per `02-gui.md` §4.1.
- [ ] `util/shutdown.rs`: `Shutdown` wrap of `tokio::sync::watch::<bool>` with `subscribe()`, `is_shutting_down()`, `trigger()`.
- [ ] `util/retry.rs`: exponential backoff iterator (1s, 2s, 4s, ..., cap 30s, max 5).
- [ ] `util/backpressure.rs`: `BoundedDropOldest<T>` over `mpsc::channel` with `try_send` + drain-then-send.
- [ ] **Tests:** shutdown propagation; backpressure drops oldest; backoff sequence.
- [ ] **Gate:** `cargo test util` green.

### Phase 2 — mDNS discovery (`cast/mdns.rs`) ← `03-cast-engine.md` §2
- [ ] UDP bind `0.0.0.0:0`, join `224.0.0.251`.
- [ ] Build PTR query for `_googlecast._tcp.local`, ID=0, no recursion flag.
- [ ] 10-second requery loop with shutdown token.
- [ ] DNS parser:
  - 12-byte header parse (QDCOUNT/ANCOUNT/NSCOUNT/ARCOUNT).
  - Question, answer, authority, additional sections.
  - Label decoding + compression pointers (max depth 4, cycle guard).
  - Skip unsupported record types.
  - Never panic on malformed packets; log + skip.
- [ ] Record correlation: PTR instance name → SRV (port), A (IPv4), TXT (`fn=`).
- [ ] De-dup by `(IP, port)`; expire after 3 missed cycles.
- [ ] Push snapshots via `BackendEvent::ReceiversUpdated`.
- [ ] Surface fatal setup errors via `ConnectionError`.
- [ ] **Tests:** golden PTR/SRV/TXT/A packets; compression pointer chains; malformed packets; instance-name correlation; friendly-name fallback.
- [ ] **Gate:** `cargo test --test dns_parser_tests` green.

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
> `CastTlsStream` I/O goes through `spawn_blocking`/dedicated threads.

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
- [ ] `RequestId` counter; `PendingMap` keyed by `u32` with 5s timeout per entry.
- [ ] JSON builders (using `serde_json::json!`):
  - Connection: `{"type":"CONNECT"}`
  - Heartbeat: `{"type":"PING"}`
  - Receiver: `LAUNCH {appId:"CC1AD845"}`, `GET_STATUS`, `SET_VOLUME {volume:{level,muted}}`, `STOP_APP {sessionId}`
  - Media: `LOAD {media:{contentId,contentType,streamType},autoplay,currentTime}`, `PLAY`, `PAUSE`, `STOP`
- [ ] Response parsers: `RECEIVER_STATUS` → `(transportId, sessionId, volume)`, `MEDIA_STATUS` → `(playerState, idleReason)`, `PONG` → heartbeat reset.
- [ ] Source/destination ID table per spec §6.0.
- [ ] `streamType`: `BUFFERED` for file/URL, `LIVE` for screen.
- [ ] **Tests:** monotonic IDs; correlation hit/miss; 5s timeout fires; JSON builders produce exact bytes (snapshot tests); parsers tolerate extra fields.
- [ ] **Gate:** `cargo test --test request_id_tests` green.

### Phase 6 — Connection lifecycle (`cast/connection.rs`) ← `03-cast-engine.md` §7
- [ ] State machine: `Disconnected → Connecting → Connected → Launching → Ready → Streaming → Teardown`.
- [ ] Heartbeat task: PING every 5s; PONG watchdog 10s → teardown + reconnect.
- [ ] Reconnect policy: exponential backoff per `util/retry.rs`, max 5 attempts; surface `ConnectionError` to GUI when exhausted.
- [ ] Inbound JSON router: PONG, RECEIVER_STATUS, MEDIA_STATUS.
- [ ] Public API: `select()`, `launch_default_receiver()`, `load(url, stream_type)`, `play()`, `pause()`, `stop()`, `set_volume(level, muted)`, `shutdown()`.
- [ ] Teardown sequence: `STOP` → `STOP_APP` → `close_notify` → close socket.
- [ ] **Tests:** state transitions with a mock TLS stream; heartbeat watchdog fires; reconnect backoff; teardown ordering.
- [ ] **Gate:** `cargo test cast::connection` green.

### Phase 7 — Media proxy (`media/`) ← `04-media-proxy.md`
- [ ] `mime.rs`: extension map (mp4/webm/mkv/mov/mp3/aac/m4a/flac/wav; default `application/octet-stream`).
- [ ] `range.rs`: parse `bytes=a-b`, `bytes=a-`, `bytes=-suffix`; build `Content-Range`; classify as valid/invalid/multi/none.
- [ ] `lan_ip.rs`: enumerate non-loopback IPv4 interfaces; match subnet containing receiver IP; fallback to default-route interface; fallback to `127.0.0.1` with `warn!`. Re-run on receiver change.
- [ ] `server.rs`: tokio `TcpListener` on `0.0.0.0:8080` (configurable); HTTP/1.1 request line + headers; GET/HEAD only (else 405); route `/stream` only (else 404); rebind on `SetProxyPort`.
- [ ] `local_file.rs`: open `tokio::fs::File`; 200 (full) / 206 (single range) / 416 (unsatisfiable); 64 KiB chunks; `Accept-Ranges`, `Content-Type`, `Content-Length`, `Cache-Control: no-cache`; HEAD = headers only.
- [ ] `url_proxy.rs`: `reqwest::Client` with rustls-tls; reject userinfo URLs; forward `Range`; up to 5 redirects; 30s first-byte timeout; no overall timeout while streaming; pass through non-2xx status + body; 502 on connection failure.
- [ ] `source.rs`: `ActiveSource { File(PathBuf) | Url(String) | Screen(monitor_name) }`; switching terminates in-flight connection via per-connection cancellation token.
- [ ] **Tests:** MIME table; Range parser all cases; LAN IP selection (subnet/default/loopback); HTTP server end-to-end with reqwest client; 404/405/200/206/416; remote proxy 502 + Range forwarding; HEAD behavior; source switch cancels in-flight.
- [ ] **Gate:** `cargo test --test range_tests --test mime_tests --test lan_ip_tests --test integration::http_e2e` green.

### Phase 8 — Screen capture pipeline (`screen/`) ← `05-screen-capture.md`
- [ ] `ffmpeg_discover.rs`: `which::which("ffmpeg")` or `std::env::var("PATH")` scan; cache result; expose `ffmpeg_available() -> bool`.
- [ ] `bgra_rgba.rs`: pure-safe BGRA→RGBA in-place byte shuffle; verify against pinned `xcap` version at impl time.
- [ ] `capture.rs`:
  - `std::thread::spawn` capture loop.
  - xcap `Monitor::from_name(name)`; enumerate names for `DisplaysUpdated`.
  - 30 fps (`std::thread::sleep` to pace).
  - Resolution from xcap; restart ffmpeg on resolution change.
  - Wayland detection → emit error, disable Display source.
  - 5 consecutive failures → stop + emit `StreamError`.
- [ ] `ffmpeg.rs`:
  - Build `Command::new("ffmpeg")` with exact args from spec §4 (rawvideo, rgba, `-s WxH`, `-r 30`, libx264 ultrafast, tune zerolatency, fMP4, `pipe:1`).
  - stdin/stdout piped; stderr captured for diagnostics.
  - Lifecycle: EOF + wait 5s + kill on shutdown; non-zero exit → error.
  - `-movflags frag_keyframe+empty_moov` baseline; record whether `default_base_moof` is needed after receiver validation.
- [ ] `bridge.rs`:
  - Bounded channel cap 2 from capture thread (drop-oldest).
  - Dedicated stdin writer thread; dedicated stdout reader thread.
  - Bounded channel cap 8 from stdout reader to HTTP server (drop-oldest).
- [ ] **Tests:** BGRA→RGBA correctness; drop-oldest behavior; ffmpeg discovery (PATH fixture); lifecycle ordering (EOF → wait → kill) using a fake ffmpeg script.
- [ ] **Gate:** `cargo test --test screen_pipeline_tests --test integration::screen_e2e` green.

### Phase 9 — GUI (`app.rs`) ← `02-gui.md`
- [ ] `CastDashboard` struct per spec §4.2.
- [ ] Left panel (~250 px): receiver list with `Scanning` / `No receivers found` / `Error+retry` states; row = name + IP:port.
- [ ] Center panel: tabbed `Display` / `Local File` / `Web URL`.
  - Display: dropdown from `DisplaysUpdated`; disabled when no monitors or ffmpeg missing.
  - Local File: `rfd::AsyncFileDialog` with media-type filters.
  - Web URL: text input; Apply disabled until `http://` / `https://` absolute URL with host parses.
- [ ] Bottom bar (~48 px): Play / Pause / Stop (disabled per spec rules); Volume slider 0..=100 → 0.0..=1.0; mute toggle; throttled to 1 message / 100 ms; corrected from `BackendEvent::Volume`.
- [ ] Status strip: colored dot (amber/green/red); playback state; transient error banner with manual dismiss.
- [ ] Settings modal: proxy port input validated `1024..=65535`; Save dispatches `SetProxyPort(u16)`.
- [ ] Each frame: `try_recv` drain `event_rx` to exhaustion before rendering.
- [ ] egui default dark theme; no custom skinning.
- [ ] **Tests:** state transitions for all `AppCommand` variants; URL validation; volume throttle timing; status-indicator updates from synthetic `BackendEvent`s.
- [ ] **Gate:** `cargo test --test gui_state_tests` green; manual smoke test on each supported OS.

### Phase 10 — Runtime & supervisor (`runtime.rs`) ← `06-concurrency.md`
- [ ] Build `tokio::runtime::Runtime::new()` (multi-threaded).
- [ ] Spawn Task A (mDNS), Task B (Cast), Task C (HTTP); spawn capture thread via `std::thread::spawn`.
- [ ] Supervisor task owns `Shutdown` token; fatal mDNS or Cast failure → `ConnectionError` + halt dependents.
- [ ] Aggregate backend → GUI through a single `UnboundedSender<BackendEvent>`.
- [ ] On app exit: trigger shutdown → drop runtime → drop TLS socket, HTTP listener, ffmpeg child via `Drop`.
- [ ] **Tests:** shutdown ordering (HTTP stops accepting → Cast closes → mDNS stops → capture thread joins → ffmpeg killed); event aggregation.
- [ ] **Gate:** `cargo test runtime` green; manual: quit app, verify no orphan `ffmpeg` process.

### Phase 11 — Integration tests + CI
- [ ] `tests/integration/http_e2e.rs`: spin server in-process, exercise all Range cases with `reqwest`.
- [ ] `tests/integration/screen_e2e.rs`: dummy rawvideo stdin feeder → ffmpeg → HTTP → reqwest consumer; skip if `ffmpeg` absent.
- [ ] `tests/integration/cast_e2e.rs`: `#[ignore]` tests requiring a real Chromecast; gated behind `--features e2e-cast`.
- [ ] GitHub Actions matrix (ubuntu/windows/macos) running §4 CI gate.
- [ ] Upload `Cargo.lock` artifact on each run.

### Phase 12 — Production hardening
- [ ] `tracing-subscriber` with `env-filter` (`CAST_APP_LOG=info` default).
- [ ] Audit every `unwrap`/`expect`/`panic` in non-init code; convert to `?` + typed error.
- [ ] Backpressure tuning: capture channel cap 2, encoder channel cap 8.
- [ ] Log file at platform log dir (use `std::env::temp_dir()` if nothing better).
- [ ] Release profile: `lto = "thin"`, `codegen-units = 1`, `strip = true`, `panic = "abort"`.
- [ ] Update `README.md` with per-platform build/run/ffmpeg-install instructions.
- [ ] Walk the manual verification script (§12 below) on each supported OS.
- [ ] Tick every acceptance-criteria box in `specs/07-requirements-and-tests.md`.

---

## 7. Coding conventions for agents

- **Every `.rs` file begins with `#![forbid(unsafe_code)]`.** No exceptions.
- **Module-level `//!` docs** state purpose and link to the owning spec section.
- **Public functions carry `///` docs** referencing requirement IDs, e.g. `/// (FR-005) Encode a CastV2 frame.`
- **Errors:** `thiserror` for typed errors inside `cast::`, `media::`, `screen::`; `anyhow` only in `main.rs`/`runtime.rs` glue. No `unwrap`/`expect` outside init code.
- **Async signatures** take a `&Shutdown` (or owned `CancellationToken`) guard where long-running.
- **Channels:**
  - GUI ↔ backend: `tokio::sync::mpsc::unbounded_channel`.
  - Capture → bridge: bounded(2), drop-oldest.
  - Encoder → HTTP: bounded(8), drop-oldest.
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
| `cast/proto.rs`, `cast/framing.rs` | `03-cast-engine.md` §4–5 |
| `cast/request_id.rs`, `cast/namespaces.rs` | `03-cast-engine.md` §6 |
| `cast/connection.rs` | `03-cast-engine.md` §7 |
| `media/*` | `04-media-proxy.md` |
| `screen/*` | `05-screen-capture.md` |
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

- [ ] `#![forbid(unsafe_code)]` compiles on every supported target.
- [ ] `cargo tree` shows zero banned crates (`cargo deny check` green).
- [ ] Every acceptance-criteria checkbox in `specs/02-07-*.md` is checked, with the verifying test artifact referenced in the PR.
- [ ] All seven spec files reviewed against final code; no silent deviations.
- [ ] No `unwrap()`/`expect()` in hot paths; init-only usages documented.
- [ ] No `await` or blocking I/O on the GUI thread (verified by code review + runtime check).
- [ ] Heartbeat watchdog, reconnect backoff, and shutdown ordering covered by tests.
- [ ] HTTP Range correctness verified with `curl` and a real Chromecast on each OS.
- [ ] fMP4 `-movflags` working set validated against a real receiver; result recorded in `screen/ffmpeg.rs` doc comment.
- [ ] CI green on ubuntu-latest, windows-latest, macos-13.
- [ ] `Cargo.lock` committed and reflects pinned versions from §3.2.
- [ ] `ffmpeg` PATH discovery error UX is clear and actionable on each platform.
- [ ] Manual verification script (§11) executed successfully on at least one developer machine per OS.

---

## 11. Manual verification script (run before tagging release)

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all
cargo build --release
./scripts/forbid-unsafe-check.sh
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
- **Protobuf:** the length prefix is part of the **framing**, not the protobuf payload. Encode payload → measure → prepend 4-byte BE length.
- **Heartbeat:** PONG must reset the watchdog, not just be received. If you only set "received any message" you'll miss silent heartbeat failures.
- **HTTP Range:** `bytes=0-` is valid (200 OK or 206 from offset 0); `bytes=-0` is unsatisfiable (416); `bytes=1-0` is malformed (416).
- **URL proxy:** do **not** forward the Chromecast's `User-Agent` or `Host` headers upstream — they will break remote CDNs.
- **Screen capture:** `xcap` returns BGRA on current versions; the spec example uses `-pix_fmt rgba`. Convert before piping, or switch ffmpeg to `-pix_fmt bgra` — pick one and pin it.
- **fMP4:** if the receiver stalls at "Buffering" forever, add `default_base_moof` to `-movflags` and re-test. Record the working flag set in code.
- **Shutdown ordering:** drop the HTTP listener **first** so no new `/stream` connections arrive while the Cast connection is tearing down; otherwise the Chromecast may retry the URL mid-teardown.
- **Tokio + std::thread:** the capture thread is `std::thread`, not a tokio task. Bridge into tokio via a `tokio::sync::mpsc::Sender` created with `unbounded_channel` (or bounded per §7) and `tokio::runtime::Handle::current()` captured before spawn.

---

## 13. Agent workflow checklist (run on every change)

1. Did I add or modify any `.rs` file? Confirm `#![forbid(unsafe_code)]` at the top.
2. Did I add a dependency? Confirm it is not on the ban list and update `deny.toml` if needed.
3. Did I touch Cast protocol code? Add or update a golden-vector unit test.
4. Did I touch HTTP code? Add or update a Range / 404 / 405 / 502 test.
5. Did I touch screen pipeline? Verify backpressure still drops oldest; verify no `xcap` call on GUI/tokio threads.
6. Did I touch the GUI? Verify no new `await` or blocking call; verify `try_recv` drain remains non-blocking.
7. `cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test --all`.
8. `./scripts/forbid-unsafe-check.sh`.
9. Update the relevant spec's acceptance-criteria checkboxes if behavior changed.
10. Conventional-commit message; reference requirement IDs in the body if applicable.

---

End of `AGENTS.md`. Treat it as the source of truth for *how* to build what the
specs describe. When in doubt, defer to the specs; when the specs are silent,
defer to this document.
