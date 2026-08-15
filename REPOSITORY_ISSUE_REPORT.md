## Issues

### ISS-004 · 🟡 Medium · Security — URL proxy has no SSRF protection for private IPs

**File:** [`src/media/url_proxy.rs`](./cast-app/src/media/url_proxy.rs#L56-L65)  
**Evidence:**
```rust
// src/media/url_proxy.rs:56-65
pub fn validate_url(&self, raw: &str) -> Result<reqwest::Url, ProxyError> {
    let url = reqwest::Url::parse(raw)?;
    if !url.username().is_empty() || url.password().is_some() { return Err(ProxyError::Userinfo); }
    if url.host_str().is_none() { return Err(ProxyError::MissingHost); }
    Ok(url)
}
```

**Description:** URL validation rejects userinfo and missing hosts, but does not block private/link-local/loopback IP ranges (e.g., `http://169.254.169.254/`, `http://127.0.0.1:8080/`, `http://[::1]/`). A user (or a crafted URL) could use the proxy to reach cloud metadata services or internal services.

**Root Cause:** The spec only mandates userinfo rejection. The threat model assumes a trusted local user, but the proxy is accessible from the LAN (ISS-002).

**Recommended Fix:** Add hostname/IP validation that rejects private RFC 1918, link-local, loopback, and cloud metadata ranges. Alternatively, document this as a known limitation.

---

### ISS-005 · 🟡 Medium · Correctness — GUI busy-loops while file picker is open

**File:** [`src/app.rs`](./cast-app/src/app.rs#L447-L449)  
**Evidence:**
```rust
// src/app.rs:447-449
Poll::Pending => {
    ctx.request_repaint();
}
```

**Description:** When the `rfd` file picker is pending, `ctx.request_repaint()` is called, triggering an immediate next frame. Since the picker waits for user interaction, this causes the GUI thread to spin at maximum FPS, spiking CPU usage until the dialog is closed.

**Root Cause:** A noop waker is used (correct for the GUI thread), but the repaint request should be delayed.

**Recommended Fix:** Replace `ctx.request_repaint()` with `ctx.request_repaint_after(Duration::from_millis(200))` to match the existing `REPAINT_INTERVAL`.

---

### ISS-006 · 🟡 Medium · Correctness — Reader thread JoinHandles accumulate unboundedly

**File:** [`src/screen/bridge.rs`](./cast-app/src/screen/bridge.rs#L486)  
**Evidence:**
```rust
lock(&reader_handles).push(reader);
```

**Description:** When the monitor resolution changes, the controller restarts ffmpeg and spawns a new stdout reader thread, pushing its handle into `reader_handles`. Old reader threads exit when their stdout EOF arrives, but their `JoinHandle` is never reaped until `ScreenBridge::join()`. Frequent resolution changes (e.g., docking/undocking a laptop) cause unbounded growth.

**Root Cause:** No periodic cleanup of finished handles.

**Recommended Fix:** Before pushing a new handle, drain finished handles from the vector:
```rust
handles.retain(|h| !h.is_finished());
handles.push(reader);
```

---

### ISS-007 · 🟡 Medium · Correctness — Capture failure counter conflates reacquire and capture errors

**File:** [`src/screen/capture.rs`](./cast-app/src/screen/capture.rs#L232-L260)  
**Evidence:**
```rust
// L232-234: Successful capture resets failures to 0
Ok(frame) => { failures = 0; ... }
// L259-260: Reacquire failure increments
if let Err(error) = source.reacquire() { failures += 1; ... }
```

**Description:** The `MAX_CONSECUTIVE_FAILURES` (5) counter is shared between capture and reacquire errors. A reacquire failure increments `failures`, but a subsequent successful `capture_frame()` on the old handle resets it to 0, masking persistent monitor reconnection failures.

**Root Cause:** Single counter for two independent failure domains.

**Recommended Fix:** Track reacquire failures independently, or only reset `failures` to 0 after both capture and reacquire succeed.

---

### ISS-008 · 🟡 Medium · Dependencies — `cargo-deny` missing `[sources]` enforcement

**File:** [`deny.toml`](./cast-app/deny.toml)  
**Evidence:** No `[sources]` section present.

**Description:** Without `[sources]` configuration, `cargo deny` does not reject dependencies from unknown registries or ad-hoc git repositories. A compromised or typosquatted dependency from a third-party registry would not be flagged.

**Root Cause:** Omission during initial configuration.

**Recommended Fix:**
```toml
[sources]
unknown-registry = "deny"
unknown-git = "deny"
```

---

### ISS-009 · 🟡 Medium · Dependencies — `deny.toml` missing duplicate-version policy

**File:** [`deny.toml`](./cast-app/deny.toml)  
**Evidence:** `[bans]` section has no `multiple-versions` key. `cargo tree --duplicates` shows multiple duplicates (`bitflags`, `calloop`, `drm`, `rustix`, `zvariant_utils`, etc.).

**Description:** AGENTS.md §4 mandates reviewing duplicate deps before merge, but `cargo deny check` does not enforce this — duplicates silently pass. The existing duplicates inflate compile time and binary size.

**Root Cause:** Omission; most duplicates are transitive from the GUI/accessibility stack.

**Recommended Fix:**
```toml
[bans]
multiple-versions = "warn"   # or "deny" once cleaned up
```

---

### ISS-010 · 🟡 Medium · Dependencies — Cargo.toml version drift from AGENTS.md spec

**File:** [`Cargo.toml`](./cast-app/Cargo.toml)  
**Evidence:**
| Dependency | AGENTS.md spec | Cargo.toml actual |
|---|---|---|
| `eframe` | `0.29` | `0.36` |
| `egui` | `0.29` | `0.36` |
| `rfd` | `0.15` | `0.17` |
| `xcap` | `0.4` | `0.9` |
| `thiserror` | `1` → now `2.x` available | `1` (OK) |

**Description:** The implementation used newer versions than the spec prescribed. While this is documented in AGENTS.md lessons, the spec section (§3.2) was never updated to match, creating a confusing discrepancy for new contributors.

**Root Cause:** Implementation discovered that newer versions were needed (eframe 0.36 API changes documented in Phase 9 lessons).

**Recommended Fix:** Update `AGENTS.md` §3.2 to reflect the actual pinned versions.

---

### ISS-011 · 🟡 Medium · Correctness — `#![forbid(unsafe_code)]` in non-crate-root files

**File:** All 30 `.rs` files in `src/`  
**Evidence:** Every module file has `#![forbid(unsafe_code)]` at line 1.

**Description:** Inner attributes (`#![...]`) are valid only in crate root files (`lib.rs`, `main.rs`) and module files when placed at the very top. In Rust edition 2024, these are technically warnings turned into future-compatibility lint issues. The attribute is redundant with the crate-root `#![forbid(unsafe_code)]` in `lib.rs` and `main.rs` which already covers all modules.

**Root Cause:** Defense-in-depth strategy from the spec, but creates noise and is fragile under future Rust editions.

**Recommended Fix:** Keep `#![forbid(unsafe_code)]` only in `src/lib.rs` and `src/main.rs`; remove from all other files. The `forbid-unsafe-check.sh` script and `xtask` already enforce this policy externally. (Since clippy passes clean today, the current edition accepts these, but this is a maintainability concern for future editions.)

---

### ISS-012 · 🟡 Medium · Testing — Screen pipeline tests are Unix-only

**File:** [`tests/screen_pipeline_tests.rs`](./cast-app/tests/screen_pipeline_tests.rs)  
**Evidence:** `#![cfg(unix)]` at line 9.

**Description:** All 5 screen pipeline tests are compiled out on Windows CI. The fake encoders rely on `/bin/sh`, POSIX file descriptors, and `kill` signals. This means the screen bridge has zero test coverage on Windows, yet Windows is a supported platform.

**Root Cause:** The test harness uses shell scripts as fake encoders, which are inherently Unix-specific.

**Recommended Fix:** Create a small compiled Rust binary as the fake encoder (reads stdin, writes stdout, respects EOF), which would work cross-platform. Or write Windows-specific fake encoder `.bat` scripts with a `#[cfg(windows)]` variant.

---

### ISS-013 · 🔵 Low · Documentation — CI comment contradicts apt-get step

**File:** [`.github/workflows/ci.yml`](./cast-app/.github/workflows/ci.yml#L5-L6)  
**Evidence:**
```yaml
# Line 5-6: "Linux needs no apt packages..."
# Line 44-47: sudo apt-get install -y libgl1-mesa-dev libegl1-mesa-dev ...
```

**Description:** The header comment at lines 5-6 says "Linux needs no apt packages" but lines 44-47 immediately install five development libraries. This is misleading and confusing.

**Root Cause:** Comment was written before the build requirements were understood; never updated.

**Recommended Fix:** Update the comment to reflect reality, e.g. "Linux needs OpenGL/EGL and PipeWire development headers for the GUI and screen capture stacks."

---

### ISS-014 · 🔵 Low · Performance — MIME lookup allocates on every call

**File:** [`src/media/mime.rs`](./cast-app/src/media/mime.rs)  
**Evidence:** `let lower = extension.to_ascii_lowercase();` allocates a new `String` on every MIME lookup just for case-insensitive comparison.

**Root Cause:** Simple implementation; not performance-critical (called once per `/stream` request).

**Recommended Fix:** Use `eq_ignore_ascii_case()` in the iterator search instead of pre-lowercasing.

---

### ISS-015 · 🔵 Low · Architecture — `xtask` binary never executed in CI

**File:** [`xtask/forbid_unsafe.rs`](./cast-app/xtask/forbid_unsafe.rs)  
**Evidence:** CI runs `scripts/forbid-unsafe-check.sh` but never `cargo run -p xtask`.

**Description:** The `xtask` binary is a Rust reimplementation of the shell unsafe check, but it is never invoked in CI. This is dead code in the CI pipeline.

**Root Cause:** The shell script was sufficient; the xtask was scaffolded per AGENTS.md but never wired in.

**Recommended Fix:** Either add `cargo run -p xtask` to CI, or remove the xtask and document the shell script as the canonical unsafe check.

---

### ISS-016 · 🔵 Low · Architecture — `xtask` sensitive to working directory

**File:** [`xtask/forbid_unsafe.rs`](./cast-app/xtask/forbid_unsafe.rs)  
**Evidence:** Uses relative paths `"src"`, `"tests"`, `"xtask"` against `std::env::current_dir()`.

**Description:** Running `cargo run -p xtask` from the `xtask/` subdirectory or any directory other than the repository root will fail with "No such file or directory".

**Root Cause:** No anchor to `CARGO_MANIFEST_DIR` or repository root detection.

**Recommended Fix:** Use `Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()` to anchor to the repo root.

---

### ISS-017 · 🔵 Low · Maintainability — `forbid-unsafe-check.sh` missing `set -eo pipefail`

**File:** [`scripts/forbid-unsafe-check.sh`](./cast-app/scripts/forbid-unsafe-check.sh#L5)  
**Evidence:** `set -u` only; missing `set -e` and `set -o pipefail`.

**Description:** Without `set -e`, intermediate command failures in the script are silently swallowed. Without `set -o pipefail`, a failure in the first command of a pipe is masked by the success of the second.

**Root Cause:** Minimal shell hardening.

**Recommended Fix:** Change to `set -euo pipefail`.

---

### ISS-018 · 🔵 Low · Maintainability — `connection.rs` is 1,774 lines

**File:** [`src/cast/connection.rs`](./cast-app/src/cast/connection.rs)  
**Evidence:** 1,774 lines, ~69 KB.

**Description:** This single file contains the connection state machine, transport abstraction, reader thread, write path, inbound routing, command dispatch, teardown logic, reconnect policy, the full run loop, the public `CastConnection` handle, and ~440 lines of `#[cfg(test)]` mock infrastructure. This is the largest file in the codebase by a factor of 3x and is difficult to review or modify.

**Root Cause:** Organic growth through phases 3-6.

**Recommended Fix:** Extract into sub-modules: `transport.rs` (trait + impl), `reader.rs`, `writer.rs`, `state_machine.rs`, `teardown.rs`. Tests could move to a dedicated `tests/` file (some already exist in `tests/connection_tests.rs`).

**Risks:** Large refactor; should be done in a dedicated commitf with no functional changes.

---
