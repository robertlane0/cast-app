## Issues

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
