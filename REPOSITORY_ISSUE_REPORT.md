## Issues

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
