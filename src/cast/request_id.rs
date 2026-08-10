#![forbid(unsafe_code)]

//! Monotonic `requestId` allocation and the pending-request map with
//! per-entry timeouts. Owned by `03-cast-engine.md` §6.
