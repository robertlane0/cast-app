#![forbid(unsafe_code)]

//! Full Cast connection lifecycle: connect, launch, heartbeat watchdog,
//! inbound routing, teardown ordering, and reconnect policy.
//! Owned by `03-cast-engine.md` §7.
