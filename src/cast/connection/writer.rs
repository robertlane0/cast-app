// SPDX-License-Identifier: MIT OR Apache-2.0
//! Write path of the connection (`03-cast-engine.md` §7): framed payload
//! writes on `spawn_blocking` workers (blocking socket). The transport is
//! protected by a fair `parking_lot::Mutex`, so a writer queued while the
//! reader holds the lock is guaranteed to acquire before the reader's next
//! re-lock — no polling `try_lock` workaround is needed.

use std::io;

use crate::cast::connection::transport::SharedTransport;
use crate::cast::framing::encode_frame;

/// Write one framed payload on a `spawn_blocking` worker (blocking socket).
///
/// Blocks on the fair transport mutex. With `parking_lot::Mutex` the waiter
/// is queued FIFO, so a continuous inbound stream cannot starve the writer:
/// the writer was enqueued while the reader held the lock and therefore
/// acquires before the reader re-locks.
pub(super) async fn send_payload(transport: &SharedTransport, payload: Vec<u8>) -> io::Result<()> {
    let transport = transport.clone();
    let result = tokio::task::spawn_blocking(move || {
        let bytes = encode_frame(&payload);
        let mut guard = transport.lock();
        guard.write_all(&bytes)
    })
    .await;
    match result {
        Ok(inner) => inner,
        Err(_) => Err(io::Error::other("frame writer worker panicked")),
    }
}
