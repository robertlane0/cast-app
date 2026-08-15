//! Write path of the connection (`03-cast-engine.md` §7): framed payload
//! writes on `spawn_blocking` workers (blocking socket), with a polling
//! `try_lock` that cannot be starved by the reader thread.

use std::io;
use std::time::Duration;

use crate::cast::connection::transport::SharedTransport;
use crate::cast::framing::encode_frame;

/// Retry cadence of the polling `try_lock` writer. A blocked mutex waiter
/// can be starved by a reader that re-locks microseconds after every
/// release; a polling writer wins a window the moment the reader yields.
const WRITER_LOCK_RETRY: Duration = Duration::from_millis(1);

/// Write one framed payload on a `spawn_blocking` worker (blocking socket).
///
/// Never blocks on the transport mutex. A reader that re-locks within
/// microseconds of every release starves a blocked mutex waiter — the
/// waiter loses the unlock→re-lock race each cycle. Instead, poll
/// `try_lock` on a short cadence: the writer wins the moment the reader
/// yields (the reader sleeps `IDLE_READ_BACKOFF` after every read cycle,
/// so a window opens at most every few milliseconds).
pub(super) async fn send_payload(transport: &SharedTransport, payload: Vec<u8>) -> io::Result<()> {
    let transport = transport.clone();
    let result = tokio::task::spawn_blocking(move || {
        let bytes = encode_frame(&payload);
        loop {
            match transport.try_lock() {
                Ok(mut guard) => return guard.write_all(&bytes),
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::sleep(WRITER_LOCK_RETRY);
                }
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    return error.into_inner().write_all(&bytes);
                }
            }
        }
    })
    .await;
    match result {
        Ok(inner) => inner,
        Err(_) => Err(io::Error::other("frame writer worker panicked")),
    }
}
