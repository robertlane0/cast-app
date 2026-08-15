//! Reader side of the connection (`03-cast-engine.md` §7): the dedicated
//! `std::thread` that owns the blocking read side of the transport, plus the
//! [`FrameAccumulator`] that reassembles transport bytes into complete
//! CastV2 frames.

use std::io;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use crate::cast::connection::transport::{SharedTransport, lock_transport};
use crate::cast::framing::{FrameError, read_frame};

/// Reader read buffer size; frames are accumulated across reads.
const READ_BUFFER_SIZE: usize = 16 * 1024;

/// Idle-read backoff: after a WouldBlock poll the reader sleeps this long
/// before re-locking the transport mutex, so a queued writer (which loses
/// every instant re-lock race — barging) can acquire it deterministically.
const IDLE_READ_BACKOFF: Duration = Duration::from_millis(5);

// ---------------------------------------------------------------------------
// Frame accumulation
// ---------------------------------------------------------------------------

/// Accumulates transport bytes into complete CastV2 frames
/// (`03-cast-engine.md` §4). The socket read is interruptible (timeouts), so
/// partial frames must survive across reads; this buffer holds them.
#[derive(Debug, Default)]
struct FrameAccumulator {
    buf: Vec<u8>,
}

impl FrameAccumulator {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Append bytes and extract every complete frame in order. A trailing
    /// partial frame stays buffered; a length prefix over the maximum is a
    /// protocol error and ends the connection.
    fn push_bytes(&mut self, bytes: &[u8]) -> Result<Vec<Vec<u8>>, FrameError> {
        self.buf.extend_from_slice(bytes);
        let mut frames = Vec::new();
        let mut consumed = 0usize;
        loop {
            let mut cursor = io::Cursor::new(&self.buf[consumed..]);
            match read_frame(&mut cursor) {
                Ok(Some(payload)) => {
                    consumed += cursor.position() as usize;
                    frames.push(payload);
                }
                Ok(None) | Err(FrameError::Truncated { .. }) => break,
                Err(error) => return Err(error),
            }
        }
        self.buf.drain(..consumed);
        Ok(frames)
    }
}

// ---------------------------------------------------------------------------
// Reader thread
// ---------------------------------------------------------------------------

/// The reader thread: owns the blocking read side of the transport and feeds
/// decoded frames to the run task. Sends `None` once, on exit. Runs on a
/// dedicated `std::thread` — never on a tokio worker (`03-cast-engine.md`
/// §7; Phase 3 lesson).
pub(super) fn spawn_reader(
    transport: SharedTransport,
    frame_tx: mpsc::UnboundedSender<Option<Vec<u8>>>,
    shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    std::thread::Builder::new()
        .name("cast-reader".into())
        .spawn(move || reader_loop(transport, frame_tx, shutdown_rx))
        .map(|_| ())
}

fn reader_loop(
    transport: SharedTransport,
    frame_tx: mpsc::UnboundedSender<Option<Vec<u8>>>,
    shutdown_rx: watch::Receiver<bool>,
) {
    let mut accumulator = FrameAccumulator::new();
    let mut buffer = [0u8; READ_BUFFER_SIZE];

    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        let read_result = {
            let mut guard = lock_transport(&transport);
            guard.read(&mut buffer)
        };
        match read_result {
            // Clean EOF (close_notify) or connection reset: the session is
            // over; the run task decides whether to reconnect.
            Ok(0) => break,
            Ok(n) => {
                match accumulator.push_bytes(&buffer[..n]) {
                    Ok(frames) => {
                        for frame in frames {
                            if frame_tx.send(Some(frame)).is_err() {
                                return; // run task gone
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "protocol error while reading frames; closing connection");
                        break;
                    }
                }
                // Yield the transport mutex to queued writers after *every*
                // read cycle, not just idle polls. With continuous inbound
                // traffic the read never blocks, so without this sleep a
                // blocked writer would starve indefinitely (barging; see
                // the WouldBlock arm below).
                std::thread::sleep(IDLE_READ_BACKOFF);
            }
            // Read timeout / would-block: poll shutdown state and retry.
            //
            // Sleep before re-locking: the reader re-acquires the transport
            // mutex within microseconds of an idle poll, which starves
            // concurrent writers (mutex barging) — a blocked writer loses
            // the race to the instantly re-locking reader every cycle. A
            // short sleep opens a scheduling window the writer always wins.
            // Writers are commands/PINGs (human-scale or 5s cadence), so the
            // added latency is irrelevant; reads stay bounded by the socket
            // timeout.
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut =>
            {
                std::thread::sleep(IDLE_READ_BACKOFF);
            }
            Err(error) => {
                tracing::debug!(%error, "transport read ended");
                break;
            }
        }
    }

    let _ = frame_tx.send(None);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cast::framing::{MAX_FRAME_SIZE, encode_frame};

    #[test]
    fn frame_accumulator_handles_partial_and_multiple_frames() {
        let mut accumulator = FrameAccumulator::new();
        let frame_one = encode_frame(b"hello");
        let frame_two = encode_frame(b"world");

        let mut partial = Vec::new();
        partial.extend_from_slice(&frame_one[..3]);
        assert!(
            accumulator
                .push_bytes(&partial)
                .expect("no error")
                .is_empty()
        );

        let mut rest = Vec::new();
        rest.extend_from_slice(&frame_one[3..]);
        rest.extend_from_slice(&frame_two);
        let frames = accumulator.push_bytes(&rest).expect("no error");
        assert_eq!(frames, vec![b"hello".to_vec(), b"world".to_vec()]);

        assert!(accumulator.push_bytes(&[]).expect("no error").is_empty());
    }

    #[test]
    fn frame_accumulator_rejects_oversized_prefix() {
        let mut accumulator = FrameAccumulator::new();
        let mut oversized = Vec::new();
        oversized.extend_from_slice(&(MAX_FRAME_SIZE as u32 + 1).to_be_bytes());
        let error = accumulator
            .push_bytes(&oversized)
            .expect_err("size limit enforced");
        assert!(matches!(error, FrameError::FrameTooLarge(_)));
    }
}
