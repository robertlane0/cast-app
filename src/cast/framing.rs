#![forbid(unsafe_code)]

//! CastV2 wire framing: a 4-byte big-endian payload length followed by the
//! CastMessage protobuf payload (`03-cast-engine.md` §4).

use std::io::{self, Read, Write};

use thiserror::Error;

/// Maximum accepted frame payload size in bytes (`03-cast-engine.md` §4.2).
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Errors produced while framing or deframing CastV2 messages.
#[derive(Debug, Error)]
pub enum FrameError {
    #[error("frame header or payload I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("frame length prefix {0} exceeds the maximum of {MAX_FRAME_SIZE} bytes")]
    FrameTooLarge(u64),
    #[error("stream ended mid-frame: expected {expected} more bytes, got {got}")]
    Truncated { expected: usize, got: usize },
}

/// Encode a payload into a frame: 4-byte big-endian length + payload.
pub fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Append a full frame to `writer` in a single `write_all` call
/// (`03-cast-engine.md` §4.1).
pub fn write_frame(writer: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    writer.write_all(&encode_frame(payload))
}

/// Read one frame from `reader` (`03-cast-engine.md` §4.2): a 4-byte
/// big-endian length followed by exactly that many bytes.
///
/// A clean EOF at a frame boundary returns `Ok(None)`; EOF in the middle of a
/// header or payload is `Truncated`. Length prefixes above
/// [`MAX_FRAME_SIZE`] are a protocol error and close the connection.
pub fn read_frame(reader: &mut impl Read) -> Result<Option<Vec<u8>>, FrameError> {
    let mut header = [0u8; 4];
    let (eof, got) = read_exact_or_eof(reader, &mut header)?;
    if eof {
        return if got == 0 {
            Ok(None) // clean EOF at a frame boundary
        } else {
            Err(FrameError::Truncated { expected: 4, got })
        };
    }

    let length = u32::from_be_bytes(header) as u64;
    if length as usize > MAX_FRAME_SIZE {
        return Err(FrameError::FrameTooLarge(length));
    }

    let mut payload = vec![0u8; length as usize];
    let (eof, got) = read_exact_or_eof(reader, &mut payload)?;
    if eof {
        return Err(FrameError::Truncated {
            expected: payload.len(),
            got,
        });
    }
    Ok(Some(payload))
}

/// Read exactly `buf.len()` bytes if possible. Returns `(true, n)` if EOF
/// arrives after `n` bytes were filled (no error path), or `(false, n)` when
/// the buffer was fully filled.
fn read_exact_or_eof(
    reader: &mut impl Read,
    mut buf: &mut [u8],
) -> Result<(bool, usize), io::Error> {
    let mut filled = 0;
    while !buf.is_empty() {
        match reader.read(buf) {
            Ok(0) => return Ok((true, filled)),
            Ok(n) => {
                filled += n;
                buf = &mut buf[n..];
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok((false, filled))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let payload = b"{\"type\":\"PING\"}";
        let framed = encode_frame(payload);
        // 4-byte big-endian length prefix.
        assert_eq!(&framed[..4], &15u32.to_be_bytes());
        let mut reader = framed.as_slice();
        assert_eq!(
            read_frame(&mut reader).expect("frame reads"),
            Some(payload.to_vec())
        );
        // Consumed exactly; the next read is a clean EOF.
        assert_eq!(read_frame(&mut reader).expect("eof"), None);
    }

    #[test]
    fn empty_payload_frame_round_trips() {
        let framed = encode_frame(b"");
        assert_eq!(
            read_frame(&mut framed.as_slice()).expect("empty frame"),
            Some(vec![])
        );
    }

    #[test]
    fn over_maximum_length_prefix_is_rejected() {
        // (FR-018) A length prefix above 16 MiB is a protocol error.
        let mut framed = Vec::new();
        framed.extend_from_slice(&(MAX_FRAME_SIZE as u32 + 1).to_be_bytes());
        framed.extend_from_slice(&[0u8; 8]);
        let result = read_frame(&mut framed.as_slice());
        assert!(
            matches!(result, Err(FrameError::FrameTooLarge(len)) if len == MAX_FRAME_SIZE as u64 + 1)
        );
    }

    #[test]
    fn truncated_header_and_payload_are_errors() {
        let mut short_header = Vec::new();
        short_header.extend_from_slice(&[0x00, 0x00, 0x04]);
        assert!(matches!(
            read_frame(&mut short_header.as_slice()),
            Err(FrameError::Truncated { .. })
        ));

        let mut short_payload = encode_frame(b"0123456789");
        short_payload.truncate(4 + 5);
        assert!(matches!(
            read_frame(&mut short_payload.as_slice()),
            Err(FrameError::Truncated { .. })
        ));
    }

    #[test]
    fn multiple_frames_in_one_stream() {
        let mut stream = Vec::new();
        let payloads: Vec<&[u8]> = vec![b"aa", b"bbbb", b"cccccccc"];
        for payload in &payloads {
            stream.extend_from_slice(&encode_frame(payload));
        }
        let mut reader = stream.as_slice();
        for payload in &payloads {
            assert_eq!(
                read_frame(&mut reader).expect("frame"),
                Some(payload.to_vec())
            );
        }
        assert_eq!(read_frame(&mut reader).expect("clean eof"), None);
    }

    #[test]
    fn write_frame_emits_one_frame() {
        let mut out = Vec::new();
        write_frame(&mut out, b"hello").expect("write");
        let mut reader = out.as_slice();
        assert_eq!(
            read_frame(&mut reader).expect("frame"),
            Some(b"hello".to_vec())
        );
    }
}
