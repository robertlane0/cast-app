// SPDX-License-Identifier: MIT OR Apache-2.0
//! Encoder stdout readers (`05-screen-capture.md` §5–§6): one thread per
//! encoder generation cuts the encoded output at ISO-BMFF fragment
//! boundaries (`screen::segments`) and pushes the resulting whole segments
//! into the cap-8 output queue. The push policy protects the init segment
//! from drop-oldest eviction: a full queue evicts only complete fragments,
//! so a slow consumer loses whole play intervals, never initialization
//! bytes.

use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::screen::segments::{EncodedSegment, Mp4Segmenter};
use crate::util::backpressure::BoundedDropOldest;

use super::lock;

/// Encoder → HTTP output queue capacity, measured in encoded segments
/// (AGENTS.md §7; drop-oldest evicts whole media fragments, never byte
/// slices). With the configured 1 s keyframe interval a segment is ~1 s of
/// video.
pub const OUTPUT_QUEUE_CAPACITY: usize = 8;

/// Chunk size used by the stdout reader (64 KiB, same as the media server).
const STDOUT_CHUNK: usize = 64 * 1024;

/// Read an encoder's stdout to EOF, cutting the bytes at decoder-safe
/// ISO-BMFF boundaries and pushing the resulting segments into `output`.
/// Extracted from the reader thread so the segmentation + push pipeline can
/// be driven by any `Read` in unit tests (a `Cursor` over a synthetic fMP4
/// stream).
fn read_encoded<R: Read>(
    mut reader: R,
    output: &BoundedDropOldest<EncodedSegment>,
) -> io::Result<()> {
    let mut chunk = vec![0u8; STDOUT_CHUNK];
    let mut segmenter = Mp4Segmenter::new();
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                for segment in segmenter.feed(&chunk[..read]) {
                    push_segment(output, segment);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    for segment in segmenter.finish() {
        push_segment(output, segment);
    }
    Ok(())
}

/// Spawn the stdout reader thread for one encoder generation: reads encoded
/// bytes, cuts them at decoder-safe boundaries, and pushes whole segments
/// into the cap-8 output queue until EOF. One thread per encoder generation.
pub(super) fn spawn_stdout_reader(
    stdout: std::process::ChildStdout,
    output: Arc<BoundedDropOldest<EncodedSegment>>,
    reader_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
) {
    let reader = std::thread::Builder::new()
        .name("ffmpeg-stdout".to_string())
        .spawn(move || {
            if let Err(error) = read_encoded(stdout, &output) {
                tracing::warn!(%error, "encoder stdout read failed");
            }
            tracing::debug!("encoder stdout reader finished");
        });
    if let Ok(reader) = reader {
        // Reap finished readers from earlier encoder generations before
        // accumulating the new handle (ISS-006).
        register_reader(&reader_handles, reader);
    } else {
        tracing::warn!("failed to spawn the encoder stdout reader");
    }
}

/// Reap finished readers from earlier encoder generations before
/// accumulating the new handle (ISS-006). A plain function so the
/// reap-before-push bookkeeping is unit-testable without a subprocess.
fn register_reader(reader_handles: &Mutex<Vec<JoinHandle<()>>>, reader: JoinHandle<()>) {
    let mut handles = lock(reader_handles);
    handles.retain(|handle| !handle.is_finished());
    handles.push(reader);
}

/// Push one encoded segment into the output queue. When the queue is full,
/// the oldest *fragment* is evicted — the init segment is protected, since
/// every following fragment depends on it. If the queue holds only protected
/// segments (pathological), the newest segment is dropped instead: a
/// whole-segment skip, never corrupt bytes.
fn push_segment(output: &BoundedDropOldest<EncodedSegment>, segment: EncodedSegment) {
    let evictable = |buffered: &EncodedSegment| matches!(buffered, EncodedSegment::Fragment(_));
    if let Some(rejected) = output.push_or(segment, evictable) {
        tracing::warn!(
            bytes = rejected.len(),
            "dropping encoded segment; queue holds only the protected init"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build `box <kind>(payload)` ISO-BMFF bytes (same helper as the
    /// `segments` module's own tests).
    fn boxed(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + payload.len());
        bytes.extend_from_slice(&((8 + payload.len()) as u32).to_be_bytes());
        bytes.extend_from_slice(kind);
        bytes.extend_from_slice(payload);
        bytes
    }

    /// A synthetic fMP4 stream: init (ftyp, moov), then N moof+mdat
    /// fragments.
    fn fmp4_stream(fragments: usize) -> Vec<u8> {
        let mut stream = Vec::new();
        stream.extend(boxed(b"ftyp", b"isom"));
        stream.extend(boxed(b"moov", &[0x11u8; 32]));
        for i in 0..fragments {
            stream.extend(boxed(b"moof", &[0x22u8; 16]));
            let marker = i as u32;
            stream.extend(boxed(b"mdat", &marker.to_be_bytes().repeat(16)));
        }
        stream
    }

    /// `read_encoded` cuts the encoder's stdout into whole segments: the
    /// init first, then one fragment per moof+mdat pair, in order.
    #[test]
    fn read_encoded_cuts_a_synthetic_fmp4_stream_into_whole_segments() {
        let output = BoundedDropOldest::new(OUTPUT_QUEUE_CAPACITY);
        read_encoded(Cursor::new(fmp4_stream(3)), &output).expect("a whole stream reads cleanly");
        assert_eq!(output.len(), 4, "init + 3 fragments");
        assert!(matches!(output.try_pop(), Some(EncodedSegment::Init(_))));
        for _ in 0..3 {
            assert!(matches!(
                output.try_pop(),
                Some(EncodedSegment::Fragment(_))
            ));
        }
        assert!(output.is_empty());
    }

    /// A truncated final fragment (the stream ends mid-mdat) is dropped at
    /// EOF rather than emitted as corrupt bytes.
    #[test]
    fn read_encoded_drops_a_truncated_fragment_tail_at_eof() {
        let stream = fmp4_stream(2);
        let cut = &stream[..stream.len() - 10];
        let output = BoundedDropOldest::new(OUTPUT_QUEUE_CAPACITY);
        read_encoded(Cursor::new(cut.to_vec()), &output)
            .expect("the truncated stream reads cleanly");
        assert_eq!(output.len(), 2, "init + first complete fragment only");
    }

    /// The module's specific usage pattern for `push_segment`: overflow
    /// evicts the oldest *fragment* while the protected init stays at the
    /// head of the queue.
    #[test]
    fn push_segment_evicts_only_fragments_and_protects_the_init() {
        let output = BoundedDropOldest::new(4);
        push_segment(&output, EncodedSegment::Init(vec![0u8; 4]));
        for i in 0..3 {
            push_segment(&output, EncodedSegment::Fragment(vec![i as u8; 4]));
        }
        // The queue is full [init, f0, f1, f2]; the new fragment evicts f0.
        push_segment(&output, EncodedSegment::Fragment(vec![9u8; 4]));
        assert_eq!(output.len(), 4);
        assert!(
            matches!(output.try_pop(), Some(EncodedSegment::Init(_))),
            "the init must never be evicted"
        );
        assert_eq!(
            output.try_pop(),
            Some(EncodedSegment::Fragment(vec![1u8; 4]))
        );
        assert_eq!(
            output.try_pop(),
            Some(EncodedSegment::Fragment(vec![2u8; 4]))
        );
        assert_eq!(
            output.try_pop(),
            Some(EncodedSegment::Fragment(vec![9u8; 4]))
        );
    }

    /// Pathological queue of only protected segments: the new segment is
    /// dropped whole rather than evicting the init (a whole-segment skip,
    /// never corrupt bytes).
    #[test]
    fn push_segment_drops_the_newest_when_only_inits_are_buffered() {
        let output = BoundedDropOldest::new(3);
        for _ in 0..3 {
            push_segment(&output, EncodedSegment::Init(vec![0u8; 4]));
        }
        push_segment(&output, EncodedSegment::Fragment(vec![1u8; 4]));
        assert_eq!(output.len(), 3, "nothing was evicted");
        while let Some(segment) = output.try_pop() {
            assert!(matches!(segment, EncodedSegment::Init(_)));
        }
    }

    /// Reader-handle bookkeeping: every registration reaps readers that
    /// finished on their own (a crashed encoder EOFs its reader), so only
    /// live handles accumulate (ISS-006).
    #[test]
    fn register_reader_reaps_finished_generations() {
        let handles = Mutex::new(Vec::new());
        for _ in 0..3 {
            let handle = std::thread::spawn(|| {});
            while !handle.is_finished() {
                std::thread::yield_now();
            }
            // The earlier finished readers are reaped by this registration;
            // only the newest finished handle can remain.
            register_reader(&handles, handle);
        }
        assert_eq!(handles.lock().unwrap().len(), 1);
        // A live reader (still blocked) survives the reap on the next
        // registration.
        let (tx, rx) = std::sync::mpsc::channel();
        let live = std::thread::spawn(move || {
            let _ = rx.recv();
        });
        register_reader(&handles, live);
        assert_eq!(
            handles.lock().unwrap().len(),
            1,
            "the live reader must survive the reap"
        );
        let mut guard = handles.lock().unwrap();
        let handle = guard.pop().unwrap();
        drop(guard);
        tx.send(()).unwrap();
        handle.join().unwrap();
    }
}
