// SPDX-License-Identifier: MIT OR Apache-2.0
//! Decoder-safe segmentation of the encoder's fMP4 stdout stream
//! (`05-screen-capture.md` §5–§6).
//!
//! The bridge's drop-oldest backpressure must discard *whole* media
//! fragments, never arbitrary byte slices: a falling-behind consumer that
//! receives a stream with truncated MP4 boxes fails to decode until the
//! whole stream is restarted, while skipping complete fragments only loses
//! the corresponding play time. [`Mp4Segmenter`] cuts the raw stdout bytes at
//! ISO-BMFF box boundaries — the stream initialization (ftyp/moov) and one
//! complete `moof`+`mdat` fragment each — and streams that are not
//! box-structured fall back to whole-read segmentation so no box can ever
//! be split.

use std::mem;

/// Upper bound for a single ISO-BMFF box buffered for parsing. Real
/// fragments at the configured 1 s keyframe interval are a few MiB at most;
/// anything larger triggers the opaque fallback so memory stays bounded.
const MAX_BOX_BYTES: usize = 64 * 1024 * 1024;

/// One self-contained unit of the encoder's output
/// (`05-screen-capture.md` §6: "drop the oldest complete segment, never an
/// arbitrary byte slice").
#[derive(Clone, PartialEq, Eq)]
pub enum EncodedSegment {
    /// The stream initialization (`ftyp`, `moov` and any leading boxes),
    /// written once per encoder generation. Every following fragment
    /// depends on it, so it is protected from drop-oldest eviction.
    Init(Vec<u8>),
    /// One complete media fragment: a `moof` box followed by its `mdat`
    /// box. Independently decodable, so it can be dropped as a whole
    /// without corrupting the rest of the stream.
    Fragment(Vec<u8>),
}

impl EncodedSegment {
    /// The raw bytes of this segment (init or fragment).
    pub fn bytes(&self) -> &[u8] {
        match self {
            EncodedSegment::Init(bytes) | EncodedSegment::Fragment(bytes) => bytes,
        }
    }

    /// The raw bytes of this segment, consuming it.
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            EncodedSegment::Init(bytes) | EncodedSegment::Fragment(bytes) => bytes,
        }
    }

    /// Number of bytes in the segment.
    pub fn len(&self) -> usize {
        self.bytes().len()
    }

    /// Whether the segment carries no bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl std::fmt::Debug for EncodedSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodedSegment::Init(bytes) => f.debug_tuple("Init").field(&bytes.len()).finish(),
            EncodedSegment::Fragment(bytes) => {
                f.debug_tuple("Fragment").field(&bytes.len()).finish()
            }
        }
    }
}

/// State machine that cuts the raw encoder stdout byte stream into
/// [`EncodedSegment`]s (`05-screen-capture.md` §6).
///
/// A real `ffmpeg` fMP4 stream is a sequence of ISO-BMFF boxes: the init
/// segment (`ftyp`, `moov`, ...), then one `moof` box followed by its
/// `mdat` box per fragment. Boxes are parsed as they arrive; a fragment is
/// complete when its `mdat` has been fully read, and everything before the
/// first `moof` is the init segment. Output that does not parse as boxes
/// (fake encoders, broken pipes) switches to opaque segmentation: every
/// completed read becomes one whole [`EncodedSegment::Fragment`], which is
/// still a decoder-safe drop unit.
pub struct Mp4Segmenter {
    /// Bytes received but not yet parsed into complete boxes.
    pending: Vec<u8>,
    /// Bytes of the stream init (everything before the first `moof`).
    init: Vec<u8>,
    /// Whether the init segment has been emitted.
    init_emitted: bool,
    /// The fragment being assembled (`moof` plus following boxes up to and
    /// including its `mdat`).
    fragment: Vec<u8>,
    /// Whether a `moof` has been seen and its `mdat` has not completed.
    in_fragment: bool,
    /// The stream is not box-structured: each completed read is emitted as
    /// one whole fragment.
    opaque: bool,
}

impl Mp4Segmenter {
    /// Create a fresh segmenter for a new encoder generation.
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            init: Vec::new(),
            init_emitted: false,
            fragment: Vec::new(),
            in_fragment: false,
            opaque: false,
        }
    }

    /// Feed the next chunk of encoder stdout; returns the segments that
    /// completed within this chunk.
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<EncodedSegment> {
        if self.opaque {
            // Non-box streams are cut at read boundaries: the drop unit is
            // one whole read, never a byte slice inside a box.
            return vec![EncodedSegment::Fragment(chunk.to_vec())];
        }
        self.pending.extend_from_slice(chunk);
        let mut out = Vec::new();
        loop {
            // A box header is 8 bytes (size + type).
            if self.pending.len() < 8 {
                break;
            }
            let size_field =
                u32::from_be_bytes(self.pending[..4].try_into().expect("length checked"));
            let kind: [u8; 4] = self.pending[4..8].try_into().expect("length checked");
            let (header_len, total) = match size_field {
                0 => {
                    // A box extending to EOF can never complete on a live
                    // pipe; the stream is not fragmented-MP4 structured.
                    self.enter_opaque(&mut out);
                    break;
                }
                1 => {
                    // 64-bit largesize header (16 bytes total).
                    if self.pending.len() < 16 {
                        break;
                    }
                    let large =
                        u64::from_be_bytes(self.pending[8..16].try_into().expect("length checked"));
                    (16, large)
                }
                size if size < 8 => {
                    // A box cannot be smaller than its header.
                    self.enter_opaque(&mut out);
                    break;
                }
                size => (8, u64::from(size)),
            };
            if total < header_len as u64 || total > MAX_BOX_BYTES as u64 {
                self.enter_opaque(&mut out);
                break;
            }
            let total = total as usize;
            if self.pending.len() < total {
                // The box is not fully received yet.
                break;
            }
            let rest = self.pending.split_off(total);
            let box_bytes = mem::replace(&mut self.pending, rest);
            match &kind {
                b"moof" => {
                    if !self.init_emitted {
                        if !self.init.is_empty() {
                            out.push(EncodedSegment::Init(mem::take(&mut self.init)));
                        }
                        self.init_emitted = true;
                    }
                    // Defensive: a second moof before the previous mdat
                    // completed (unreachable with ffmpeg's moof+mdat
                    // layout) flushes the pending fragment as a whole.
                    if self.in_fragment {
                        out.push(EncodedSegment::Fragment(mem::take(&mut self.fragment)));
                    }
                    self.fragment = box_bytes;
                    self.in_fragment = true;
                }
                b"mdat" if self.in_fragment => {
                    self.fragment.extend_from_slice(&box_bytes);
                    out.push(EncodedSegment::Fragment(mem::take(&mut self.fragment)));
                    self.in_fragment = false;
                }
                _ => {
                    if self.in_fragment {
                        self.fragment.extend_from_slice(&box_bytes);
                    } else {
                        self.init.extend_from_slice(&box_bytes);
                    }
                }
            }
        }
        out
    }

    /// The encoder's stdout closed. A truncated final fragment cannot be
    /// decoded and is dropped rather than emitted as corrupt bytes.
    pub fn finish(&mut self) -> Vec<EncodedSegment> {
        if self.opaque {
            return Vec::new();
        }
        let mut out = Vec::new();
        if !self.init_emitted {
            // The stream ended before any moof: everything received is the
            // whole (short) stream; hand it over as one init segment.
            self.init.extend_from_slice(&self.pending);
            self.pending.clear();
            if !self.init.is_empty() {
                out.push(EncodedSegment::Init(mem::take(&mut self.init)));
            }
            return out;
        }
        if self.in_fragment || !self.pending.is_empty() {
            tracing::debug!(
                bytes = self.pending.len() + self.fragment.len(),
                "dropping truncated fragment tail at encoder EOF"
            );
        }
        out
    }

    /// The stream cannot be parsed as ISO-BMFF boxes: emit whatever whole
    /// init was collected, drop the unparseable pending bytes, and cut every
    /// subsequent read at its own boundary.
    fn enter_opaque(&mut self, out: &mut Vec<EncodedSegment>) {
        if !self.init_emitted && !self.init.is_empty() {
            out.push(EncodedSegment::Init(mem::take(&mut self.init)));
        }
        if !self.pending.is_empty() {
            tracing::debug!(
                bytes = self.pending.len(),
                "encoder output is not box-structured; switching to opaque chunk segmentation"
            );
            self.pending.clear();
        }
        self.fragment.clear();
        self.in_fragment = false;
        self.opaque = true;
    }
}

impl Default for Mp4Segmenter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build `box <kind>(payload)` ISO-BMFF bytes.
    fn boxed(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + payload.len());
        bytes.extend_from_slice(&((8 + payload.len()) as u32).to_be_bytes());
        bytes.extend_from_slice(kind);
        bytes.extend_from_slice(payload);
        bytes
    }

    /// A synthetic fMP4 stream: init (ftyp, moov), then N moof+mdat
    /// fragments. Each fragment is distinguishable by its mdat payload.
    fn fmp4_stream(fragments: usize) -> Vec<u8> {
        let mut stream = Vec::new();
        stream.extend(boxed(b"ftyp", b"isom\x00\x00\x00\x00isom"));
        stream.extend(boxed(b"moov", &[0x11u8; 32]));
        for i in 0..fragments {
            stream.extend(boxed(b"moof", &[0x22u8; 16]));
            let marker = i as u32;
            stream.extend(boxed(b"mdat", &marker.to_be_bytes().repeat(16)));
        }
        stream
    }

    fn feed_all(segmenter: &mut Mp4Segmenter, stream: &[u8]) -> Vec<EncodedSegment> {
        let mut out = Vec::new();
        for chunk in stream.chunks(7) {
            out.extend(segmenter.feed(chunk));
        }
        out.extend(segmenter.finish());
        out
    }

    #[test]
    fn segments_a_real_fmp4_stream() {
        let stream = fmp4_stream(3);
        let segments = feed_all(&mut Mp4Segmenter::new(), &stream);
        assert_eq!(segments.len(), 4);
        let init = &segments[0];
        assert!(matches!(init, EncodedSegment::Init(_)));
        assert!(init.bytes().starts_with(b"\x00\x00\x00\x14ftyp"));
        assert!(init.bytes().windows(4).any(|w| w == b"moov"));
        for (i, segment) in segments[1..].iter().enumerate() {
            let fragment = match segment {
                EncodedSegment::Fragment(bytes) => bytes,
                _ => panic!("expected a fragment, got an init"),
            };
            assert_eq!(&fragment[..8], b"\x00\x00\x00\x18moof");
            // The fragment ends with its mdat box (8-byte header + 64-byte
            // payload), so the mdat type sits 68 bytes before the end.
            let mdat_type = fragment.len() - (8 + 64);
            assert_eq!(
                &fragment[mdat_type + 4..mdat_type + 8],
                b"mdat",
                "fragment must end with its mdat box"
            );
            // The whole mdat payload is byte-exact.
            assert_eq!(
                &fragment[fragment.len() - 64..],
                &(i as u32).to_be_bytes().repeat(16)
            );
        }
        // Feeding the same stream again re-segments identically.
        let again = feed_all(&mut Mp4Segmenter::new(), &stream);
        assert_eq!(again, segments);
    }

    #[test]
    fn init_contains_every_box_before_the_first_moof() {
        let stream = {
            let mut s = Vec::new();
            s.extend(boxed(b"ftyp", b"isom"));
            s.extend(boxed(b"free", &[0u8; 8]));
            s.extend(boxed(b"moov", &[0x33u8; 24]));
            s.extend(boxed(b"moof", &[0u8; 8]));
            s.extend(boxed(b"mdat", &[0u8; 8]));
            s
        };
        let segments = feed_all(&mut Mp4Segmenter::new(), &stream);
        assert_eq!(segments.len(), 2);
        let init = segments[0].bytes();
        assert!(init.windows(4).any(|w| w == b"ftyp"));
        assert!(init.windows(4).any(|w| w == b"free"));
        assert!(init.windows(4).any(|w| w == b"moov"));
    }

    #[test]
    fn boxes_between_moof_and_mdat_belong_to_the_fragment() {
        let stream = {
            let mut s = Vec::new();
            s.extend(boxed(b"ftyp", b"isom"));
            s.extend(boxed(b"moov", &[0u8; 8]));
            s.extend(boxed(b"moof", &[0u8; 8]));
            s.extend(boxed(b"prft", &[0x44u8; 12]));
            s.extend(boxed(b"mdat", &[0x55u8; 16]));
            s
        };
        let segments = feed_all(&mut Mp4Segmenter::new(), &stream);
        assert_eq!(segments.len(), 2);
        let fragment = match &segments[1] {
            EncodedSegment::Fragment(bytes) => bytes,
            _ => panic!("expected a fragment"),
        };
        assert_eq!(&fragment[..8], b"\x00\x00\x00\x10moof");
        assert!(fragment.windows(4).any(|w| w == b"prft"));
        assert!(fragment.windows(4).any(|w| w == b"mdat"));
    }

    #[test]
    fn fragments_split_across_chunk_boundaries() {
        let stream = fmp4_stream(2);
        // Feed byte-by-byte: every box boundary and header is split.
        let mut segmenter = Mp4Segmenter::new();
        let mut out = Vec::new();
        for byte in &stream {
            out.extend(segmenter.feed(&[*byte]));
        }
        out.extend(segmenter.finish());
        assert_eq!(out.len(), 3, "init + 2 fragments");
        let mut expected = feed_all(&mut Mp4Segmenter::new(), &stream);
        let expected = expected.split_off(0);
        assert_eq!(out, expected);
    }

    #[test]
    fn truncated_fragment_tail_is_dropped_at_eof() {
        let stream = fmp4_stream(2);
        // Cut the stream 10 bytes short of the final mdat: the incomplete
        // tail must not reach the consumer.
        let cut = &stream[..stream.len() - 10];
        let mut segmenter = Mp4Segmenter::new();
        let mut out = Vec::new();
        for chunk in cut.chunks(31) {
            out.extend(segmenter.feed(chunk));
        }
        out.extend(segmenter.finish());
        assert_eq!(out.len(), 2, "init + first complete fragment only");
        // A whole stream yields one more (complete) fragment, and the first
        // two segments are identical to the cut stream's output.
        let whole = feed_all(&mut Mp4Segmenter::new(), &stream);
        assert_eq!(whole.len(), 3);
        assert_eq!(out, whole[..2]);
    }

    #[test]
    fn stream_ending_before_any_moof_is_one_init() {
        let stream = {
            let mut s = Vec::new();
            s.extend(boxed(b"ftyp", b"isom"));
            s.extend(boxed(b"moov", &[0u8; 16]));
            s
        };
        let segments = feed_all(&mut Mp4Segmenter::new(), &stream);
        assert_eq!(segments.len(), 1);
        assert!(matches!(segments[0], EncodedSegment::Init(_)));
        assert_eq!(segments[0].bytes(), &stream[..]);
    }

    #[test]
    fn empty_stream_yields_no_segments() {
        let segments = feed_all(&mut Mp4Segmenter::new(), &[]);
        assert!(segments.is_empty());
    }

    #[test]
    fn opaque_fallback_for_non_box_output() {
        let mut segmenter = Mp4Segmenter::new();
        // All-zero bytes parse as a size-0 box ("to EOF"): the stream is not
        // box-structured, the unparseable bytes are dropped, and every
        // subsequent read becomes one whole fragment.
        let first = segmenter.feed(&[0u8; 64]);
        assert!(
            first.is_empty(),
            "unparseable bytes are dropped, not emitted"
        );
        let second = segmenter.feed(&[0xAB; 64]);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].bytes(), &[0xABu8; 64]);
        assert!(matches!(second[0], EncodedSegment::Fragment(_)));
        assert!(segmenter.finish().is_empty());
    }

    #[test]
    fn oversized_box_triggers_the_opaque_fallback() {
        let mut stream = Vec::new();
        // A box whose declared size exceeds the buffering bound.
        stream.extend_from_slice(&(MAX_BOX_BYTES as u32 + 1).to_be_bytes());
        stream.extend_from_slice(b"mdat");
        stream.extend_from_slice(&[0u8; 8]);
        let mut segmenter = Mp4Segmenter::new();
        // The unparseable oversized box is dropped, nothing is emitted.
        assert!(segmenter.feed(&stream).is_empty());
        // Everything after it is opaque: each subsequent read is one whole
        // fragment, still a decoder-safe drop unit.
        let mut out = Vec::new();
        out.extend(segmenter.feed(&[0u8; 64]));
        out.extend(segmenter.feed(&[0xAAu8; 64]));
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].bytes(), &[0u8; 64]);
        assert_eq!(out[1].bytes(), &[0xAAu8; 64]);
        assert!(out.iter().all(|s| matches!(s, EncodedSegment::Fragment(_))));
    }

    #[test]
    fn largesize_boxes_are_parsed() {
        // A valid box using the 64-bit largesize header (size field = 1).
        let payload = [0x66u8; 24];
        let mut stream = Vec::new();
        stream.extend_from_slice(&1u32.to_be_bytes());
        stream.extend_from_slice(b"mdat");
        stream.extend_from_slice(&((16 + payload.len()) as u64).to_be_bytes());
        stream.extend_from_slice(&payload);
        stream.extend(boxed(b"moof", &[0u8; 8]));
        stream.extend(boxed(b"mdat", &[0x77u8; 8]));
        let mut segmenter = Mp4Segmenter::new();
        let mut out = Vec::new();
        for chunk in stream.chunks(5) {
            out.extend(segmenter.feed(chunk));
        }
        out.extend(segmenter.finish());
        // The largesize box precedes any moof, so it belongs to the init;
        // the moof+mdat pair is the first fragment.
        assert_eq!(out.len(), 2);
        let init = match &out[0] {
            EncodedSegment::Init(bytes) => bytes,
            _ => panic!("expected an init"),
        };
        assert!(init.windows(4).any(|w| w == b"mdat"));
        assert!(init.ends_with(&[0x66u8; 24]));
        let fragment = match &out[1] {
            EncodedSegment::Fragment(bytes) => bytes,
            _ => panic!("expected a fragment"),
        };
        assert_eq!(&fragment[..8], b"\x00\x00\x00\x10moof");
        assert!(fragment.ends_with(&[0x77u8; 8]));
    }

    #[test]
    fn invalid_header_size_triggers_the_opaque_fallback() {
        let mut stream = Vec::new();
        stream.extend_from_slice(&3u32.to_be_bytes());
        stream.extend_from_slice(b"moof");
        stream.extend(boxed(b"mdat", &[0u8; 8]));
        let mut segmenter = Mp4Segmenter::new();
        assert!(segmenter.feed(&stream).is_empty());
        assert_eq!(
            segmenter.feed(&[0x12u8; 16]),
            vec![EncodedSegment::Fragment(vec![0x12u8; 16])]
        );
    }

    #[test]
    fn segment_accessors() {
        let init = EncodedSegment::Init(vec![1, 2, 3]);
        assert_eq!(init.bytes(), &[1, 2, 3]);
        assert_eq!(init.len(), 3);
        assert!(!init.is_empty());
        assert_eq!(init.clone().into_bytes(), vec![1, 2, 3]);
        assert!(EncodedSegment::Fragment(Vec::new()).is_empty());
    }
}
