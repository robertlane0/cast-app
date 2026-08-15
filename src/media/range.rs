//! HTTP `Range` header parsing, `Content-Range` building, and range
//! validation (`04-media-proxy.md` §3.1).

/// How the server must treat a request, derived from its `Range` header
/// (`04-media-proxy.md` §3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeDecision {
    /// No `Range` header, a non-`bytes` unit, or multiple ranges: serve
    /// `200 OK` with the full body.
    Full,
    /// Serve `206 Partial Content` with the inclusive absolute byte range
    /// `[start, end]`.
    Partial { start: u64, end: u64 },
    /// Malformed or unsatisfiable: serve `416 Range Not Satisfiable` with
    /// `Content-Range: bytes */<size>`.
    Unsatisfiable,
}

/// Resolve a request's `Range` header against a resource of `size` bytes
/// (`04-media-proxy.md` §3.1):
///
/// - no header -> [`RangeDecision::Full`];
/// - valid single `bytes=a-b`, `bytes=a-`, `bytes=-suffix` ->
///   [`RangeDecision::Partial`] with inclusive bounds;
/// - multiple ranges -> [`RangeDecision::Full`] (ignored per spec);
/// - malformed or unsatisfiable (including `bytes=-0`, `bytes=a-b` with
///   `a > b`, or a start at/after EOF) -> [`RangeDecision::Unsatisfiable`].
pub fn parse_range(header: Option<&str>, size: u64) -> RangeDecision {
    let Some(header) = header else {
        return RangeDecision::Full;
    };
    let header = header.trim();
    let Some(rest) = header.strip_prefix("bytes=") else {
        // Unknown range unit: ignored (RFC 7233 §3.1).
        return RangeDecision::Full;
    };

    let ranges: Vec<&str> = rest.split(',').map(str::trim).collect();
    if ranges.is_empty() || ranges.iter().all(|range| range.is_empty()) {
        return RangeDecision::Unsatisfiable;
    }
    if ranges.len() > 1 {
        // Multi-range: ignored; full 200 per `04-media-proxy.md` §3.1.
        return RangeDecision::Full;
    }
    parse_single_range(ranges[0], size)
}

fn parse_single_range(spec: &str, size: u64) -> RangeDecision {
    let (start_text, end_text) = match spec.split_once('-') {
        Some(parts) => parts,
        None => return RangeDecision::Unsatisfiable,
    };

    if start_text.is_empty() {
        // Suffix range: `bytes=-N` = the last N bytes.
        let Ok(suffix) = end_text.trim().parse::<u64>() else {
            return RangeDecision::Unsatisfiable;
        };
        if suffix == 0 || size == 0 {
            // `bytes=-0` is unsatisfiable (AGENTS.md §12).
            return RangeDecision::Unsatisfiable;
        }
        if suffix >= size {
            return RangeDecision::Partial {
                start: 0,
                end: size - 1,
            };
        }
        return RangeDecision::Partial {
            start: size - suffix,
            end: size - 1,
        };
    }

    let Ok(start) = start_text.trim().parse::<u64>() else {
        return RangeDecision::Unsatisfiable;
    };

    if end_text.is_empty() {
        // Open-ended: `bytes=a-`.
        if size == 0 || start >= size {
            return RangeDecision::Unsatisfiable;
        }
        return RangeDecision::Partial {
            start,
            end: size - 1,
        };
    }

    let Ok(end) = end_text.trim().parse::<u64>() else {
        return RangeDecision::Unsatisfiable;
    };
    if start > end {
        // `bytes=a-b` with a > b is malformed (AGENTS.md §12).
        return RangeDecision::Unsatisfiable;
    }
    if start >= size {
        return RangeDecision::Unsatisfiable;
    }
    RangeDecision::Partial {
        start,
        end: end.min(size - 1),
    }
}

/// Build a `Content-Range` header value for a `206` response
/// (`04-media-proxy.md` §3.1).
pub fn content_range(start: u64, end: u64, size: u64) -> String {
    format!("bytes {start}-{end}/{size}")
}

/// Build the `Content-Range` value carried by a `416` response.
pub fn unsatisfiable_content_range(size: u64) -> String {
    format!("bytes */{size}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partial(decision: RangeDecision, start: u64, end: u64) {
        assert_eq!(
            decision,
            RangeDecision::Partial { start, end },
            "expected [{start}, {end}]"
        );
    }

    #[test]
    fn no_header_is_full() {
        assert_eq!(parse_range(None, 1000), RangeDecision::Full);
    }

    #[test]
    fn zero_length_resource_is_unsatisfiable_for_ranges() {
        assert_eq!(
            parse_range(Some("bytes=0-"), 0),
            RangeDecision::Unsatisfiable
        );
        assert_eq!(
            parse_range(Some("bytes=-1"), 0),
            RangeDecision::Unsatisfiable
        );
    }

    #[test]
    fn closed_range_clamps_end_to_eof() {
        partial(parse_range(Some("bytes=0-499"), 1000), 0, 499);
        partial(parse_range(Some("bytes=500-9999"), 1000), 500, 999);
        partial(parse_range(Some("bytes=0-0"), 1000), 0, 0);
        partial(parse_range(Some("bytes=999-999"), 1000), 999, 999);
    }

    #[test]
    fn open_ended_range_runs_to_eof() {
        partial(parse_range(Some("bytes=0-"), 1000), 0, 999);
        partial(parse_range(Some("bytes=500-"), 1000), 500, 999);
        partial(parse_range(Some("bytes=999-"), 1000), 999, 999);
    }

    #[test]
    fn suffix_range_takes_last_n_bytes() {
        partial(parse_range(Some("bytes=-100"), 1000), 900, 999);
        partial(parse_range(Some("bytes=-1"), 1000), 999, 999);
        partial(parse_range(Some("bytes=-1000"), 1000), 0, 999);
        partial(parse_range(Some("bytes=-9999"), 1000), 0, 999);
    }

    #[test]
    fn whitespace_is_tolerated() {
        partial(parse_range(Some(" bytes=0-499 "), 1000), 0, 499);
        partial(parse_range(Some("bytes= 0 - 499"), 1000), 0, 499);
    }

    #[test]
    fn start_at_eof_is_unsatisfiable() {
        assert_eq!(
            parse_range(Some("bytes=1000-"), 1000),
            RangeDecision::Unsatisfiable
        );
        assert_eq!(
            parse_range(Some("bytes=1000-1001"), 1000),
            RangeDecision::Unsatisfiable
        );
        assert_eq!(
            parse_range(Some("bytes=100000-"), 1000),
            RangeDecision::Unsatisfiable
        );
    }

    #[test]
    fn reversed_bounds_are_unsatisfiable() {
        assert_eq!(
            parse_range(Some("bytes=1-0"), 1000),
            RangeDecision::Unsatisfiable
        );
    }

    #[test]
    fn zero_suffix_is_unsatisfiable() {
        assert_eq!(
            parse_range(Some("bytes=-0"), 1000),
            RangeDecision::Unsatisfiable
        );
    }

    #[test]
    fn malformed_values_are_unsatisfiable() {
        assert_eq!(
            parse_range(Some("bytes="), 1000),
            RangeDecision::Unsatisfiable
        );
        assert_eq!(
            parse_range(Some("bytes=abc"), 1000),
            RangeDecision::Unsatisfiable
        );
        assert_eq!(
            parse_range(Some("bytes=abc-def"), 1000),
            RangeDecision::Unsatisfiable
        );
        assert_eq!(
            parse_range(Some("bytes=-"), 1000),
            RangeDecision::Unsatisfiable
        );
        assert_eq!(
            parse_range(Some("bytes=1-2-3"), 1000),
            RangeDecision::Unsatisfiable
        );
    }

    #[test]
    fn multiple_ranges_are_ignored_as_full() {
        assert_eq!(
            parse_range(Some("bytes=0-10,20-30"), 1000),
            RangeDecision::Full
        );
    }

    #[test]
    fn non_bytes_unit_is_ignored_as_full() {
        assert_eq!(parse_range(Some("items=0-10"), 1000), RangeDecision::Full);
    }

    #[test]
    fn content_range_formatting() {
        assert_eq!(content_range(0, 499, 1000), "bytes 0-499/1000");
        assert_eq!(content_range(900, 999, 1000), "bytes 900-999/1000");
        assert_eq!(unsatisfiable_content_range(1000), "bytes */1000");
    }
}
