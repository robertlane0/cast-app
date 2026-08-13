//! HTTP Range parsing tests (`04-media-proxy.md` §3.1).
//! Full/single/suffix/multi/unsatisfiable classification and
//! `Content-Range` formatting.
//! Gate: `cargo test --test range_tests`.

#![forbid(unsafe_code)]

use cast_app::media::range::{
    RangeDecision, content_range, parse_range, unsatisfiable_content_range,
};

fn partial(header: Option<&str>, size: u64, start: u64, end: u64) {
    assert_eq!(
        parse_range(header, size),
        RangeDecision::Partial { start, end },
        "expected [{start}, {end}] for {header:?} of size {size}"
    );
}

// ---------------------------------------------------------------------------
// Missing / multi / unknown-unit
// ---------------------------------------------------------------------------

#[test]
fn missing_range_serves_full_body() {
    assert_eq!(parse_range(None, 1000), RangeDecision::Full);
}

#[test]
fn multi_range_is_ignored_as_full() {
    assert_eq!(
        parse_range(Some("bytes=0-10,20-30"), 1000),
        RangeDecision::Full
    );
    assert_eq!(
        parse_range(Some("bytes=0-10, 20-30"), 1000),
        RangeDecision::Full
    );
}

#[test]
fn unknown_range_unit_is_ignored_as_full() {
    assert_eq!(parse_range(Some("items=0-10"), 1000), RangeDecision::Full);
    assert_eq!(parse_range(Some("chars=5-"), 1000), RangeDecision::Full);
}

// ---------------------------------------------------------------------------
// Valid single ranges -> 206
// ---------------------------------------------------------------------------

#[test]
fn closed_range_is_partial() {
    partial(Some("bytes=0-499"), 1000, 0, 499);
    partial(Some("bytes=500-999"), 1000, 500, 999);
    partial(Some("bytes=0-0"), 1000, 0, 0);
    partial(Some("bytes=999-999"), 1000, 999, 999);
}

#[test]
fn closed_range_clamps_to_eof() {
    partial(Some("bytes=500-9999"), 1000, 500, 999);
}

#[test]
fn open_ended_range_is_partial_to_eof() {
    partial(Some("bytes=0-"), 1000, 0, 999);
    partial(Some("bytes=500-"), 1000, 500, 999);
    partial(Some("bytes=999-"), 1000, 999, 999);
}

#[test]
fn suffix_range_takes_last_bytes() {
    partial(Some("bytes=-100"), 1000, 900, 999);
    partial(Some("bytes=-1"), 1000, 999, 999);
}

#[test]
fn suffix_larger_than_file_is_whole_file() {
    partial(Some("bytes=-1000"), 1000, 0, 999);
    partial(Some("bytes=-9999"), 1000, 0, 999);
}

#[test]
fn whitespace_is_tolerated() {
    partial(Some("bytes= 0 - 499 "), 1000, 0, 499);
    partial(Some(" bytes=500-999 "), 1000, 500, 999);
}

// ---------------------------------------------------------------------------
// Unsatisfiable / malformed -> 416
// ---------------------------------------------------------------------------

#[test]
fn start_at_or_past_eof_is_unsatisfiable() {
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
fn empty_file_has_no_satisfiable_range() {
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
fn malformed_syntax_is_unsatisfiable() {
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
    assert_eq!(
        parse_range(Some("bytes=0-abc"), 1000),
        RangeDecision::Unsatisfiable
    );
}

// ---------------------------------------------------------------------------
// Content-Range formatting
// ---------------------------------------------------------------------------

#[test]
fn content_range_values_are_exact() {
    assert_eq!(content_range(0, 499, 1000), "bytes 0-499/1000");
    assert_eq!(content_range(900, 999, 1000), "bytes 900-999/1000");
    assert_eq!(content_range(0, 0, 1), "bytes 0-0/1");
    assert_eq!(unsatisfiable_content_range(1000), "bytes */1000");
}
