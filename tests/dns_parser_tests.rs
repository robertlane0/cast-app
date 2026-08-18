// SPDX-License-Identifier: MIT OR Apache-2.0
#![forbid(unsafe_code)]

//! DNS packet parser and record-correlation tests for mDNS discovery.
//! Owned by `03-cast-engine.md` §2.

use cast_app::cast::mdns::{
    DnsError, GOOGLECAST_SERVICE, build_ptr_query, correlate, parse_packet,
};
use std::net::Ipv4Addr;

const TYPE_A: u16 = 1;
const TYPE_PTR: u16 = 12;
const TYPE_TXT: u16 = 16;
const TYPE_SRV: u16 = 33;

/// Realistic Chromecast-style response for one device
/// ("My Living Room", 192.168.1.42:8009, TXT `fn=My Living Room`), using
/// compression pointers throughout. Verified byte layout:
/// header 0..12, question 12..40, PTR 40..91 (rdlen 39, instance labels at
/// 52), SRV 91..131 (target labels at 109), TXT 131..161, A 161..177.
const GOLDEN: &str = concat!(
    // header: ID 0, flags 0x8400 (QR|AA), QD=1 AN=2 NS=0 AR=2
    "000084000001000200000002",
    // question: _googlecast._tcp.local PTR IN
    "0B5F676F6F676C6563617374045F746370056C6F63616C00",
    "000C0001",
    // answer 1: PTR, owner -> question name (offset 12)
    "C00C000C0001000000780027",
    "0E4D79204C6976696E6720526F6F6D",
    "0B5F676F6F676C6563617374045F746370056C6F63616C00",
    // answer 2: SRV, owner -> instance name (offset 52), target my-living-room.local
    "C0340021000100000078001C",
    "000000001F49",
    "0E6D792D6C6976696E672D726F6F6D056C6F63616C00",
    // additional 1: TXT, owner -> instance name (offset 52), "fn=My Living Room"
    "C03400100001000000780012",
    "11666E3D4D79204C6976696E6720526F6F6D",
    // additional 2: A, owner -> SRV target (offset 109), 192.168.1.42
    "C06D00010001000000780004",
    "C0A8012A",
);

fn from_hex(hex: &str) -> Vec<u8> {
    let hex: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(hex.len() % 2 == 0, "hex string must have even length");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex digit"))
        .collect()
}

fn header(qd: u16, an: u16, ns: u16, ar: u16) -> Vec<u8> {
    let mut out = vec![0x00, 0x00, 0x84, 0x00];
    out.extend_from_slice(&qd.to_be_bytes());
    out.extend_from_slice(&an.to_be_bytes());
    out.extend_from_slice(&ns.to_be_bytes());
    out.extend_from_slice(&ar.to_be_bytes());
    out
}

fn name(labels: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for label in labels {
        assert!(label.len() <= 63);
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    out
}

fn record(owner: &[u8], record_type: u16, ttl: u32, rdata: &[u8]) -> Vec<u8> {
    let mut out = owner.to_vec();
    out.extend_from_slice(&record_type.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes()); // class IN
    out.extend_from_slice(&ttl.to_be_bytes());
    out.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
    out.extend_from_slice(rdata);
    out
}

fn srv_rdata(port: u16, target_labels: &[&str]) -> Vec<u8> {
    let mut out = vec![0, 0, 0, 0]; // priority 0, weight 0
    out.extend_from_slice(&port.to_be_bytes());
    out.extend_from_slice(&name(target_labels));
    out
}

fn txt_rdata(entries: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for entry in entries {
        out.push(entry.len() as u8);
        out.extend_from_slice(entry.as_bytes());
    }
    out
}

fn a_rdata(octets: [u8; 4]) -> Vec<u8> {
    octets.to_vec()
}

fn ptr_rdata(instance_labels: &[&str]) -> Vec<u8> {
    name(instance_labels)
}

/// Single-instance packet built from scratch; used by several tests.
fn single_device_packet() -> Vec<u8> {
    let mut pkt = header(0, 2, 0, 2);
    pkt.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["Kitchen", "_googlecast", "_tcp", "local"]),
    ));
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_SRV,
        120,
        &srv_rdata(8009, &["kitchen", "local"]),
    ));
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_TXT,
        120,
        &txt_rdata(&["fn=Kitchen TV"]),
    ));
    pkt.extend(record(
        &name(&["kitchen", "local"]),
        TYPE_A,
        120,
        &a_rdata([10, 0, 0, 5]),
    ));
    pkt
}

// ---------------------------------------------------------------------------
// Query builder
// ---------------------------------------------------------------------------

#[test]
fn ptr_query_has_expected_layout() {
    let query = build_ptr_query();
    assert_eq!(query.len(), 40);
    assert_eq!(&query[0..2], &[0x00, 0x00]); // ID = 0
    assert_eq!(&query[2..4], &[0x00, 0x00]); // flags: recursion-desired cleared
    assert_eq!(&query[4..6], &[0x00, 0x01]); // QDCOUNT = 1
    assert_eq!(&query[6..12], &[0, 0, 0, 0, 0, 0]); // AN/NS/AR = 0
    assert_eq!(
        &query[12..24],
        &[
            0x0B, b'_', b'g', b'o', b'o', b'g', b'l', b'e', b'c', b'a', b's', b't'
        ]
    );
    assert_eq!(&query[24..29], &[0x04, b'_', b't', b'c', b'p']);
    assert_eq!(&query[29..35], &[0x05, b'l', b'o', b'c', b'a', b'l']);
    assert_eq!(query[35], 0x00); // root label
    assert_eq!(&query[36..38], &[0x00, 0x0C]); // type PTR
    assert_eq!(&query[38..40], &[0x00, 0x01]); // class IN
}

// ---------------------------------------------------------------------------
// Golden packets and record correlation
// ---------------------------------------------------------------------------

#[test]
fn golden_response_yields_device() {
    let devices = parse_packet(&from_hex(GOLDEN)).expect("golden packet parses");
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].ip, Ipv4Addr::new(192, 168, 1, 42));
    assert_eq!(devices[0].port, 8009);
    assert_eq!(devices[0].name, "My Living Room");
    assert_eq!(
        devices[0].device_id, None,
        "no TXT id= in the golden packet"
    );
}

#[test]
fn single_device_packet_correlates() {
    let devices = parse_packet(&single_device_packet()).expect("packet parses");
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].ip, Ipv4Addr::new(10, 0, 0, 5));
    assert_eq!(devices[0].port, 8009);
    assert_eq!(devices[0].name, "Kitchen TV");
    assert_eq!(
        devices[0].device_id, None,
        "single_device_packet TXT has no id= entry"
    );
}

#[test]
fn friendly_name_falls_back_to_instance_label() {
    let mut pkt = header(0, 2, 0, 2);
    pkt.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["Kitchen", "_googlecast", "_tcp", "local"]),
    ));
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_SRV,
        120,
        &srv_rdata(8009, &["kitchen", "local"]),
    ));
    // TXT present but without an fn= key.
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_TXT,
        120,
        &txt_rdata(&["cd=deadbeef", "id=0123456789"]),
    ));
    pkt.extend(record(
        &name(&["kitchen", "local"]),
        TYPE_A,
        120,
        &a_rdata([10, 0, 0, 5]),
    ));

    let devices = parse_packet(&pkt).expect("packet parses");
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].name, "Kitchen");
    assert_eq!(
        devices[0].device_id.as_deref(),
        Some("0123456789"),
        "TXT id= is extracted for the TOFU pin key"
    );
}

#[test]
fn fn_value_wins_over_instance_label() {
    let mut pkt = header(0, 2, 0, 2);
    pkt.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["dining-room", "_googlecast", "_tcp", "local"]),
    ));
    pkt.extend(record(
        &name(&["dining-room", "_googlecast", "_tcp", "local"]),
        TYPE_SRV,
        120,
        &srv_rdata(8009, &["dining-room", "local"]),
    ));
    pkt.extend(record(
        &name(&["dining-room", "_googlecast", "_tcp", "local"]),
        TYPE_TXT,
        120,
        &txt_rdata(&["fn=Dining Room"]),
    ));
    pkt.extend(record(
        &name(&["dining-room", "local"]),
        TYPE_A,
        120,
        &a_rdata([10, 0, 0, 6]),
    ));

    let devices = parse_packet(&pkt).expect("packet parses");
    assert_eq!(devices[0].name, "Dining Room");
}

#[test]
fn instance_matching_is_case_insensitive() {
    let mut pkt = header(0, 2, 0, 2);
    pkt.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["KITCHEN", "_googlecast", "_tcp", "local"]),
    ));
    // SRV/TXT owner in different case than the PTR rdata instance name.
    pkt.extend(record(
        &name(&["kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_SRV,
        120,
        &srv_rdata(8009, &["KITCHEN", "local"]),
    ));
    pkt.extend(record(
        &name(&["kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_TXT,
        120,
        &txt_rdata(&["fn=Kitchen TV"]),
    ));
    // A owner in different case than the SRV target.
    pkt.extend(record(
        &name(&["kitchen", "local"]),
        TYPE_A,
        120,
        &a_rdata([10, 0, 0, 5]),
    ));

    let devices = parse_packet(&pkt).expect("packet parses");
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].ip, Ipv4Addr::new(10, 0, 0, 5));
    assert_eq!(devices[0].name, "Kitchen TV");
}

#[test]
fn multiple_instances_are_correlated_separately() {
    let mut pkt = header(0, 4, 0, 4);
    pkt.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["Kitchen", "_googlecast", "_tcp", "local"]),
    ));
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_SRV,
        120,
        &srv_rdata(8009, &["kitchen", "local"]),
    ));
    pkt.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["Den", "_googlecast", "_tcp", "local"]),
    ));
    pkt.extend(record(
        &name(&["Den", "_googlecast", "_tcp", "local"]),
        TYPE_SRV,
        120,
        &srv_rdata(8443, &["den", "local"]),
    ));
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_TXT,
        120,
        &txt_rdata(&["fn=Kitchen TV"]),
    ));
    pkt.extend(record(
        &name(&["Den", "_googlecast", "_tcp", "local"]),
        TYPE_TXT,
        120,
        &txt_rdata(&["fn=Den"]),
    ));
    pkt.extend(record(
        &name(&["kitchen", "local"]),
        TYPE_A,
        120,
        &a_rdata([10, 0, 0, 5]),
    ));
    pkt.extend(record(
        &name(&["den", "local"]),
        TYPE_A,
        120,
        &a_rdata([10, 0, 0, 9]),
    ));

    let mut devices = parse_packet(&pkt).expect("packet parses");
    assert_eq!(devices.len(), 2);
    devices.sort_by_key(|d| d.ip);
    assert_eq!(devices[0].ip, Ipv4Addr::new(10, 0, 0, 5));
    assert_eq!(devices[0].port, 8009);
    assert_eq!(devices[0].name, "Kitchen TV");
    assert_eq!(devices[1].ip, Ipv4Addr::new(10, 0, 0, 9));
    assert_eq!(devices[1].port, 8443);
    assert_eq!(devices[1].name, "Den");
}

#[test]
fn instance_without_srv_is_skipped() {
    let mut pkt = header(0, 1, 0, 0);
    pkt.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["Ghost", "_googlecast", "_tcp", "local"]),
    ));
    let devices = parse_packet(&pkt).expect("packet parses");
    assert!(devices.is_empty());
}

#[test]
fn instance_without_a_record_is_skipped() {
    let mut pkt = header(0, 2, 0, 1);
    pkt.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["Kitchen", "_googlecast", "_tcp", "local"]),
    ));
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_SRV,
        120,
        &srv_rdata(8009, &["kitchen", "local"]),
    ));
    // TXT exists, but no A record anywhere.
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_TXT,
        120,
        &txt_rdata(&["fn=Kitchen TV"]),
    ));
    let devices = parse_packet(&pkt).expect("packet parses");
    assert!(devices.is_empty());
}

#[test]
fn a_record_owner_matching_instance_name_is_used_as_fallback() {
    let mut pkt = header(0, 2, 0, 2);
    pkt.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["Kitchen", "_googlecast", "_tcp", "local"]),
    ));
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_SRV,
        120,
        &srv_rdata(8009, &["unresolvable", "local"]),
    ));
    // A record owner equals the instance name, not the SRV target.
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_A,
        120,
        &a_rdata([10, 0, 0, 5]),
    ));
    pkt.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_TXT,
        120,
        &txt_rdata(&["fn=Kitchen TV"]),
    ));

    let devices = parse_packet(&pkt).expect("packet parses");
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].ip, Ipv4Addr::new(10, 0, 0, 5));
}

#[test]
fn unsupported_record_types_are_skipped() {
    // Insert an AAAA record and an OPT record (and a CNAME) into the answer
    // section; they must be skipped without breaking correlation.
    let mut with_extra = Vec::new();
    with_extra.extend(header(0, 5, 0, 2));
    with_extra.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["Kitchen", "_googlecast", "_tcp", "local"]),
    ));
    with_extra.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_SRV,
        120,
        &srv_rdata(8009, &["kitchen", "local"]),
    ));
    // Unsupported: AAAA (16-byte rdata), OPT (11-byte rdata), CNAME (name).
    with_extra.extend(record(
        &name(&["kitchen", "local"]),
        28,
        120,
        &[0xFE, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
    ));
    with_extra.extend(record(
        &[0x00], // empty owner name, correctly terminated
        41,
        120,
        &[
            0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ],
    ));
    with_extra.extend(record(
        &name(&["kitchen", "local"]),
        5,
        120,
        &name(&["other", "local"]),
    ));
    with_extra.extend(record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_TXT,
        120,
        &txt_rdata(&["fn=Kitchen TV"]),
    ));
    with_extra.extend(record(
        &name(&["kitchen", "local"]),
        TYPE_A,
        120,
        &a_rdata([10, 0, 0, 5]),
    ));

    let devices = parse_packet(&with_extra).expect("packet parses");
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].ip, Ipv4Addr::new(10, 0, 0, 5));
    assert_eq!(devices[0].name, "Kitchen TV");
}

#[test]
fn aaaa_records_are_ignored() {
    // IPv4-only discovery: only A records are consumed.
    let mut pkt = header(0, 1, 0, 1);
    pkt.extend(record(
        &name(&["_googlecast", "_tcp", "local"]),
        TYPE_PTR,
        120,
        &ptr_rdata(&["Kitchen", "_googlecast", "_tcp", "local"]),
    ));
    pkt.extend(record(
        &name(&["kitchen", "local"]),
        28,
        120,
        &[0xFE, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
    ));
    let devices = parse_packet(&pkt).expect("packet parses");
    assert!(devices.is_empty());
}

// ---------------------------------------------------------------------------
// Compression pointers
// ---------------------------------------------------------------------------

#[test]
fn pointer_chain_of_two_hops_resolves() {
    // PTR rdata name: ptr(cell1) -> ptr(cell2) -> labels. The pointer cells
    // are stashed after the last record — compression pointers resolve
    // against any packet offset.
    let mut pkt = header(0, 2, 0, 1);
    let mut rec = name(&["_googlecast", "_tcp", "local"]);
    rec.extend_from_slice(&TYPE_PTR.to_be_bytes());
    rec.extend_from_slice(&1u16.to_be_bytes());
    rec.extend_from_slice(&120u32.to_be_bytes());
    rec.extend_from_slice(&2u16.to_be_bytes()); // rdlength: one pointer
    let srv = record(
        &name(&["Kitchen", "_googlecast", "_tcp", "local"]),
        TYPE_SRV,
        120,
        &srv_rdata(8009, &["kitchen", "local"]),
    );
    let a = record(
        &name(&["kitchen", "local"]),
        TYPE_A,
        120,
        &a_rdata([10, 0, 0, 5]),
    );
    // Trail cells (after the last record) hold the chain.
    let cell1 = 12 + rec.len() + 2 + srv.len() + a.len();
    let cell2 = cell1 + 2;
    pkt.extend(rec);
    pkt.extend([0xC0, cell1 as u8]); // PTR rdata: ptr -> cell1
    pkt.extend(srv);
    pkt.extend(a);
    pkt.extend([0xC0, cell2 as u8]); // cell1: ptr -> cell2
    pkt.extend(name(&["Kitchen", "_googlecast", "_tcp", "local"])); // cell2

    let devices = parse_packet(&pkt).expect("chain parses");
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].name, "Kitchen");
    assert_eq!(devices[0].port, 8009);
}

#[test]
fn pointer_cycle_is_rejected() {
    // rdata name points at itself.
    let mut pkt = header(0, 1, 0, 0);
    let owner = name(&["_googlecast", "_tcp", "local"]);
    let mut rec = owner.clone();
    rec.extend_from_slice(&TYPE_PTR.to_be_bytes());
    rec.extend_from_slice(&1u16.to_be_bytes());
    rec.extend_from_slice(&120u32.to_be_bytes());
    rec.extend_from_slice(&2u16.to_be_bytes());
    let rdata_pos = 12 + rec.len();

    pkt.extend(rec);
    pkt.extend([0xC0, rdata_pos as u8]);

    assert_eq!(parse_packet(&pkt), Err(DnsError::PointerCycle));
}

#[test]
fn pointer_chain_beyond_depth_limit_is_rejected() {
    // Five pointers in a row; the depth limit is four.
    let mut pkt = header(0, 1, 0, 0);
    let owner = name(&["_googlecast", "_tcp", "local"]);
    let mut rec = owner.clone();
    rec.extend_from_slice(&TYPE_PTR.to_be_bytes());
    rec.extend_from_slice(&1u16.to_be_bytes());
    rec.extend_from_slice(&120u32.to_be_bytes());
    rec.extend_from_slice(&2u16.to_be_bytes());
    let rdata_pos = 12 + rec.len();
    let cell1 = rdata_pos + 2;
    let cell2 = cell1 + 2;
    let cell3 = cell2 + 2;
    let cell4 = cell3 + 2;
    let cell5 = cell4 + 2;

    pkt.extend(rec);
    pkt.extend([0xC0, cell1 as u8]); // rdata (1st pointer)
    pkt.extend([0xC0, cell2 as u8]); // cell1 (2nd)
    pkt.extend([0xC0, cell3 as u8]); // cell2 (3rd)
    pkt.extend([0xC0, cell4 as u8]); // cell3 (4th)
    pkt.extend([0xC0, cell5 as u8]); // cell4 (5th) -> depth limit
    pkt.extend(name(&["Kitchen", "_googlecast", "_tcp", "local"])); // cell5

    assert_eq!(parse_packet(&pkt), Err(DnsError::PointerDepthExceeded));
}

#[test]
fn pointer_out_of_bounds_is_rejected() {
    let mut pkt = header(0, 1, 0, 0);
    let owner = name(&["_googlecast", "_tcp", "local"]);
    let mut rec = owner.clone();
    rec.extend_from_slice(&TYPE_PTR.to_be_bytes());
    rec.extend_from_slice(&1u16.to_be_bytes());
    rec.extend_from_slice(&120u32.to_be_bytes());
    rec.extend_from_slice(&2u16.to_be_bytes());

    pkt.extend(rec);
    pkt.extend([0xFF, 0xFF]); // offset 0x3FFF = 16383

    assert_eq!(parse_packet(&pkt), Err(DnsError::PointerOutOfBounds(16383)));
}

// ---------------------------------------------------------------------------
// Malformed packets
// ---------------------------------------------------------------------------

#[test]
fn truncated_packets_never_panic() {
    for len in 0..32 {
        let data: Vec<u8> = (0..len).map(|i| i as u8 * 7).collect();
        let _ = parse_packet(&data);
    }
}

#[test]
fn every_prefix_of_golden_packet_is_safe() {
    let golden = from_hex(GOLDEN);
    for cut in 0..golden.len() {
        // Must not panic; only the full packet is expected to parse.
        let _ = parse_packet(&golden[..cut]);
    }
    assert_eq!(golden.len(), 177);
    assert!(parse_packet(&golden).is_ok());
}

#[test]
fn empty_packet_is_rejected() {
    assert!(parse_packet(&[]).is_err());
    assert!(parse_packet(&[0, 0, 0, 0]).is_err());
}

#[test]
fn rdlength_overruns_packet_is_rejected() {
    let mut pkt = header(0, 1, 0, 0);
    let mut rec = name(&["_googlecast", "_tcp", "local"]);
    rec.extend_from_slice(&TYPE_PTR.to_be_bytes());
    rec.extend_from_slice(&1u16.to_be_bytes());
    rec.extend_from_slice(&120u32.to_be_bytes());
    rec.extend_from_slice(&1000u16.to_be_bytes()); // rdlength beyond packet
    rec.extend_from_slice(&[0x00]);
    pkt.extend(rec);
    assert!(parse_packet(&pkt).is_err());
}

#[test]
fn reserved_label_bits_are_rejected() {
    // 0x40 has the reserved high bits of the old EDNS0 label type.
    let mut pkt = header(0, 1, 0, 0);
    let mut rec = name(&["_googlecast", "_tcp", "local"]);
    rec.extend_from_slice(&TYPE_PTR.to_be_bytes());
    rec.extend_from_slice(&1u16.to_be_bytes());
    rec.extend_from_slice(&120u32.to_be_bytes());
    rec.extend_from_slice(&1u16.to_be_bytes());
    pkt.extend(rec);
    pkt.push(0x40);
    assert_eq!(parse_packet(&pkt), Err(DnsError::BadLabelLength(0x40)));
}

#[test]
fn label_longer_than_available_bytes_is_rejected() {
    let mut pkt = header(0, 1, 0, 0);
    let mut rec = name(&["_googlecast", "_tcp", "local"]);
    rec.extend_from_slice(&TYPE_PTR.to_be_bytes());
    rec.extend_from_slice(&1u16.to_be_bytes());
    rec.extend_from_slice(&120u32.to_be_bytes());
    rec.extend_from_slice(&1u16.to_be_bytes());
    pkt.extend(rec);
    pkt.push(0x3F); // claims 63 bytes, none follow
    assert_eq!(parse_packet(&pkt), Err(DnsError::Truncated(pkt.len())));
}

#[test]
fn non_utf8_labels_are_decoded_lossily() {
    let mut pkt = header(0, 0, 0, 1);
    pkt.extend([0x02, 0xFF, 0xFF, 0x00]); // owner name with invalid UTF-8
    pkt.extend_from_slice(&TYPE_A.to_be_bytes());
    pkt.extend_from_slice(&1u16.to_be_bytes());
    pkt.extend_from_slice(&120u32.to_be_bytes());
    pkt.extend_from_slice(&4u16.to_be_bytes());
    pkt.extend_from_slice(&[10, 0, 0, 5]);
    assert!(parse_packet(&pkt).is_ok());
}

#[test]
fn correlate_with_no_records_yields_nothing() {
    assert!(correlate(&[]).is_empty());
}

#[test]
fn dns_error_has_stable_variants() {
    assert_eq!(
        parse_packet(&[0, 0, 0, 0, 0, 0]),
        Err(DnsError::Truncated(6))
    );
    assert_eq!(GOOGLECAST_SERVICE, "_googlecast._tcp.local");
}
