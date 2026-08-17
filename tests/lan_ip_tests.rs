// SPDX-License-Identifier: MIT OR Apache-2.0
//! LAN IP selection tests (`04-media-proxy.md` §1.1).
//! Subnet-match precedence, default-route fallback, and loopback
//! exclusion. Gate: `cargo test --test lan_ip_tests`.

#![forbid(unsafe_code)]

use std::net::{IpAddr, Ipv4Addr};

use cast_app::media::lan_ip::{Iface, select_lan_ip_from};

fn iface(name: &str, ip: [u8; 4], netmask: [u8; 4]) -> Iface {
    Iface {
        name: name.to_string(),
        ip: Ipv4Addr::from(ip),
        netmask: Ipv4Addr::from(netmask),
    }
}

fn v4(ip: [u8; 4]) -> Ipv4Addr {
    Ipv4Addr::from(ip)
}

// ---------------------------------------------------------------------------
// Rule 1: the interface whose subnet contains the receiver wins
// ---------------------------------------------------------------------------

#[test]
fn receiver_subnet_match_beats_default_route() {
    let interfaces = vec![
        iface("eth0", [192, 168, 1, 10], [255, 255, 255, 0]),
        iface("wlan0", [10, 0, 0, 5], [255, 0, 0, 0]),
    ];
    assert_eq!(
        select_lan_ip_from(
            &interfaces,
            Some(v4([10, 0, 0, 200])),
            Some(v4([192, 168, 1, 10]))
        ),
        Some(IpAddr::V4(v4([10, 0, 0, 5])))
    );
}

#[test]
fn subnet_match_uses_cidr_bits() {
    // /24 interface cannot reach a /16-only address range.
    let interfaces = vec![
        iface("eth0", [192, 168, 1, 10], [255, 255, 255, 0]),
        iface("lan", [172, 16, 7, 1], [255, 255, 0, 0]),
    ];
    assert_eq!(
        select_lan_ip_from(&interfaces, Some(v4([172, 16, 42, 9])), None),
        Some(IpAddr::V4(v4([172, 16, 7, 1])))
    );
    assert_eq!(
        select_lan_ip_from(&interfaces, Some(v4([172, 17, 1, 1])), None),
        None
    );
}

// ---------------------------------------------------------------------------
// Rule 2: default-route interface is the fallback
// ---------------------------------------------------------------------------

#[test]
fn default_route_wins_when_no_subnet_matches() {
    let interfaces = vec![
        iface("eth0", [192, 168, 1, 10], [255, 255, 255, 0]),
        iface("wlan0", [10, 0, 0, 5], [255, 0, 0, 0]),
    ];
    assert_eq!(
        select_lan_ip_from(
            &interfaces,
            Some(v4([172, 16, 3, 9])),
            Some(v4([192, 168, 1, 10]))
        ),
        Some(IpAddr::V4(v4([192, 168, 1, 10])))
    );
}

#[test]
fn loopback_default_route_is_ignored() {
    let interfaces = vec![iface("eth0", [192, 168, 1, 10], [255, 255, 255, 0])];
    assert_eq!(
        select_lan_ip_from(
            &interfaces,
            Some(v4([172, 16, 3, 9])),
            Some(Ipv4Addr::LOCALHOST)
        ),
        None
    );
}

// ---------------------------------------------------------------------------
// Rule 3: loopback is never advertised
// ---------------------------------------------------------------------------

#[test]
fn loopback_interface_is_never_a_candidate() {
    let interfaces = vec![iface("lo", [127, 0, 0, 1], [255, 0, 0, 0])];
    assert_eq!(
        select_lan_ip_from(&interfaces, Some(Ipv4Addr::LOCALHOST), None),
        None
    );
}

#[test]
fn no_candidates_yields_none() {
    assert_eq!(select_lan_ip_from(&[], None, None), None);
    // With no interfaces but a default route, rule 2 still applies.
    assert_eq!(
        select_lan_ip_from(&[], Some(v4([192, 168, 1, 50])), Some(v4([10, 0, 0, 1]))),
        Some(IpAddr::V4(v4([10, 0, 0, 1])))
    );
}

#[test]
fn link_local_addresses_are_candidates_when_they_match() {
    let interfaces = vec![iface("en0", [169, 254, 1, 2], [255, 255, 0, 0])];
    assert_eq!(
        select_lan_ip_from(&interfaces, Some(v4([169, 254, 9, 9])), None),
        Some(IpAddr::V4(v4([169, 254, 1, 2])))
    );
}
