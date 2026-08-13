#![forbid(unsafe_code)]

//! LAN IP selection for the advertised proxy endpoint
//! (`04-media-proxy.md` §1.1): the interface whose subnet contains the
//! receiver's IP; falling back to the default-route interface; falling back
//! to `127.0.0.1` with a warning.

use std::net::{IpAddr, Ipv4Addr, UdpSocket};

/// A simplified interface descriptor decoupled from the platform-specific
/// `if-addrs` types so selection logic is unit-testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Iface {
    /// Interface name, e.g. `wlan0`.
    pub name: String,
    /// IPv4 address.
    pub ip: Ipv4Addr,
    /// IPv4 netmask.
    pub netmask: Ipv4Addr,
}

impl Iface {
    /// Whether `candidate` is inside this interface's subnet.
    fn contains(&self, candidate: Ipv4Addr) -> bool {
        ipv4_netmask(self.ip, self.netmask) == ipv4_netmask(candidate, self.netmask)
    }
}

fn ipv4_netmask(ip: Ipv4Addr, netmask: Ipv4Addr) -> u32 {
    u32::from(ip) & u32::from(netmask)
}

/// Select the LAN IP used to advertise `http://<ip>:<port>/stream`
/// (`04-media-proxy.md` §1.1). Re-run whenever the receiver selection
/// changes (the caller owns that trigger).
///
/// Order: interface whose subnet contains `receiver_ip` → default-route
/// interface → `127.0.0.1` with a `warn!` (the Chromecast cannot reach
/// loopback).
pub fn select_lan_ip(receiver_ip: Option<IpAddr>) -> IpAddr {
    let receiver_v4 = receiver_ip.and_then(|ip| match ip {
        IpAddr::V4(v4) => Some(v4),
        IpAddr::V6(_) => None,
    });

    match if_addrs::get_if_addrs() {
        Ok(interfaces) => {
            let candidates: Vec<Iface> = interfaces
                .iter()
                .filter(|interface| interface.is_oper_up())
                .filter_map(|interface| match &interface.addr {
                    if_addrs::IfAddr::V4(v4) if !v4.ip.is_loopback() => Some(Iface {
                        name: interface.name.clone(),
                        ip: v4.ip,
                        netmask: v4.netmask,
                    }),
                    _ => None,
                })
                .collect();
            if let Some(ip) = select_lan_ip_from(&candidates, receiver_v4, default_route_ip()) {
                return ip;
            }
        }
        Err(error) => {
            tracing::warn!(%error, "failed to enumerate network interfaces");
        }
    }

    tracing::warn!("no usable LAN interface; advertising 127.0.0.1 (unreachable by receivers)");
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

/// Pure selection logic over a provided interface list
/// (`04-media-proxy.md` §1.1).
///
/// `default_route_ip` is used only when no interface subnet contains the
/// receiver; `127.0.0.1` is the final fallback.
pub fn select_lan_ip_from(
    interfaces: &[Iface],
    receiver_ip: Option<Ipv4Addr>,
    default_route_ip: Option<Ipv4Addr>,
) -> Option<IpAddr> {
    if let Some(receiver) = receiver_ip {
        if let Some(matched) = interfaces
            .iter()
            .find(|iface| !iface.ip.is_loopback() && iface.contains(receiver))
        {
            return Some(IpAddr::V4(matched.ip));
        }
    }
    if let Some(route) = default_route_ip {
        if !route.is_loopback() {
            return Some(IpAddr::V4(route));
        }
    }
    None
}

/// Best-effort IP of the interface carrying the default route, via the
/// classic no-packet UDP trick: `connect()` to a public address only
/// consults the routing table. Returns `None` when offline or the route is
/// loopback-only.
pub fn default_route_ip() -> Option<Ipv4Addr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect((Ipv4Addr::new(8, 8, 8, 8), 80)).ok()?;
    let local = socket.local_addr().ok()?;
    match local.ip() {
        IpAddr::V4(v4) if !v4.is_loopback() => Some(v4),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iface(name: &str, ip: Ipv4Addr, netmask: Ipv4Addr) -> Iface {
        Iface {
            name: name.to_string(),
            ip,
            netmask,
        }
    }

    #[test]
    fn subnet_match_wins_over_default_route() {
        let interfaces = vec![
            iface(
                "eth0",
                Ipv4Addr::new(192, 168, 1, 10),
                Ipv4Addr::new(255, 255, 255, 0),
            ),
            iface(
                "wlan0",
                Ipv4Addr::new(10, 0, 0, 5),
                Ipv4Addr::new(255, 0, 0, 0),
            ),
        ];
        let ip = select_lan_ip_from(
            &interfaces,
            Some(Ipv4Addr::new(10, 0, 0, 200)),
            Some(Ipv4Addr::new(192, 168, 1, 10)),
        );
        assert_eq!(ip, Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))));
    }

    #[test]
    fn default_route_wins_without_subnet_match() {
        let interfaces = vec![
            iface(
                "eth0",
                Ipv4Addr::new(192, 168, 1, 10),
                Ipv4Addr::new(255, 255, 255, 0),
            ),
            iface(
                "wlan0",
                Ipv4Addr::new(10, 0, 0, 5),
                Ipv4Addr::new(255, 0, 0, 0),
            ),
        ];
        let ip = select_lan_ip_from(
            &interfaces,
            Some(Ipv4Addr::new(172, 16, 3, 9)),
            Some(Ipv4Addr::new(192, 168, 1, 10)),
        );
        assert_eq!(ip, Some(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10))));
    }

    #[test]
    fn loopback_default_route_is_not_used_without_subnet_match() {
        let interfaces = vec![iface(
            "eth0",
            Ipv4Addr::new(192, 168, 1, 10),
            Ipv4Addr::new(255, 255, 255, 0),
        )];
        let ip = select_lan_ip_from(&interfaces, None, Some(Ipv4Addr::LOCALHOST));
        assert_eq!(ip, None);
    }

    #[test]
    fn no_candidates_yields_none() {
        assert_eq!(select_lan_ip_from(&[], None, None), None);
    }

    #[test]
    fn loopback_interface_is_not_a_candidate() {
        let interfaces = vec![iface(
            "lo",
            Ipv4Addr::LOCALHOST,
            Ipv4Addr::new(255, 0, 0, 0),
        )];
        let ip = select_lan_ip_from(&interfaces, Some(Ipv4Addr::LOCALHOST), None);
        assert_eq!(ip, None);
    }
}
