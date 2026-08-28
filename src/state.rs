// SPDX-License-Identifier: MIT OR Apache-2.0
//! Shared GUI/backend state types: `CastDevice`, `SourceTab`, `AppCommand`,
//! `BackendEvent`. Owned by `02-gui.md` §4.1.

use std::net::SocketAddr;
use std::path::PathBuf;

/// A discovered Chromecast receiver (`02-gui.md` §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CastDevice {
    /// Stable identifier, e.g. `IP:port`.
    pub id: String,
    /// Friendly name (mDNS TXT `fn=`).
    pub name: String,
    /// Receiver TCP address (mDNS SRV port plus A record).
    pub addr: SocketAddr,
    /// TOFU certificate-pin key (`03-cast-engine.md` §3.1): the mDNS TXT
    /// `id=` when advertised, else `friendlyName+IP`.
    pub tofu_key: String,
}

impl CastDevice {
    /// Manual receiver at `addr` without mDNS (e.g. `127.0.0.1:18009` via
    /// `adb forward` to the Android TV emulator). The `id`/`tofu_key` are
    /// the `IP:port` string so pinning survives restarts like `CAST_E2E_RECEIVER`.
    pub fn from_manual_addr(addr: SocketAddr) -> Self {
        let id = addr.to_string();
        Self {
            id: id.clone(),
            name: format!("Manual {id}"),
            addr,
            tofu_key: id,
        }
    }
}

/// Default Cast receiver port (`03-cast-engine.md` §2.4: usually `8009`).
pub const DEFAULT_CAST_PORT: u16 = 8009;

/// Parse a manual `IP` + optional `port` string into a `SocketAddr`.
///
/// `ip` must be a valid IPv4/IPv6 literal (no hostnames, to keep TOFU and
/// `select_lan_ip` deterministic). `port_str` may be empty (defaults to
/// `DEFAULT_CAST_PORT`) or a decimal `1..=65535`; `0` is rejected because the
/// Cast receiver never listens on `0`. As a convenience, `ip` may already
/// contain a `:port` suffix (e.g. `127.0.0.1:18009` from `adb forward`) when
/// `port_str` is empty; that form is parsed as a `SocketAddr` directly.
pub fn parse_manual_addr(ip: &str, port_str: &str) -> Result<SocketAddr, String> {
    let ip = ip.trim();
    let port_str = port_str.trim();
    if ip.is_empty() {
        return Err("IP address is required".to_string());
    }
    // Convenience: `IP:port` in the IP field when the port field is empty.
    // This covers the common `adb forward` copy-paste `127.0.0.1:18009`.
    if port_str.is_empty() && ip.contains(':') {
        if let Ok(addr) = ip.parse::<SocketAddr>() {
            if addr.port() == 0 {
                return Err("port must be 1..=65535".to_string());
            }
            return Ok(addr);
        }
        // For bare IPv6 without brackets (e.g. `::1`) the above fails; fall
        // through to the strict IP parse which will succeed for `::1` and
        // default the port.
    }
    let parsed_ip: std::net::IpAddr = ip
        .parse()
        .map_err(|_| format!("invalid IP address: {ip}"))?;
    let port = if port_str.is_empty() {
        DEFAULT_CAST_PORT
    } else {
        let p: u16 = port_str
            .parse()
            .map_err(|_| format!("invalid port: {port_str}"))?;
        if p == 0 {
            return Err("port must be 1..=65535".to_string());
        }
        p
    };
    Ok(SocketAddr::new(parsed_ip, port))
}

/// Source selection tab (`02-gui.md` §3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceTab {
    Display,
    LocalFile,
    WebUrl,
}

/// Commands sent from the GUI to the backend over the unbounded command
/// channel (`02-gui.md` §4.1).
#[derive(Debug, Clone, PartialEq)]
pub enum AppCommand {
    SelectReceiver(CastDevice),
    /// Manually connect to a receiver at `IP[:port]` without mDNS
    /// (`CAST_E2E_RECEIVER` equivalent for the desktop GUI). The port defaults
    /// to `8009` when omitted. Used for the Android TV emulator at
    /// `127.0.0.1:18009` via `adb forward tcp:18009 tcp:8009`.
    ManualConnect(SocketAddr),
    SelectSource(SourceTab),
    SelectDisplay(String),
    SelectFile(PathBuf),
    SelectUrl(String),
    Play,
    Pause,
    Stop,
    SetVolume(f32), // 0.0 ..= 1.0
    Mute(bool),
    SetProxyPort(u16),
    /// Re-run mDNS discovery (GUI Error-state retry action, `02-gui.md` §3.1).
    Rescan,
    /// Answer the `BindFallbackRequested` consent prompt: `true` allows the
    /// media server to bind `0.0.0.0` (all interfaces) as a fallback
    /// (`04-media-proxy.md` §1.1).
    BindFallback(bool),
}

/// Events received from the backend over the unbounded event channel
/// (`02-gui.md` §4.1).
#[derive(Debug, Clone, PartialEq)]
pub enum BackendEvent {
    ReceiversUpdated(Vec<CastDevice>),
    DisplaysUpdated(Vec<String>),
    ReceiverConnected(CastDevice),
    ReceiverDisconnected(CastDevice),
    ConnectionError(String),
    StreamError(String),
    MediaStatus {
        playing: bool,
        buffering: bool,
    },
    Volume {
        level: f32,
        muted: bool,
    },
    /// The media server could not (or cannot yet) bind the interface
    /// address resolved for the current receiver; the backend wants to
    /// fall back to `0.0.0.0` and asks the user for explicit consent
    /// (`04-media-proxy.md` §1.1). The payload explains what failed and why
    /// the fallback is needed; answered via `AppCommand::BindFallback`.
    BindFallbackRequested(String),
    /// TOFU pin mismatch (`03-cast-engine.md` §3.1): the certificate the
    /// receiver presented differs from the one first seen for this device.
    /// The connection proceeds (warn, not block); the payload explains what
    /// changed and what it may mean.
    CertificateWarning(String),
}
