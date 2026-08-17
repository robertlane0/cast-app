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
    MediaStatus { playing: bool, buffering: bool },
    Volume { level: f32, muted: bool },
}
