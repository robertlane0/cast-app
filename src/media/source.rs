//! The active media source model (`04-media-proxy.md` §1.2): exactly one
//! source serves `/stream`, and switching sources terminates any in-flight
//! connection via a per-generation cancellation token.

use std::path::PathBuf;

/// The single active `/stream` source (`04-media-proxy.md` §1.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActiveSource {
    /// Serve a local file.
    File(PathBuf),
    /// Proxy a remote URL.
    Url(String),
    /// Serve the screen-capture encoder output. The payload is informational
    /// (monitor name); the byte stream itself is attached to the server
    /// separately (Phase 8).
    Screen(String),
}

impl ActiveSource {
    /// A human-readable label for logs and UI.
    pub fn label(&self) -> String {
        match self {
            ActiveSource::File(path) => path.display().to_string(),
            ActiveSource::Url(url) => url.clone(),
            ActiveSource::Screen(monitor) => format!("screen: {monitor}"),
        }
    }

    /// Whether this source serves a live (continuous, unknown-length)
    /// stream rather than a seekable resource.
    pub fn is_live(&self) -> bool {
        matches!(self, ActiveSource::Screen(_))
    }
}
