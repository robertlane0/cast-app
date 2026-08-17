// SPDX-License-Identifier: MIT OR Apache-2.0
//! Media proxy: local HTTP/1.1 server, Range handling, local-file streaming,
//! remote-URL proxying, anonymous network-share streaming, LAN IP selection,
//! and source switching.
//! Owned by `04-media-proxy.md`.

pub mod flush;
pub mod lan_ip;
pub mod local_file;
pub mod mime;
pub mod range;
pub mod server;
pub mod smb_source;
pub mod source;
pub mod url_proxy;
