//! Media proxy: local HTTP/1.1 server, Range handling, local-file streaming,
//! remote-URL proxying, LAN IP selection, and source switching.
//! Owned by `04-media-proxy.md`.

pub mod lan_ip;
pub mod local_file;
pub mod mime;
pub mod range;
pub mod server;
pub mod source;
pub mod url_proxy;
