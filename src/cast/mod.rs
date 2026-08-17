// SPDX-License-Identifier: MIT OR Apache-2.0
//! Hand-rolled Google Cast V2 engine: mDNS discovery, TLS transport,
//! length-prefix framing, protobuf codec, request correlation, namespace
//! messages, and the connection lifecycle. Owned by `03-cast-engine.md`.

pub mod connection;
pub mod framing;
pub mod mdns;
pub mod namespaces;
pub mod proto;
pub mod request_id;
pub mod tls;
