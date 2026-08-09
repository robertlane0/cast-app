# 08 — Open Questions / TBD Register

This document intentionally records decisions not supported by the project overview. They should be resolved before treating the specifications as implementation-complete.

## Cast protocol

- [ ] Exact CastMessage Protobuf field numbers and complete schema.
- [ ] Decoder requirements for incoming CastMessage payloads.
- [ ] Complete JSON schema for `CONNECT`, `LAUNCH`, media commands and transport controls.
- [ ] Source/destination IDs for every Cast message.
- [ ] Request ID generation and correlation.
- [ ] Receiver status parsing model.
- [ ] PING/PONG timeout and reconnect behavior.
- [ ] Connection teardown behavior.
- [ ] Reconnection strategy after network loss.

## mDNS

- [ ] DNS parser grammar and supported record variants.
- [ ] Packet compression-pointer handling.
- [ ] Discovery refresh interval.
- [ ] Device expiration behavior.
- [ ] Duplicate-device reconciliation.
- [ ] IPv6 support or explicit IPv4-only policy.
- [ ] Multicast socket error behavior.

## TLS

- [ ] Exact `rustls` version/API.
- [ ] Certificate-verifier implementation details.
- [ ] TLS handshake timeout.
- [ ] TLS shutdown behavior.

## HTTP proxy

- [ ] Exact listen address and configurable port.
- [ ] Route structure.
- [ ] MIME type detection.
- [ ] HTTP response headers.
- [ ] Range syntax supported.
- [ ] Invalid-range behavior.
- [ ] HEAD support.
- [ ] Remote redirects.
- [ ] Remote request/header forwarding.
- [ ] Remote authentication model.
- [ ] Remote request timeouts.
- [ ] Remote response error handling.
- [ ] Security policy for exposing a local HTTP server to the LAN.

## Screen capture

- [ ] Exact capture crate and version.
- [ ] Supported operating systems.
- [ ] Monitor enumeration behavior.
- [ ] Dynamic resolution handling.
- [ ] Frame rate selection.
- [ ] Pixel format guarantees.
- [ ] Frame-drop/backpressure policy.
- [ ] `ffmpeg` executable discovery.
- [ ] Behavior when `ffmpeg` is missing.
- [ ] Behavior when encoder exits unexpectedly.
- [ ] Shutdown sequence.
- [ ] Exact HTTP stream lifecycle.

## GUI

- [ ] Exact visual design.
- [ ] Receiver list row contents.
- [ ] Loading/empty/error states.
- [ ] Disabled-state rules.
- [ ] Volume range and UI control.
- [ ] File type filters.
- [ ] URL validation.
- [ ] User-facing error reporting.
- [ ] Status indicators.
- [ ] Application settings.

## Build and release

- [ ] Supported OS/version matrix.
- [ ] Rust toolchain version.
- [ ] Dependency versions.
- [ ] `ffmpeg` distribution/install strategy.
- [ ] Packaging and installer format.
- [ ] CI configuration.
- [ ] Release signing.

## Security

The overview's self-signed certificate acceptance and LAN HTTP-server design are intentional architecture points, but the threat model is not specified.

Resolve:

- [ ] Whether the TLS verifier accepts any certificate or pins discovered receiver identity.
- [ ] Whether the HTTP proxy is intentionally reachable by every LAN host.
- [ ] Whether remote URL proxying permits arbitrary outbound URLs.
- [ ] Whether SSRF protections are required.
- [ ] Whether URL credentials may be supplied.
- [ ] Whether local file paths require additional access restrictions.

## Traceability note

These questions are not omissions from the source conversion; they are explicit boundaries where the source overview does not provide enough information to specify an implementation detail safely.
