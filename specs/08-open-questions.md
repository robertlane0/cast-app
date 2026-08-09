# 08 — Open Questions / TBD Register

This document records decisions not supported by the project overview. Each entry is either resolved and referenced to the defining specification, or remains open for implementation.

## Resolved decisions

### Cast protocol

| Decision | Specified in |
|---|---|
| CastMessage field numbers and schema | `03-cast-engine.md` §5 |
| Inbound CastMessage decoder, unknown-field tolerance, 16 MiB frame cap | `03-cast-engine.md` §4.2, §5 |
| JSON schema for `CONNECT`, `LAUNCH`, media commands and transport controls | `03-cast-engine.md` §6 |
| Source/destination IDs for every message | `03-cast-engine.md` §6.0 |
| Request ID generation and correlation (monotonic u32, 5 s timeout) | `03-cast-engine.md` §6.0 |
| Receiver status parsing model | `03-cast-engine.md` §6.3 |
| PING/PONG timeout (10 s) and reconnect policy (backoff, 5 attempts) | `03-cast-engine.md` §6.2, §7.1 |
| Connection teardown and reconnection strategy | `03-cast-engine.md` §7, §7.1 |
| Media namespace `LOAD` and transport controls | `03-cast-engine.md` §6.4 |
| Stop semantics (media `STOP`; teardown via `STOP_APP`) | `03-cast-engine.md` §6.4, §7 |

### mDNS

| Decision | Specified in |
|---|---|
| DNS parser grammar and record variants | `03-cast-engine.md` §2.3 |
| Compression-pointer handling (depth cap, cycle guard) | `03-cast-engine.md` §2.3 |
| Record correlation by instance name | `03-cast-engine.md` §2.4 |
| Query destination and refresh interval (10 s) | `03-cast-engine.md` §2.1, §2.2 |
| Device expiration (~30 s) and de-duplication | `03-cast-engine.md` §2.5 |
| IPv4-only policy | `03-cast-engine.md` §2.5 |
| Multicast socket error behavior | `03-cast-engine.md` §2.5 |

### TLS

| Decision | Specified in |
|---|---|
| `rustls` 0.23.x with `ring`; version pinned at implementation | `03-cast-engine.md` §3 |
| Certificate-verifier policy (accept-any after full handshake; pinning deferred) | `03-cast-engine.md` §3.1 |
| Handshake timeout (5 s) and shutdown (`close_notify`) | `03-cast-engine.md` §3.2 |

### HTTP proxy

| Decision | Specified in |
|---|---|
| Bind address and configurable port (`0.0.0.0:8080`) | `04-media-proxy.md` §1.1 |
| Route structure (single `/stream`) | `04-media-proxy.md` §1.1 |
| LAN-IP advertisement for the proxy endpoint | `04-media-proxy.md` §1.1 |
| MIME detection | `04-media-proxy.md` §3.2 |
| Response headers | `04-media-proxy.md` §3.2 |
| Range syntax, invalid-range (`416`), multi-range (ignore, `200`) | `04-media-proxy.md` §3.1 |
| `HEAD` support | `04-media-proxy.md` §3.2 |
| Remote redirects (5), first-byte timeout (30 s), status pass-through, `502` | `04-media-proxy.md` §4.2 |
| Remote header forwarding (`Range` only) | `04-media-proxy.md` §4.1 |
| Remote authentication (none; URL userinfo rejected) | `04-media-proxy.md` §4.2 |
| Security posture: LAN exposure intentional; SSRF note | `04-media-proxy.md` §1.1, §4.3 |

### Screen capture

| Decision | Specified in |
|---|---|
| Capture crate (`xcap`, pinned at implementation) | `05-screen-capture.md` §3 |
| Monitor enumeration by name | `05-screen-capture.md` §3 |
| Dynamic resolution handling (restart `ffmpeg`) | `05-screen-capture.md` §3.2 |
| Frame rate (30 fps) | `05-screen-capture.md` §3 |
| Pixel format (BGRA-to-RGBA conversion) | `05-screen-capture.md` §3.1 |
| Frame-drop/backpressure (bounded channel, drop-oldest) | `05-screen-capture.md` §5, §6 |
| `ffmpeg` discovery and missing-binary behavior | `05-screen-capture.md` §4.1 |
| Unexpected encoder exit | `05-screen-capture.md` §4.2 |
| Shutdown sequence (EOF, then kill after 5 s) | `05-screen-capture.md` §4.2 |
| HTTP stream lifecycle (open until source switch/stop/disconnect) | `04-media-proxy.md` §5 |

### GUI and concurrency

| Decision | Specified in |
|---|---|
| Receiver-list row contents | `02-gui.md` §3.1 |
| Loading/empty/error states | `02-gui.md` §3.1, §3.2 |
| Disabled-state rules | `02-gui.md` §3.2, §3.3 |
| Volume range and control (0..100 mapped to 0.0..1.0, throttled) | `02-gui.md` §3.3 |
| File-type filters | `02-gui.md` §3.2 |
| URL validation | `02-gui.md` §3.2 |
| User-facing error reporting | `02-gui.md` §3.1, §4.1 |
| Channel crate (`tokio::sync::mpsc` unbounded only) | `02-gui.md` §2, `06-concurrency.md` §3 |
| Backend-to-GUI event channel | `02-gui.md` §4, `06-concurrency.md` §3 |
| Data ownership (backend authoritative, GUI mirror) | `02-gui.md` §4.2, `06-concurrency.md` §4 |
| Supervision and cancellation (watch channel, `Drop`-based teardown) | `06-concurrency.md` §5 |

### Security

| Decision | Specified in |
|---|---|
| TLS verifier accepts any certificate; pinning is future hardening | `03-cast-engine.md` §3.1 |
| HTTP proxy intentionally reachable by every LAN host | `04-media-proxy.md` §1.1 |
| Remote proxying is user-initiated only; no URL-driven SSRF surface | `04-media-proxy.md` §4.3 |
| URL credentials (userinfo) rejected | `04-media-proxy.md` §4.2 |
| Local file access restricted to the user-selected active source | `04-media-proxy.md` §1.2 |

## Still open

These do not block production implementation and may be decided during implementation or iteration.

### Capture and encoding

- [ ] fMP4 start-of-stream tuning (`empty_moov` baseline; verify moov/`ftyp` handshake against a real receiver).
- [ ] Supported operating systems for `xcap` and `ffmpeg`.

### GUI

- [ ] Exact visual design and styling.
- [ ] Status-indicator design (playback state, connection state).
- [ ] Application settings UI (e.g. proxy port).

### Build and release

- [ ] Supported OS/version matrix.
- [ ] Rust toolchain version.
- [ ] Full dependency version matrix.
- [ ] `ffmpeg` distribution/install strategy.
- [ ] Packaging and installer format.
- [ ] CI configuration.
- [ ] Release signing.

## Traceability note

These questions are not omissions from the source conversion; they are explicit boundaries where the source overview does not provide enough information to specify an implementation detail safely. Resolved items now carry a normative definition in the referenced specifications; open items remain intentionally deferred.
