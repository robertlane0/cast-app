# 07 — Requirements, Acceptance Criteria & Test Matrix

## 1. Functional requirements

| ID | Requirement | Verification |
|---|---|---|
| FR-001 | Discover Chromecast receivers via `_googlecast._tcp.local`. | Integration test on a Cast-capable LAN |
| FR-002 | Extract receiver IP, port and friendly name. | DNS parser tests |
| FR-003 | Establish TLS to a receiver using `rustls`. | Cast integration test |
| FR-004 | Accept receiver self-signed certificates using the specified custom verifier. | TLS integration test |
| FR-005 | Encode CastV2 messages with a 4-byte big-endian length prefix. | Unit test |
| FR-006 | Hand-roll required Protobuf serialization without `prost`. | Golden-vector/unit tests |
| FR-007 | Send Cast connection `CONNECT`. | Protocol integration test |
| FR-008 | Send heartbeat `PING` every 5 seconds. | Timed integration test |
| FR-009 | Launch Default Media Receiver app `CC1AD845`. | Receiver integration test |
| FR-010 | Serve local media through a local HTTP URL. | HTTP integration test |
| FR-011 | Support HTTP byte ranges for local media. | Range-response tests |
| FR-012 | Proxy remote URL GET responses. | Proxy integration test |
| FR-013 | Capture a selected display using safe Rust capture. | Capture integration test |
| FR-014 | Feed RGBA frames to `ffmpeg` stdin. | Pipeline test |
| FR-015 | Produce H.264 fragmented MP4 from `ffmpeg`. | Encoder/process test |
| FR-016 | Stream encoded output through HTTP as `video/mp4`. | End-to-end pipeline test |
| FR-017 | Expose Display, Local File and Web URL source choices. | GUI test/manual verification |
| FR-018 | Provide Play, Pause, Stop and Volume controls. | GUI/protocol integration test |
| FR-019 | Decode incoming CastV2 frames and tolerate unknown Protobuf fields. | Unit test |
| FR-020 | Send media-namespace `LOAD` with the local proxy URL. | Protocol integration test |
| FR-021 | Correlate responses to requests by `requestId`. | Unit test |
| FR-022 | Advertise the local HTTP endpoint using a LAN IP reachable by the receiver. | HTTP/integration test |
| FR-035 | Bind the media server to the interface address resolved for the selected receiver (exposure limited to the receiver's LAN segment); rebind on receiver change. | `http_e2e` `set_bind_addr` tests |
| FR-036 | The `0.0.0.0` wildcard bind is gated on explicit user consent (yes/no pop-up); the server starts unbound and `SetProxyPort` cannot create a listener without a bind address. | `runtime_tests` consent round-trip + `gui_state_tests` prompt tests |
| FR-023 | Backend state changes reach the GUI through a non-blocking event channel. | GUI test |
| FR-024 | Screen-capture backpressure drops the oldest frame instead of blocking. | Pipeline test |
| FR-025 | A missing `ffmpeg` executable disables the Display source with an error. | Process test/manual |
| FR-026 | Invalid HTTP ranges return `416`; multi-range headers are ignored with `200`. | Range-response tests |
| FR-027 | Configure the proxy port from the Settings UI. | GUI test/integration |
| FR-033 | Serve anonymous `smb://host/share/path` network-share URLs through `/stream` with the §3.1 Range policy. | `smb_tests` (fakes) + feature-gated `smb_e2e` |
| FR-034 | SMB is anonymous-only: URLs with userinfo are rejected; a server rejecting the guest logon fails with `401`; no credentials exist in the app. | `smb_tests` parse/serve tests |
| FR-037 | TOFU certificate pinning: store the SHA-256 of the first-seen receiver certificate per receiver key (mDNS TXT `id=`, else `friendlyName+IP`), persisted across restarts; warn — never block — on a mismatch in later connections, and never re-pin silently. | `tofu` unit tests + `tls_e2e` TOFU lifecycle + `gui_state_tests` security-notice tests |
| FR-038 | Manual `IP[:port]` connection without mDNS (`02-gui.md` §3.1): `ManualConnect(SocketAddr)` with `parse_manual_addr` / `DEFAULT_CAST_PORT` and `CastDevice::from_manual_addr`. | `gui_state_tests` manual-connect tests + `runtime_tests` handling |

## 2. Safety and architecture requirements

| ID | Requirement | Verification |
|---|---|---|
| SAF-001 | Rust source SHALL enforce `#![forbid(unsafe_code)]`. | Compile/lint/build check |
| SAF-002 | No `rust-cast`, `mdns`, or `prost` dependency SHALL be used for the stated Cast implementation. | Dependency audit |
| SAF-003 | Video encoding SHALL be isolated in an external `ffmpeg` process. | Architecture/code review |
| SAF-004 | GUI SHALL remain decoupled from async backend I/O. | Architecture/code review |
| SAF-005 | Screen capture polling SHALL run on a dedicated standard thread. | Runtime/code review |

## 3. Unit-test targets

### DNS parser

Test:

- valid PTR/SRV/TXT/A records;
- extraction of IP;
- extraction of port;
- extraction of friendly name;
- extraction of the TXT `id=` device identifier (TOFU pin key);
- malformed packet handling (discarded and logged, never panicking, per `03-cast-engine.md` §2.3);
- compression-pointer handling.

### Protobuf/framing

Test:

- protocol version encoding;
- string field encoding;
- payload type encoding;
- UTF-8 payload;
- exact 4-byte big-endian length prefix;
- decoder round-trip for the full field set;
- decoder tolerance of unknown fields;
- rejection of frames over the 16 MiB limit.

### Request-ID correlation

Test:

- monotonic `requestId` assignment;
- response correlation by `requestId`;
- 5-second response timeout behavior.

### Event channel

Test:

- `BackendEvent` messages are delivered to the GUI;
- `try_recv` polling drains all pending events without blocking.

### HTTP Range handling

Test:

- valid byte range;
- partial-content response;
- requested byte boundaries;
- unsatisfiable range returns `416` with `Content-Range: bytes */<size>`;
- multi-range header ignored, full body returned as `200`;
- missing range header;
- `HEAD` returns headers without a body.

### SMB serving (FR-033, FR-034)

Test against `SmbConnector`/`SmbFile` fakes:

- `smb://host/share/path` URL parsing: percent-decoding, non-default ports, host requirement;
- rejection of userinfo URLs (`smb://user:pass@...`), query/fragment, missing share or file path, and empty path segments;
- `200` full-body, `206` single/open-ended/suffix ranges, `416` unsatisfiable, `HEAD` without body;
- the `Range` header is translated into positioned reads (`read_at(offset, len)`);
- guest-logon rejection → `401`, missing resource → `404`, access denied → `403`, transport failure → `502`, invalid URL → `400`;
- a read failure mid-stream aborts the connection;
- feature-gated `smb_e2e` integration tests exercise the real `smb2` client against a guest-accessible share (`SMB_E2E_SERVER`/`SHARE`/`PATH`, optional `SMB_E2E_AUTH_SERVER` for the 401 path).

### LAN IP selection

Test:

- subnet match against the selected receiver;
- fallback to the default-route interface;
- loopback fallback with a warning.

### Media-server binding

Test:

- `set_bind_addr` restricts the listener to one interface: the same port no longer accepts on other interfaces (loopback-only listener, LAN-address connect refused);
- a failed bind (occupied address) is reported through the ack and releases the old listener; a retry on a free address re-establishes service;
- the unbound constructor ignores `SetPort` until the first `SetBindAddr` (no listener can be created behind the consent gate);
- the runtime consent round-trip: startup `BindFallbackRequested` event, decline still binds the receiver's resolved interface, later consent re-enables Play's advertised port.

### MIME detection

Test the extension-based map for known video/audio types and the `application/octet-stream` default.

### Screen pipeline

Test:

- BGRA-to-RGBA conversion;
- drop-oldest backpressure on a full channel;
- segment-aware encoded-byte backpressure: slow consumer receives only whole fMP4 segments, encoder restart emits a fresh init before any new fragments;
- `ffmpeg` discovery on `PATH`;
- graceful shutdown sends EOF, then kills after the timeout.

### TOFU pinning (FR-037)

Test:

- receiver key construction: TXT `id=` wins over `friendlyName+IP`; an empty `id=` falls back;
- first-seen certificate is pinned and stored; the identical certificate matches afterwards;
- a different certificate reports both digests as a mismatch, never blocks, and keeps the original pin;
- pins persist across store reloads; a missing/corrupt store file or malformed entries degrade to an empty store;
- end-to-end (real rustls handshakes): Pinned → Matched → Mismatch → still-Matched;
- GUI: `CertificateWarning` shows a sticky security notice that success events do not auto-dismiss.

### GUI state

Test state transitions for:

- receiver selection and manual `IP[:port]` connection (`ManualConnect`);
- source-tab selection;
- display selection;
- local-file selection;
- URL entry (including `smb://` validity and userinfo rejection);
- transport command dispatch;
- status-indicator updates from backend events;
- proxy-port setting validation and dispatch;
- wildcard-bind consent prompt (`BindFallbackRequested` / `BindFallback`).

## 4. Integration tests

A Cast-capable local network is required for full end-to-end verification.

Minimum scenarios:

1. Discover a receiver.
2. Connect using TLS.
3. Send `CONNECT`.
4. Maintain heartbeat.
5. Launch Default Media Receiver.
6. Correlate the `LAUNCH` response by `requestId` and extract `transportId`.
7. Send a media-namespace `LOAD` with the local proxy URL to the media destination (`transport-<transportId>` on Chromecast, raw `transportId` UUID on Android TV) after an explicit `CONNECT` to that destination.
8. Serve a local file.
9. Seek using an HTTP Range request.
10. Proxy a remote media URL.
11. Advertise the proxy endpoint using a LAN IP reachable by the receiver.
12. Capture a display.
13. Encode frames through `ffmpeg`.
14. Stream the resulting fMP4 to the receiver.
15. Dispatch play/pause/stop/volume commands.
16. Verify backpressure drops frames instead of blocking capture.
17. Verify a slow consumer receives only whole fMP4 segments (no partial boxes on the wire).
18. Serve a guest-accessible SMB share through `/stream` (feature-gated `smb_e2e`; requires an SMB server on the LAN).

## 5. Dependency audit

The final build SHALL be checked for:

- accidental unsafe code;
- prohibited Cast crates;
- expected runtime dependencies;
- availability of the external `ffmpeg` executable.

Policy:

- `Cargo.lock` SHALL be committed.
- CI SHALL run `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`, and `cargo build` on stable Rust across the supported OS matrix (per `01-architecture.md` §8, §9).

## 6. Definition of done

The implementation is considered aligned with this specification set when all stated requirements are implemented and verified and no architecture decision violates the safety/dependency constraints.

TBDs required for production are resolved and reflected in these specifications. The mandatory set is:

- Cast protocol: CastMessage field schema, inbound decoder, `requestId` correlation, source/destination IDs (incl. `receiver-0` initial `CONNECT` and UUID-aware `media_destination_id`), media-namespace `LOAD` with pre-`CONNECT` and transport controls, heartbeat/reconnect policy.
- HTTP proxy: bind address and port (interface-restricted, wildcard only after user consent), LAN-IP advertisement, route structure, MIME map, response headers, range policy, `HEAD` support, remote redirect/timeout/error policy, SSRF posture, anonymous SMB share serving (`04-media-proxy.md` §4.4).
- Screen capture: capture crate, pixel-format conversion, resolution handling, `ffmpeg` discovery and lifecycle, backpressure, shutdown.
- Concurrency: channel crate, event channel, data ownership, supervision and cancellation.
- Platform: OS support matrix, toolchain, dependency pinning, `ffmpeg` install strategy, CI gate.

No TBDs remain in this specification set; every decision is documented in its owning document.
