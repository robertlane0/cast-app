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
- malformed packet handling.

Exact malformed-input policy is **TBD**.

### Protobuf/framing

Test:

- protocol version encoding;
- string field encoding;
- payload type encoding;
- UTF-8 payload;
- exact 4-byte big-endian length prefix.

### HTTP Range handling

Test:

- valid byte range;
- partial-content response;
- requested byte boundaries;
- invalid range behavior (**TBD**);
- missing range header.

### GUI state

Test state transitions for:

- receiver selection;
- source-tab selection;
- display selection;
- local-file selection;
- URL entry;
- transport command dispatch.

## 4. Integration tests

A Cast-capable local network is required for full end-to-end verification.

Minimum scenarios:

1. Discover a receiver.
2. Connect using TLS.
3. Send `CONNECT`.
4. Maintain heartbeat.
5. Launch Default Media Receiver.
6. Serve a local file.
7. Seek using an HTTP Range request.
8. Proxy a remote media URL.
9. Capture a display.
10. Encode frames through `ffmpeg`.
11. Stream the resulting fMP4 to the receiver.
12. Dispatch play/pause/stop/volume commands.

## 5. Dependency audit

The final build SHALL be checked for:

- accidental unsafe code;
- prohibited Cast crates;
- expected runtime dependencies;
- availability of the external `ffmpeg` executable.

The overview does not define the project's exact dependency lockfile or CI toolchain; those are **TBD**.

## 6. Definition of done

The implementation is considered aligned with this specification set when all stated requirements are implemented and verified, all explicit TBDs required for production are resolved, and no architecture decision violates the safety/dependency constraints.
