# 01 — Architecture Specification

## 1. Purpose

Define the architecture for a Rust desktop application that discovers Google Cast receivers, sends media/control commands to a selected receiver, serves local or proxied media over HTTP, and streams captured display frames through an external `ffmpeg` process.

## 2. Hard constraints

| ID | Requirement |
|---|---|
| ARCH-001 | The application SHALL be implemented in Rust. |
| ARCH-002 | The application SHALL use Rust 2024. |
| ARCH-003 | The Rust codebase SHALL enforce `#![forbid(unsafe_code)]`. |
| ARCH-004 | The GUI SHALL be built with `egui` through `eframe`. |
| ARCH-005 | The Cast engine SHALL be hand-rolled rather than delegated to `rust-cast`, `mdns`, or `prost`. |
| ARCH-006 | mDNS discovery, TLS wrapping, and CastV2 Protobuf serialization SHALL be implemented by the application. |
| ARCH-007 | Media delivery SHALL use a lightweight local HTTP server. |
| ARCH-008 | Screen encoding SHALL avoid unsafe C FFI in the Rust process and SHALL use an external `ffmpeg` child process. |
| ARCH-009 | GUI rendering SHALL remain decoupled from asynchronous backend work. |

## 3. Domain boundaries

### 3.1 GUI layer

- Runs on the main thread.
- Owns immediate-mode rendering and user-facing state.
- Communicates with backend functionality through asynchronous channels.
- Uses `eframe` as the native framework.

### 3.2 Custom Cast engine

Owns:

- mDNS discovery
- Cast receiver metadata extraction
- TCP connection establishment
- TLS wrapping
- CastV2 framing
- hand-rolled Protobuf serialization
- Cast namespace JSON messages
- receiver launch
- heartbeat traffic

### 3.3 Media proxy and pipeline

Owns:

- local HTTP listener
- local-file byte serving
- HTTP Range handling
- remote URL proxying
- screen-capture frame ingestion
- `ffmpeg` subprocess management
- encoded media streaming

## 4. Dependency policy

The overview explicitly excludes heavyweight or protocol-specific dependencies for the Cast implementation, including:

- `rust-cast`
- `mdns`
- `prost`

The overview identifies these implementation technologies:

- `eframe` / `egui`
- `rfd`
- Tokio
- `rustls`
- `reqwest`
- `xcap`
- system `ffmpeg`

The exact dependency versions are **TBD**.

## 5. Safety model

The Rust application SHALL contain no unsafe code.

Complex or unsafe-native media encoding work SHALL be isolated in the external `ffmpeg` process. Rust communicates with it through standard process pipes.

## 6. High-level data flow

```text
User
  |
  v
egui / eframe GUI
  |
  | async command channel
  v
Backend runtime
  |----------------------|
  v                      v
Cast Engine          Media Pipeline
  |                      |
  | TLS/CastV2           | HTTP
  v                      v
Chromecast <--------- Local media URL
                         ^
                         |
                 local file / remote URL /
                 screen capture + ffmpeg
```

## 7. Explicit non-goals from the overview

The overview does not define:

- persistence
- authentication UI
- user accounts
- receiver grouping
- playlists
- subtitles
- DRM handling beyond proxy motivation
- application update mechanism
- packaging/installers
- telemetry
- logging format
- exact error UX
- exact platform support matrix
- exact dependency versions

The exact HTTP API schema was not defined by the overview; it is now specified in `04-media-proxy.md`.

Those not listed above remain TBD.
