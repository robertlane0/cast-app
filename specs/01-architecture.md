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

### 4.1 Toolchain

- The project SHALL build on stable Rust with a minimum of 1.85 (the first stable release to ship edition 2024).
- The toolchain SHALL be pinned to the latest stable release at implementation start via a `rust-toolchain.toml` file.

### 4.2 Version pinning

- Exact dependency versions SHALL be pinned in `Cargo.lock`, which SHALL be committed.
- `Cargo.toml` SHALL record major-version anchors: Tokio `1.x`, `rustls` `0.23.x`, `reqwest` `0.12.x`, and the latest stable `eframe`/`egui`, `rfd`, and `xcap` at implementation time.

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

## 7. Explicit non-goals

The overview does not define the following, and they remain explicit non-goals for this release:

- persistence
- authentication UI
- user accounts
- receiver grouping
- playlists
- subtitles
- DRM handling beyond proxy motivation
- application update mechanism
- telemetry
- logging format
- exact error UX

Two overview gaps are now specified elsewhere in this set: the HTTP API schema in `04-media-proxy.md`, and the platform support matrix in §8.

## 8. Platform support

- Linux (X11): supported.
- Linux (Wayland): supported. `xcap` capture is not reliably available on Wayland sessions (`XDG_SESSION_TYPE == "wayland"` or `WAYLAND_DISPLAY` set); display enumeration instead exposes a single virtual `Screen` entry, and the pipeline runs through the xdg-desktop-portal ScreenCast D-Bus interface plus an in-process PipeWire client (`05-screen-capture.md` §3.4). The Display source is disabled with an explanatory error when `ffmpeg` is missing or the portal service is unreachable.
- Windows 10/11: supported.
- macOS 13+: supported.
- Anything else (e.g. BSD): unsupported.

## 9. Build and release policy

- Development and release builds SHALL run via `cargo build`; this release distributes source only.
- CI SHALL gate merge on `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`, and `cargo build`, running on stable Rust across the supported OS matrix when the repository is hosted on GitHub Actions.
- Packaging, installers, signed binaries, and an application update mechanism are explicitly deferred and SHALL NOT block this release.
