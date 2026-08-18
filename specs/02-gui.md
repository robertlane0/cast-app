# 02 — GUI Specification

## 1. Scope

Specify the desktop interface and its decoupled state model.

## 2. Framework

- UI: `egui`
- Native application framework: `eframe`
- UI execution: main thread
- Backend communication: asynchronous `tokio::sync::mpsc` unbounded channels (commands down, events up). Crossbeam is not used.

## 3. Layout

The application SHALL expose a centralized dashboard with three primary regions.

### 3.0 Visual design

- The UI SHALL use egui's default dark theme with no custom skinning.
- Layout: left panel fixed at ~250 px; center panel fills the remaining width; bottom bar ~48 px high.
- Layout proportions are functional defaults and SHALL NOT change the acceptance criteria.

### 3.1 Target Selection — left panel

The panel SHALL:

- display discovered Chromecast devices;
- continuously update the available receiver list;
- allow selection of a receiver.

Each row SHALL show the device friendly name and, in smaller text, its IP:port.

The panel SHALL render explicit states:

- *Scanning* — discovery has started but returned no devices yet;
- *No receivers found* — empty state;
- *Error* — fatal discovery failure (e.g. multicast socket setup), with a retry action.

Selecting a receiver SHALL dispatch `AppCommand::SelectReceiver`.

### 3.2 Source Selection — center panel

The source selector SHALL be tabbed.

#### Display tab

- Provide a dropdown populated with available monitors (`BackendEvent::DisplaysUpdated`).
- The dropdown SHALL be disabled when no monitors are available or when the `ffmpeg` executable is missing (error message shown).
- Selecting a monitor SHALL dispatch `AppCommand::SelectDisplay`.

#### Local File tab

- Provide a native file-picker action using `rfd`.
- File-type filters: video and audio extensions (`mp4`, `mkv`, `mov`, `webm`, `mp3`, `aac`, `m4a`, `flac`, `wav`).
- Selecting a file SHALL dispatch `AppCommand::SelectFile`.

#### Web URL tab

- Provide a text input for a remote media URL.
- A URL is valid when it parses as absolute `http://` or `https://` with a host, or as an anonymous `smb://host/share/path` network-share URL (`04-media-proxy.md` §4.4). The Apply action SHALL be disabled while the input is invalid.
- SMB URLs SHALL be rejected when they carry userinfo (`smb://user:pass@...`) or lack a share and file path; the input hint SHALL advertise anonymous shares (e.g. `smb://nas/share/video.mp4`).
- Submitting a valid URL SHALL dispatch `AppCommand::SelectUrl`.

### 3.3 Transport Controls — bottom bar

Provide controls for:

- Play
- Pause
- Stop
- Volume

Transport commands SHALL be dispatched to the selected Cast receiver.

- Play, Pause and Stop SHALL be disabled when no receiver is selected.
- Play and Pause SHALL additionally be disabled when no source is active.
- Volume SHALL be a slider mapping `0..=100` to a receiver `level` of `0.0..=1.0`, plus a mute toggle.
- Volume changes SHALL be dispatched as `AppCommand::SetVolume`, throttled to at most one message per 100 ms to avoid flooding `SET_VOLUME`.
- Volume is a live slider; the local value SHALL be corrected from `BackendEvent::Volume` when receiver status arrives.

### 3.4 Status indicators

The bottom bar SHALL include a status strip rendering:

- connection state: `Scanning`, `Connected <name>`, or `Disconnected` — rendered as a colored dot (amber = scanning, green = connected, red = disconnected/error) next to the state text;
- playback state from `BackendEvent::MediaStatus`: `Idle`, `Playing`, `Paused`, or `Buffering`;
- a transient error banner showing the most recent `BackendEvent::ConnectionError` or `StreamError`, dismissed on the next successful event or on manual dismiss;
- a security-notice banner on `BackendEvent::CertificateWarning` (TOFU pin mismatch, `03-cast-engine.md` §3.1): unlike the transient error banner it SHALL NOT be auto-dismissed by success events — only manual dismiss clears it, so a security notice cannot blink away during connect.

### 3.5 Settings

- A Settings action (gear button in the top bar) SHALL open an egui modal window.
- The window SHALL expose the proxy port, defaulting to `8080`, validated to the range `1024..=65535`.
- Saving SHALL dispatch `AppCommand::SetProxyPort(u16)`; the backend SHALL rebind the HTTP listener on the new port (keeping the current bind address) and the advertised URL SHALL reflect the change.

### 3.6 Wildcard-bind consent

- On `BackendEvent::BindFallbackRequested(reason)` the GUI SHALL open a modal asking the user (yes/no) to allow the media server to bind `0.0.0.0` (`04-media-proxy.md` §1.1).
- The modal SHALL show the reason (what failed / why the fallback is wanted: no receiver selected yet, or the interface bind failed) and the exposure: while bound to `0.0.0.0`, `/stream` is reachable from every interface of the machine, including VPN tunnels and virtual adapters (e.g. while a VPN is active).
- The answer SHALL dispatch `AppCommand::BindFallback(bool)`; the modal closes on either answer.

## 4. State model

The GUI state SHALL be decoupled from backend execution.

### 4.1 Shared types

```rust
#![forbid(unsafe_code)]

struct CastDevice {
    id: String,          // stable id, e.g. IP:port
    name: String,        // friendly name (TXT fn=)
    addr: std::net::SocketAddr,
    tofu_key: String,    // TOFU pin key: TXT id= when advertised, else friendlyName+IP
}

enum SourceTab {
    Display,
    LocalFile,
    WebUrl,
}
```

Commands sent from the GUI to the backend:

```rust
enum AppCommand {
    SelectReceiver(CastDevice),
    SelectSource(SourceTab),
    SelectDisplay(String),
    SelectFile(std::path::PathBuf),
    SelectUrl(String),
    Play,
    Pause,
    Stop,
    SetVolume(f32), // 0.0 ..= 1.0
    Mute(bool),
    SetProxyPort(u16),
    Rescan, // re-run mDNS discovery (GUI Error-state retry action, §3.1)
}
```

Events received from the backend:

```rust
enum BackendEvent {
    ReceiversUpdated(Vec<CastDevice>),
    DisplaysUpdated(Vec<String>),
    ReceiverConnected(CastDevice),
    ReceiverDisconnected(CastDevice),
    ConnectionError(String),
    StreamError(String),
    MediaStatus { playing: bool, buffering: bool },
    Volume { level: f32, muted: bool },
    BindFallbackRequested(String), // wildcard-bind consent prompt (§3.6)
    CertificateWarning(String),    // TOFU pin mismatch (`03-cast-engine.md` §3.1)
}
```

### 4.2 Dashboard state

```rust
struct CastDashboard {
    available_receivers: Vec<CastDevice>,
    selected_receiver: Option<CastDevice>,
    source_tab: SourceTab,
    displays: Vec<String>,
    command_tx: tokio::sync::mpsc::UnboundedSender<AppCommand>,
    event_rx: tokio::sync::mpsc::UnboundedReceiver<BackendEvent>,
}
```

The GUI owns a mirror of backend state. The authoritative receiver list lives in the discovery task and is pushed to the GUI via `BackendEvent::ReceiversUpdated`; the GUI never mutates the backend's copy.

### 4.3 Event polling

Each frame, before rendering, the GUI SHALL drain `event_rx` with `try_recv` (non-blocking) and apply all pending `BackendEvent`s to the dashboard state. The UI thread never blocks on the channel.

## 5. Responsiveness

The UI thread SHALL NOT block on network or media operations.

Backend work SHALL be dispatched through asynchronous channels and handled by the Tokio runtime or dedicated capture thread as defined in the concurrency specification.

## 6. GUI acceptance criteria

- [x] Dashboard renders on the main thread.
- [x] Receiver list updates without blocking UI rendering.
- [x] User can select a receiver.
- [x] User can switch among Display, Local File and Web URL source tabs.
- [x] Display tab exposes available monitors.
- [x] Local File tab invokes a native picker with media-type filters.
- [x] Web URL tab accepts a validated remote media URL.
- [x] Play, pause, stop and volume controls dispatch backend commands.
- [x] Volume changes are throttled and corrected from receiver status.
- [x] Scanning, empty, error and disabled states are rendered.
- [x] Connection, playback and error status indicators render and update from backend events.
- [x] Settings window opens and applies the proxy port.
- [x] Backend events are polled non-blocking each frame.
- [x] GUI remains responsive while discovery, networking and media streaming are active.
