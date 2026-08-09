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
- A URL is valid when it parses as absolute `http://` or `https://` with a host. The Apply action SHALL be disabled while the input is invalid.
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

## 4. State model

The GUI state SHALL be decoupled from backend execution.

### 4.1 Shared types

```rust
#![forbid(unsafe_code)]

struct CastDevice {
    id: String,          // stable id, e.g. IP:port
    name: String,        // friendly name (TXT fn=)
    addr: std::net::SocketAddr,
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

- [ ] Dashboard renders on the main thread.
- [ ] Receiver list updates without blocking UI rendering.
- [ ] User can select a receiver.
- [ ] User can switch among Display, Local File and Web URL source tabs.
- [ ] Display tab exposes available monitors.
- [ ] Local File tab invokes a native picker with media-type filters.
- [ ] Web URL tab accepts a validated remote media URL.
- [ ] Play, pause, stop and volume controls dispatch backend commands.
- [ ] Volume changes are throttled and corrected from receiver status.
- [ ] Scanning, empty, error and disabled states are rendered.
- [ ] Backend events are polled non-blocking each frame.
- [ ] GUI remains responsive while discovery, networking and media streaming are active.
