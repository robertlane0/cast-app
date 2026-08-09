# 02 — GUI Specification

## 1. Scope

Specify the desktop interface and its decoupled state model.

## 2. Framework

- UI: `egui`
- Native application framework: `eframe`
- UI execution: main thread
- Backend communication: asynchronous channels

## 3. Layout

The application SHALL expose a centralized dashboard with three primary regions.

### 3.1 Target Selection — left panel

The panel SHALL:

- display discovered Chromecast devices;
- continuously update the available receiver list;
- allow selection of a receiver.

The exact device-list row design is **TBD**.

### 3.2 Source Selection — center panel

The source selector SHALL be tabbed.

#### Display tab

- Provide a dropdown.
- Populate it with available monitors.
- Example display names include `DP-1` and `HDMI-2`.

#### Local File tab

- Provide a native file-picker action.
- Use `rfd` for the safe Rust file dialog.
- Support selecting video/audio media.

#### Web URL tab

- Provide a text input for a remote media URL.

The overview does not specify URL validation behavior, accepted schemes, or media-type validation; these are **TBD**.

### 3.3 Transport Controls — bottom bar

Provide controls for:

- Play
- Pause
- Stop
- Volume

Transport commands SHALL be dispatched to the selected Cast receiver.

The exact volume range, slider/button design, command acknowledgements and disabled-state behavior are **TBD**.

## 4. State model

The GUI state SHALL be decoupled from backend execution.

Minimum state represented by the overview:

```rust
#![forbid(unsafe_code)]

struct CastDashboard {
    available_receivers: Vec<CastDevice>,
    selected_receiver: Option<CastDevice>,
    source_tab: SourceTab,
    displays: Vec<String>,
    command_tx: tokio::sync::mpsc::UnboundedSender<AppCommand>,
}
```

The production state model SHALL preserve these concepts:

- available receivers
- selected receiver
- active source tab
- available displays
- asynchronous command sender

Additional fields may be required during implementation, but their semantics are not defined by the overview and should be documented when introduced.

## 5. Responsiveness

The UI thread SHALL NOT block on network or media operations.

Backend work SHALL be dispatched through asynchronous channels and handled by the Tokio runtime or dedicated capture thread as defined in the concurrency specification.

## 6. GUI acceptance criteria

- [ ] Dashboard renders on the main thread.
- [ ] Receiver list can update without blocking UI rendering.
- [ ] User can select a receiver.
- [ ] User can switch among Display, Local File and Web URL source tabs.
- [ ] Display tab exposes available monitors.
- [ ] Local File tab invokes a native picker.
- [ ] Web URL tab accepts a remote media URL.
- [ ] Play, pause, stop and volume controls dispatch backend commands.
- [ ] GUI remains responsive while discovery, networking and media streaming are active.
