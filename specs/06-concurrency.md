# 06 — Concurrency Specification

## 1. Goals

Keep GUI rendering responsive while discovery, Cast communication, HTTP serving, and screen capture run concurrently.

## 2. Execution domains

### Main thread

Owned exclusively by `egui` rendering at approximately 60 FPS.

Responsibilities:

- render dashboard;
- read/update GUI state;
- drain `BackendEvent`s via non-blocking `try_recv`;
- enqueue backend commands.

It SHALL NOT perform blocking network or media operations.

### Tokio runtime

A multi-threaded Tokio runtime handles asynchronous I/O.

It contains at least these logical tasks:

#### Task A — mDNS

- listens for UDP multicast discovery traffic;
- parses receiver advertisements;
- owns the authoritative receiver list and pushes snapshots to the GUI via `BackendEvent::ReceiversUpdated`.

#### Task B — Cast TLS connection

- manages the receiver TLS connection;
- sends periodic heartbeat `PING` messages;
- handles incoming receiver status JSON;
- performs the reconnect policy on failure.

#### Task C — HTTP proxy

- accepts local HTTP connections;
- serves local files;
- proxies remote URLs;
- exposes screen-encoded output.

### Screen capture thread

A dedicated standard thread polls display frames because OS capture calls may block.

It passes captured data toward the asynchronous media pipeline through a bounded channel (drop-oldest on overflow).

## 3. Communication boundaries

The GUI SHALL communicate with the backend using asynchronous `tokio::sync::mpsc` unbounded channels:

- **downward** — `UnboundedSender<AppCommand>` from the GUI to the backend;
- **upward** — `UnboundedSender<BackendEvent>` from the backend to the GUI.

The types are defined in the GUI specification. The GUI SHALL poll the upward channel with `try_recv` each frame; it SHALL NOT await it.

Channel topology:

```text
GUI (command_tx) ---AppCommand---> Backend tasks
GUI (event_rx)   <---BackendEvent--- Backend tasks
```

## 4. Data ownership

Authoritative state lives in the backend; the GUI holds a mirror updated by events.

```text
GUI state (mirror)
   |
   | command message
   v
backend task(s)
   |
   +--> Cast engine       (owns receiver list, connection, status)
   |
   +--> Media proxy       (owns active source, HTTP server)
   |
   +--> Screen pipeline   (owns capture thread, ffmpeg process)
```

- The mDNS task owns the authoritative `Vec<CastDevice>` and broadcasts `BackendEvent::ReceiversUpdated` snapshots.
- The Cast connection task owns connection state and emits `ReceiverConnected` / `ReceiverDisconnected` / `MediaStatus` / `Volume` events.
- The media pipeline owns the active source and emits `StreamError`.
- No task mutates another task's state; all state changes cross task boundaries as messages.

## 5. Supervision and cancellation

- A supervisor task SHALL own the runtime graph. A fatal failure of the mDNS task (e.g. multicast socket setup) or the Cast connection task emits `BackendEvent::ConnectionError` and SHALL halt dependent tasks; non-fatal failures (e.g. a socket read error) SHALL be logged and the task SHALL continue or restart per its policy.
- All tasks, the capture thread and the `ffmpeg` bridge SHALL observe a shared shutdown signal (`tokio::sync::watch` channel).
- Dropping the GUI (application exit) SHALL trigger shutdown, releasing the TLS socket, the HTTP listener and the `ffmpeg` child process through their `Drop` implementations.

## 6. Responsiveness requirements

- [x] GUI rendering remains independent of network latency.
- [x] mDNS parsing does not block GUI rendering.
- [x] TLS I/O does not block GUI rendering.
- [x] HTTP serving does not block GUI rendering.
- [x] Screen capture polling does not run on the GUI thread.
- [x] Backends communicate through explicit asynchronous boundaries.
- [x] The GUI polls events with `try_recv` each frame.
- [x] Shutdown is coordinated across all tasks and the capture thread.
