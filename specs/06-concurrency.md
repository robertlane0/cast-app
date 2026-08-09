# 06 — Concurrency Specification

## 1. Goals

Keep GUI rendering responsive while discovery, Cast communication, HTTP serving, and screen capture run concurrently.

## 2. Execution domains

### Main thread

Owned exclusively by `egui` rendering at approximately 60 FPS.

Responsibilities:

- render dashboard;
- read/update GUI state;
- enqueue backend commands.

It SHALL NOT perform blocking network or media operations.

### Tokio runtime

A multi-threaded Tokio runtime handles asynchronous I/O.

It contains at least these logical tasks:

#### Task A — mDNS

- listens for UDP multicast discovery traffic;
- parses receiver advertisements;
- updates available receiver state.

#### Task B — Cast TLS connection

- manages the receiver TLS connection;
- sends periodic heartbeat `PING` messages;
- handles incoming receiver status JSON.

#### Task C — HTTP proxy

- accepts local HTTP connections;
- serves local files;
- proxies remote URLs;
- exposes screen-encoded output.

### Screen capture thread

A dedicated standard thread polls display frames because OS capture calls may block.

It passes captured data toward the asynchronous media pipeline through channels.

## 3. Communication boundaries

The GUI SHALL communicate with the backend using asynchronous channels.

The overview gives the following representative channel:

```rust
tokio::sync::mpsc::UnboundedSender<AppCommand>
```

Crossbeam `mpsc` channels are also identified as an acceptable approach.

The exact channel topology and message enum definitions are **TBD**.

## 4. Data ownership

The design SHOULD preserve a clear ownership boundary:

```text
GUI state
   |
   | command message
   v
backend task(s)
   |
   +--> Cast engine
   |
   +--> Media proxy
   |
   +--> Screen pipeline
```

The overview does not define shared-state primitives, cancellation tokens or task supervision. Those are **TBD**.

## 5. Responsiveness requirements

- [ ] GUI rendering remains independent of network latency.
- [ ] mDNS parsing does not block GUI rendering.
- [ ] TLS I/O does not block GUI rendering.
- [ ] HTTP serving does not block GUI rendering.
- [ ] Screen capture polling does not run on the GUI thread.
- [ ] Backends communicate through explicit asynchronous boundaries.
