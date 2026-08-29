## 1. Architectural Philosophy & Constraints

To meet strict safety and dependency requirements, the application is divided into three isolated domains:

1. **The GUI Layer (`egui`):** An immediate-mode frontend built with `eframe`. It runs on the main thread and communicates with the backend via asynchronous channels.
2. **The Custom Cast Engine:** A completely hand-rolled implementation of the Google Cast (V2) protocol. It natively handles mDNS discovery, TLS socket wrapping, and Protocol Buffer serialization without relying on crates like `rust-cast`, `mdns`, or `prost`.
3. **The Media Proxy & Pipeline:** A lightweight local HTTP server handling local file serving, remote URL proxying, and screen capture streaming. To satisfy the `#![forbid(unsafe_code)]` mandate, screen encoding eschews unsafe FFI C-bindings in favor of piping raw desktop frames into a child `ffmpeg` process and capturing its output.

---

## 2. The GUI Layer (`egui`)

Rust 2024’s improved lifetime elision and async closures integrate beautifully with `egui`. The app utilizes `eframe` (the native `egui` framework) and maintains a decoupled state model.

### Interface Layout

The UI is divided into a centralized dashboard with three primary panels:

* **Target Selection (Left Panel):** Continuously updates a list of discovered Chromecast devices on the local network, plus a *Manual connection* disclosure for direct `IP[:port]` entry when mDNS is unavailable (e.g. the Android TV emulator via `adb forward`).
* **Source Selection (Center Panel):** A tabbed interface allowing the user to select their streaming source:
* *Display:* A dropdown listing available monitors (e.g., `DP-1`, `HDMI-2`).
* *Local File:* A native file picker button (via `rfd`, a safe Rust file dialog crate) for video/audio.
* *Web URL:* A text input for a remote media URL.


* **Transport Controls (Bottom Bar):** Play, pause, stop, and volume controls, which dispatch commands to the selected Cast receiver.

### State Management

Because `egui` is an immediate-mode GUI, we avoid blocking the UI thread by using Tokio's unbounded channels. The GUI struct simply holds the current state and a sender queue:

```rust
struct CastDashboard {
    available_receivers: Vec<CastDevice>,
    selected_receiver: Option<CastDevice>,
    source_tab: SourceTab,
    displays: Vec<String>,

    // Command channel to the async runtime
    command_tx: tokio::sync::mpsc::UnboundedSender<AppCommand>,
    // Backend event channel, drained with `try_recv` each frame
    event_rx: tokio::sync::mpsc::UnboundedReceiver<BackendEvent>,

    // ... plus mirrored backend state (discovery/connection/playback
    // status, volume throttle, error banner, security warning, settings,
    // manual IP/port fields) — see `src/app.rs`
}
```

---

## 3. The Custom "Zero-Dependency" Cast Engine

Chromecast communication relies on a specific sequence: mDNS discovery -> TLS Connection -> Protobuf framing -> JSON messaging. We will build this entirely in safe Rust.

### 3.1 mDNS Discovery (UDP Multicast)

Instead of importing a heavyweight mDNS crate, we implement a lightweight DNS parser.

1. The app binds a `std::net::UdpSocket` to `0.0.0.0:0` and joins the IPv4 multicast address `224.0.0.251`.
2. It sends a standard DNS query for the PTR record `_googlecast._tcp.local`.
3. The engine safely parses the incoming binary DNS response packet, extracting the device's IP address from the `A` record, the port (usually `8009`) from the `SRV` record, and the friendly device name from the `TXT` record.

When mDNS is unavailable (e.g. an Android TV emulator), the left-panel
*Manual connection* bypasses discovery: the IP (or `IP:port`) entered there is
validated by `parse_manual_addr` (`DEFAULT_CAST_PORT = 8009`) and dispatched
as `ManualConnect`, creating a `CastDevice::from_manual_addr` that flows
through the same TLS and CastV2 path.

### 3.2 Transport Layer & Security

Chromecast requires TLS without strict certificate validation (as devices use self-signed certs). We use `rustls` (a pure-Rust, safe TLS implementation). We configure a `ClientConfig` with a custom safe certificate verifier that accepts all certificates, wrap our `TcpStream` into a `rustls::StreamOwned` (the `CastTlsStream` alias in `cast/tls.rs`), and establish the connection on port 8009 with a 5-second handshake timeout. Shutdown sends `close_notify` before closing the socket.

### 3.3 Hand-Rolled Protocol Buffers & CastV2 Framing

To avoid adding `prost` and a `build.rs` step for a single message type, we hand-roll the Protobuf serialization. The CastV2 protocol wraps all communication in a single `CastMessage` structure:

1. **Framing:** Every message is prefixed by a 4-byte big-endian `u32` length header.
2. **Serialization:** Using standard Protobuf wire formatting, we write a pure Rust encoder. For example, writing a string field (like `namespace`) is just `[tag_byte, length_byte(s)]` followed by the UTF-8 bytes.

```rust
// Minimal hand-rolled protobuf payload generator for CastMessage.
// The 4-byte BE length prefix is added by the framing layer
// (`cast/framing.rs`), not by this encoder (`cast/proto.rs`).
fn encode_cast_message(source: &str, dest: &str, namespace: &str, payload: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    // 1: protocol_version (0 = CASTV2_1_0)
    buf.extend_from_slice(&[0x08, 0x00]);
    // 5: payload_type (0 = STRING)
    buf.extend_from_slice(&[0x28, 0x00]);
    // 2: source_id, 3: destination_id, 4: namespace, 6: payload_utf8
    //    (length-delimited: [tag, length] + UTF-8 bytes)
    write_length_delimited(&mut buf, 2, source.as_bytes());
    write_length_delimited(&mut buf, 3, dest.as_bytes());
    write_length_delimited(&mut buf, 4, namespace.as_bytes());
    write_length_delimited(&mut buf, 6, payload.as_bytes());
    buf
}
```

### 3.4 Namespaces and Heartbeats

Once connected, the engine handles the three core Chromecast namespaces via JSON:

* `urn:x-cast:com.google.cast.tp.connection`: Sends `{"type": "CONNECT"}` to `receiver-0` to initialize (compatible with both Chromecast and Android TV; the latter rejects `transport-0`), plus an explicit `CONNECT` to the media destination before the first media message.
* `urn:x-cast:com.google.cast.tp.heartbeat`: Runs a background Tokio task sending `{"type": "PING"}` every 5 seconds to keep the TLS socket alive.
* `urn:x-cast:com.google.cast.receiver`: Sends a `LAUNCH` command with the Default Media Receiver App ID (`CC1AD845`).

---

## 4. Media Proxy & Streaming Engine

Chromecasts cannot play local files or easily bypass CORS restrictions on arbitrary URLs. Therefore, our Rust app acts as a **Local Media Proxy**.

Using a lightweight `tokio::net::TcpListener`, we spin up a local HTTP server (e.g., `http://<PC-IP>:8080/stream`). The Cast engine sends this local URL to the Chromecast's Default Media Receiver.

### 4.1 Serving Local Files

When a local file is selected, the HTTP proxy intercepts requests from the Chromecast. It uses Rust's `tokio::fs::File` to read the media. Crucially, it must parse HTTP `Range: bytes=...` headers to allow the Chromecast to seek and buffer properly. It replies with `206 Partial Content` and streams the exact byte chunks safely.

### 4.2 Proxying URLs

When the user inputs a remote URL, the Chromecast often blocks it due to DRM, CORS, or format support.
Our HTTP server acts as a middleman. It uses `reqwest` to initiate a GET request to the target URL from the PC. The incoming byte stream is piped directly into the local HTTP server socket pointing to the Chromecast. This effectively "masks" the origin, allowing the PC to handle authentication or headers while seamlessly feeding standard bytes to the Cast device.

### 4.3 Screen Capture & H.264 fMP4 Pipeline

Achieving cross-platform screen capture and H.264 video encoding without `unsafe` blocks is traditionally the hardest hurdle, as standard video encoders (like `x264` or hardware encoders) rely heavily on unsafe C-FFI.

To strictly enforce `#![forbid(unsafe_code)]`, we adopt a highly efficient **Subprocess Pipelining** architecture:

1. **Safe Capture:** We utilize a 100% safe Rust capture crate (like `xcap`) to grab RGBA byte frames of the selected monitor in a loop. On Linux Wayland sessions, `xcap` is not usable; the app instead runs the xdg-desktop-portal **ScreenCast** D-Bus dance (pure-Rust `zbus`) to obtain a PipeWire stream fd and reads frames with an in-process pure-Rust PipeWire client (`screen/portal.rs`, `screen/pipewire.rs`) — the negotiated pixel format is fed straight to `ffmpeg`, no conversion. A virtual `Screen` entry replaces the monitor list there.
2. **Child Process Encoder:** We spawn an `ffmpeg` child process via `std::process::Command`, configured to accept raw RGBA video from `stdin` and output an fMP4 (Fragmented MP4) stream to `stdout`.
```rust
let mut ffmpeg = Command::new("ffmpeg")
    .args(&[
        "-f", "rawvideo", "-pix_fmt", "rgba",
        "-s", "1920x1080", "-r", "30",
        "-i", "-", // Read from stdin
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-preset", "ultrafast",
        "-tune", "zerolatency",
        "-g", "30", // 1 s keyframe interval (recorded working set)
        "-f", "mp4", "-movflags", "frag_keyframe+empty_moov",
        "pipe:1" // Output to stdout
    ])
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .spawn()?;
```


3. **The Bridge:** A Tokio task constantly takes the RGBA buffers from the screen capturer and writes them to the `ffmpeg` `stdin`.
4. **The HTTP Stream:** Our local HTTP server asynchronously reads from `ffmpeg`'s `stdout` and streams those bytes directly to the Chromecast as a continuous `video/mp4` HTTP response.

This achieves high-performance screen mirroring while entirely offloading the complex, memory-unsafe C-code of video encoding to a separated OS-level process, keeping our Rust codebase immaculately safe.

---

## 5. Concurrency Model

The application strictly divides CPU-bound and IO-bound tasks:

* **Main Thread:** Owned exclusively by `egui` for rendering at 60fps.
* **Tokio Runtime:** A multi-threaded runtime handles the network I/O.
* *Task A:* Listens for UDP mDNS packets.
* *Task B:* Manages the TLS connection (PING/PONG loop, incoming receiver status JSON).
* *Task C:* The HTTP Proxy listener serving media.


* **Screen Capture Thread:** A dedicated standard thread (`std::thread::spawn`) dedicated to polling the display buffer (since OS capture APIs can sometimes block) and feeding the Tokio HTTP pipeline via channels. On Wayland, a second dedicated thread drives the PipeWire loop; the portal dance itself runs on the pipeline's controller thread (a pending share dialog must never block the GUI or the Tokio runtime).

This architecture creates a robust, highly modular Google Cast desktop application entirely from scratch. By wrapping Protobuf and mDNS manually and utilizing standard HTTP/subprocess streams for media, the application drastically minimizes dependency bloat while proving the capability of zero-unsafe Rust.
