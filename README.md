# cast-app

A zero-unsafe Rust desktop application that discovers Google Cast (Chromecast)
receivers on the LAN, streams local files / remote URLs / anonymous network
shares (SMB) / captured displays to them, and controls playback — with a fully
hand-rolled Cast V2 stack (mDNS, TLS, CastV2 framing, Protobuf) and an external
`ffmpeg` subprocess for video encoding.

- **GUI:** `egui` / `eframe`
- **Async:** Tokio (multi-threaded), `rustls` (ring provider)
- **Encoding:** external `ffmpeg` child process — never in-process
- **Safety:** `#![forbid(unsafe_code)]` across the crate
- **No** `rust-cast`, `mdns`, `prost`, or any C-FFI encoder binding

The full design lives in the specification set under `specs/`
(`01-architecture.md` … `07-requirements-and-tests.md`); `AGENTS.md` is the
canonical implementation guide.

## Features

- mDNS discovery of `_googlecast._tcp.local` receivers (friendly name, IP, port)
  plus a manual `IP[:port]` connection (e.g. `127.0.0.1:18009` via
  `adb forward` to the Android TV emulator) behind the *Manual connection*
  disclosure
- TLS connection with acceptance of receiver self-signed certificates
  (TOFU pinning — SHA-256 of the first-seen certificate per receiver)
- Receiver `LAUNCH` (Default Media Receiver `CC1AD845`), heartbeat keep-alive,
  reconnect with exponential backoff; `CONNECT` targets `receiver-0` for
  compatibility with both Chromecast and Android TV
- Media proxy: local-file serving with HTTP `Range` (200/206/416), remote-URL
  proxying, anonymous SMB share serving (`smb://host/share/file`, guest logon
  only — shares requiring authentication fail with `401`), LAN-IP
  advertisement, configurable port, listener restricted to the selected
  receiver's interface (wildcard `0.0.0.0` only with explicit user consent)
- Screen capture (X11/macOS/Windows via `xcap`, Wayland via the
  xdg-desktop-portal ScreenCast interface + an in-process PipeWire client)
  piped to `ffmpeg` as raw video, H.264 fragmented MP4 streamed live
  (`video/mp4`)
- Play / Pause / Stop / volume / mute controls

## Platform support

| OS | Status | Screen capture |
|---|---|---|
| Linux (X11) | supported | yes |
| Linux (Wayland) | supported | yes — via xdg-desktop-portal ScreenCast + in-process PipeWire client (a single virtual `Screen` display; requires `ffmpeg` and a portal-backed session) |
| Windows 10/11 | supported | yes |
| macOS 13+ | supported | yes |

## Prerequisites

- Rust **stable ≥ 1.85** (edition 2024). Install via
  [rustup](https://rustup.rs) — the repository pins the toolchain in
  `rust-toolchain.toml`.
- **`ffmpeg` on `PATH`** (only required for the Display source; the app
  disables it with a clear error when missing).

## Installing ffmpeg

### Linux (Debian/Ubuntu)

```bash
sudo apt install ffmpeg
```

Other distributions: use the native package manager (`dnf install ffmpeg`,
`pacman -S ffmpeg`, `apk add ffmpeg`, …). On Wayland, screen capture also
needs an xdg-desktop-portal backend (e.g. `xdg-desktop-portal-gnome` or
`xdg-desktop-portal-kde`); a portal-less session disables the Display source
(see platform table).

### Windows

```powershell
# via winget
winget install Gyan.FFmpeg
# or scoop
scoop install ffmpeg
```

Verify `ffmpeg -version` works in a fresh terminal (winget adds FFmpeg to
`PATH`; reopen the terminal if not).

### macOS

```bash
brew install ffmpeg
```

## Building

```bash
cargo build                 # debug
cargo build --release       # release (LTO, stripped, panic=abort)
```

## Running

```bash
cargo run
```

On a LAN with a Chromecast:

1. Wait ~10 s for the receiver to appear in the left panel. If the receiver
   is not discoverable via mDNS (e.g. an Android TV emulator), open the
   **Manual connection** disclosure and enter its `IP[:port]` (e.g.
   `127.0.0.1:18009` via `adb forward tcp:18009 tcp:8009`).
2. At launch, the app asks whether the media server may bind all interfaces
   (`0.0.0.0`) before a receiver is selected — see *Security* below. If you
   decline, the server binds the selected receiver's interface once you pick
   one.
3. Select the receiver (or connect manually); the status dot turns green.
4. **Local File** tab — pick a media file, then **Play**.
5. **Web URL** tab — enter an absolute `http(s)://` media URL or an anonymous
   `smb://host/share/dir/file.mp4` network-share URL (no credentials accepted),
   then **Play**.
6. **Display** tab — pick a monitor (requires `ffmpeg`), then **Play**.
7. Quit — verify no orphan `ffmpeg` process remains.

## Security

The media server normally binds only the network interface used to reach the
selected Chromecast, so its `/stream` endpoint is reachable only by hosts on
the **same LAN segment as the receiver** — the exposure that casting itself
requires. The listener follows receiver selection changes.

Until a receiver is selected (e.g. right after launch) the interface cannot be
determined, so the app asks before falling back to binding all interfaces
(`0.0.0.0`). While bound that way, `/stream` is reachable from **every
interface of the machine** — the LAN, other Wi-Fi networks, and any VPN tunnel
or virtual adapter (with a VPN active, the tunnel interface is part of the
exposure). The fallback is never the default; it happens only with your
explicit yes, and once granted it stays permitted for the session. Decline it
if you don't need to cast before selecting a receiver.

## Configuration

| Setting | Default | Notes |
|---|---|---|
| `CAST_APP_LOG` | `info` | Log filter (`debug`, `warn`, `cast_app=debug`, …). Falls back to `RUST_LOG`, then `info`. |
| Proxy port (Settings UI) | `8080` | Valid range `1024..=65535`; the advertised `/stream` URL rebinds on the current bind address (the selected receiver's interface, or `0.0.0.0` after consent). |

Logs go to the console (INFO and above) and to `cast-app.log` in the platform
log directory:

- Linux: `$XDG_STATE_HOME/cast-app/logs` (or `~/.local/state/cast-app/logs`)
- macOS: `~/Library/Logs/cast-app`
- Windows: `%LOCALAPPDATA%\cast-app\logs`
- Fallback: `<temp>/cast-app/logs`

## Testing

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all
cargo test --doc
cargo run -p xtask              # zero `unsafe` tokens in src/, tests/, xtask/
cargo deny check                   # license + ban list audit
```

Real-Chromecast and real-SMB end-to-end tests are feature-gated and ignored
by default:

```bash
cargo test --features e2e-cast -- --ignored --test-threads=1
# SMB (requires a guest-accessible share)
SMB_E2E_SERVER=host:445 SMB_E2E_SHARE=guest-share SMB_E2E_PATH=dir/video.mp4 \
  cargo test --features e2e-smb --test smb_e2e -- --ignored --test-threads=1
```

Manual receivers can be exercised without mDNS via the left-panel **Manual
connection** disclosure, or in `cast_e2e` via `CAST_E2E_RECEIVER=IP:port`.

## Repository layout

```
src/
  main.rs        entrypoint: logging, runtime, GUI launch
  lib.rs         crate root
  state.rs       GUI/backend shared types (CastDevice, AppCommand incl. ManualConnect, BackendEvent)
  app.rs         egui dashboard (Manual connection disclosure, try_manual_connect)
  runtime.rs     tokio runtime + supervisor (ManualConnect handling)
  util/          shutdown token, retry backoff, drop-oldest channels
  cast/          mDNS, TLS (+ TOFU pinning), framing, hand-rolled protobuf, connection state machine
  media/         HTTP proxy: local files, URL proxy, SMB shares, Range, MIME, LAN IP
  screen/        xcap capture thread, Wayland portal/PipeWire client, ffmpeg subprocess, capture→ffmpeg→HTTP bridge
tests/
  unit + integration test suites (incl. feature-gated cast_e2e and smb_e2e)
  support/fake_encoder.rs  cross-platform fake encoder binary (ISS-012)
xtask/           unsafe-scan binary (CI gate)
specs/           01–07 implementation specifications
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.