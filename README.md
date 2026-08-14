# cast-app

A zero-unsafe Rust desktop application that discovers Google Cast (Chromecast)
receivers on the LAN, streams local files / remote URLs / captured displays to
them, and controls playback — with a fully hand-rolled Cast V2 stack (mDNS,
TLS, CastV2 framing, Protobuf) and an external `ffmpeg` subprocess for video
encoding.

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
- TLS connection with acceptance of receiver self-signed certificates
- Receiver `LAUNCH` (Default Media Receiver `CC1AD845`), heartbeat keep-alive,
  reconnect with exponential backoff
- Media proxy: local-file serving with HTTP `Range` (200/206/416), remote-URL
  proxying, LAN-IP advertisement, configurable port
- Screen capture (X11/macOS/Windows) piped to `ffmpeg` as raw RGBA, H.264
  fragmented MP4 streamed live (`video/mp4`)
- Play / Pause / Stop / volume / mute controls

## Platform support

| OS | Status | Screen capture |
|---|---|---|
| Linux (X11) | supported | yes |
| Linux (Wayland) | supported | no — Display source disabled with an explanatory error |
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
`pacman -S ffmpeg`, `apk add ffmpeg`, …). X11 session required for screen
capture; on Wayland, capture is disabled by design (see platform table).

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

1. Wait ~10 s for the receiver to appear in the left panel.
2. Select it; the status dot turns green.
3. **Local File** tab — pick a media file, then **Play**.
4. **Web URL** tab — enter an absolute `http(s)://` media URL, then **Play**.
5. **Display** tab — pick a monitor (requires `ffmpeg`), then **Play**.
6. Quit — verify no orphan `ffmpeg` process remains.

## Configuration

| Setting | Default | Notes |
|---|---|---|
| `CAST_APP_LOG` | `info` | Log filter (`debug`, `warn`, `cast_app=debug`, …). Falls back to `RUST_LOG`, then `info`. |
| Proxy port (Settings UI) | `8080` | Valid range `1024..=65535`; the advertised `/stream` URL rebinds. |

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
./scripts/forbid-unsafe-check.sh   # zero `unsafe` tokens in src/
cargo deny check                   # license + ban list audit
```

Real-Chromecast end-to-end tests are feature-gated and ignored by default:

```bash
cargo test --features e2e-cast -- --ignored --test-threads=1
```

## Repository layout

```
src/
  main.rs        entrypoint: logging, runtime, GUI launch
  lib.rs         crate root
  state.rs       GUI/backend shared types
  app.rs         egui dashboard
  runtime.rs     tokio runtime + supervisor
  util/          shutdown token, retry backoff, drop-oldest channels
  cast/          mDNS, TLS, framing, hand-rolled protobuf, connection state machine
  media/         HTTP proxy: local files, URL proxy, Range, MIME, LAN IP
  screen/        xcap capture thread, ffmpeg subprocess, capture→ffmpeg→HTTP bridge
specs/           01–07 implementation specifications
```

## License

MIT © 2026 Robert Lane.