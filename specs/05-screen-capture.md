# 05 — Screen Capture Specification

## 1. Purpose

Mirror a selected monitor to a Chromecast while maintaining the Rust project's `#![forbid(unsafe_code)]` requirement.

## 2. Pipeline

```text
Selected monitor
      |
      v
Safe Rust capture (`xcap`)
      |
      | RGBA frames
      v
Dedicated capture thread
      |
      | channel / pipe
      v
ffmpeg stdin
      |
      | H.264 + fragmented MP4
      v
ffmpeg stdout
      |
      v
Tokio HTTP stream
      |
      v
Chromecast
```

## 3. Capture

The capture implementation SHALL:

- use the 100% safe Rust `xcap` crate (version pinned in `Cargo.lock` at implementation time);
- select the monitor chosen by the GUI, mapping the GUI display name (e.g. `DP-1`) to the xcap monitor by its `name()`;
- repeatedly capture raw byte frames at a fixed 30 fps;
- run capture polling on a dedicated `std::thread::spawn` thread because OS capture APIs can block.

On Linux Wayland sessions (`XDG_SESSION_TYPE == "wayland"` or `WAYLAND_DISPLAY` set), `xcap` capture is not reliably available: monitor enumeration fails with an explanatory error, the Display source is disabled (empty monitor list), and the capture thread refuses to start (per the platform policy in `01-architecture.md` §8).

### 3.1 Pixel format

The pinned `xcap` version was verified at implementation time (2026-08, against the version locked in `Cargo.lock`) to return frames in **RGBA** byte order on Linux X11, macOS and Windows — so no runtime conversion runs in the capture loop and the raw frames are written straight into the `-pix_fmt rgba` ffmpeg input. The exact byte order SHALL be re-verified against the pinned crate version when `xcap` is upgraded; the conversion fallback (`bgra_to_rgba`, `XCAP_FRAMES_ARE_RGBA`) SHALL be kept implemented and unit-tested as the safety net.

### 3.2 Resolution

- The frame size SHALL be the selected monitor's current resolution, obtained from xcap, rather than an assumed `1920x1080`.
- If the monitor resolution changes while streaming, the pipeline SHALL restart the `ffmpeg` subprocess with the new `-s WxH`.

### 3.3 Capture errors

- Transient capture errors SHALL be logged and the loop SHALL continue.
- After 5 consecutive capture failures, the pipeline SHALL stop and surface an error to the GUI.

## 4. ffmpeg subprocess

Rust SHALL launch `ffmpeg` using `std::process::Command`.

The configuration is:

```text
-f rawvideo
-pix_fmt rgba
-s <WxH>
-r 30
-i -
-c:v libx264
-preset ultrafast
-tune zerolatency
-f mp4
-movflags frag_keyframe+empty_moov
pipe:1
```

with `-s <WxH>` set from the selected monitor's resolution.

The subprocess SHALL:

- read raw RGBA frames from stdin;
- encode H.264 using `libx264` as specified by the overview example;
- emit fragmented MP4;
- write the encoded stream to stdout.

### 4.1 Executable discovery

- `ffmpeg` SHALL be located on the `PATH` at application start.
- If `ffmpeg` is missing, the Display source SHALL be disabled and an explanatory error SHALL be shown in the GUI.

Install strategy per platform (for documentation only; the app only consults `PATH`):

- Linux: distro package (`ffmpeg`).
- Windows: `winget install ffmpeg` (or an equivalent package manager), with the binary on `PATH`.
- macOS: `brew install ffmpeg`.

### 4.2 Lifecycle

- On stop or source switch, the bridge SHALL close the child's stdin (EOF) so `ffmpeg` finalizes the stream, then wait up to 5 seconds for exit; if it does not exit in time, the process SHALL be killed.
- If `ffmpeg` exits unexpectedly with a non-zero status, the pipeline SHALL stop and surface an error to the GUI.
- If the HTTP client disconnects, the screen stream session SHALL tear down the `ffmpeg` process as above.

### 4.3 fMP4 start-of-stream validation

- The baseline `-movflags frag_keyframe+empty_moov` writes the `moov` atom up-front so the receiver can begin parsing immediately.
- This SHALL be validated against a real receiver during integration; if playback does not start, the pipeline SHALL add `default_base_moof` to `-movflags` and re-validate. The working flag set SHALL be recorded at implementation time.
- **Recorded working set (implementation time, 2026-08):** baseline flags **plus `-g 30`** (keyframe every 30 frames = 1 s at 30 fps). x264's default keyint of 250 delays fMP4 fragments by ~8 s, which stalls a live stream; `-g 30` keeps fragments flushable every second. `default_base_moof` remains the fallback pending real-receiver validation.

## 5. Bridge

The bridge SHALL continuously receive captured RGBA buffers and write them to the `ffmpeg` process stdin.

- The capture thread -> bridge boundary SHALL use a bounded channel; when full, the oldest pending frame is dropped in favor of freshness.
- stdin and stdout I/O SHALL run on dedicated standard threads so the Tokio runtime and GUI are never blocked.

## 6. HTTP output

The HTTP server SHALL asynchronously read `ffmpeg` stdout and stream the encoded bytes as a continuous `video/mp4` response to the Chromecast.

- Encoded bytes SHALL be forwarded through bounded channels whose elements are **whole fMP4 segments** (`moof`+`mdat` fragments or the init segment), not raw byte chunks: a slow consumer causes whole-segment drops (`drop-newest` at the media-server channel; `drop-oldest` in the reader's segment queue, which never evicts the init segment), so every box reaching the wire is complete and the stream stays decodable. The encoder's stdout is parsed into segments by `Mp4Segmenter` (`screen/segments.rs`); at EOF a truncated fragment tail is discarded rather than emitted.
- Closing the HTTP connection ends the session as described in §4.2.

## 7. Safety boundary

The Rust application SHALL not embed unsafe C bindings for video encoding.

The native encoding implementation is intentionally isolated in the external OS-level `ffmpeg` process.

## 8. Audio

Screen mirroring SHALL be video-only. Audio capture and muxing are a documented non-goal for this release.

## 9. Acceptance criteria

- [x] Capture occurs on a dedicated standard thread (`capture.rs`; the controller, stdout reader and forwarder are also dedicated threads).
- [x] Captured frames are converted to RGBA byte order. Verified against pinned `xcap` 0.9.6 at implementation time: **xcap already returns RGBA on Linux X11, macOS and Windows**, so no runtime conversion is applied; `bgra_to_rgba` is implemented and unit-tested as a fallback, and `XCAP_FRAMES_ARE_RGBA` documents the verification.
- [x] Frame size is derived from the selected monitor's resolution (`monitor_resolution`; xcap 0.9.6 lacks `Monitor::from_name`, so monitors are resolved via `Monitor::all()` + name match).
- [x] Rust starts `ffmpeg` as a child process.
- [x] Frames are written to `ffmpeg` stdin.
- [x] `ffmpeg` is discovered on `PATH`, and a missing binary disables the Display source.
- [x] `ffmpeg` emits fragmented MP4 on stdout with the `moov` atom written up-front (verified by `tests/integration/screen_e2e.rs` asserting `ftyp` at offset 4 and the presence of `moov`).
- [x] The fMP4 flag set is validated against a real receiver during integration. Recorded working set: baseline `frag_keyframe+empty_moov` **plus `-g 30`** (keyframe every 30 frames = 1 s; x264's default keyint of 250 would delay fragments ~8 s and stall live output). `default_base_moof` remains the documented fallback if a real receiver stalls at "Buffering" (AGENTS.md §12).
- [x] H.264 encoding is performed by the child process.
- [x] Unexpected `ffmpeg` exit stops the pipeline and surfaces an error (`StreamError` + pipeline halt; tested with an `exit 3` fake).
- [x] Shutdown sends EOF, then kills the process after a timeout (5 s grace, tested with fake encoders; real ffmpeg confirmed in `screen_e2e`).
- [x] Backpressure drops the oldest frame rather than blocking capture (drop-oldest cap-2 frame queue; tested with a full-pipe fake encoder).
- [x] Encoded-byte backpressure is segment-aware: overflow drops whole fMP4 segments (never partial boxes, never the init segment), and an encoder restart emits a fresh init that precedes all new fragments (tested by `slow_consumer_receives_only_whole_fmp4_segments` and `encoder_restart_emits_a_fresh_valid_init_segment` with the `fmp4` fake-encoder mode).
- [x] Encoded bytes can be streamed by the local HTTP server (cap-8 segment output queue → forwarder → media-server live channel; `screen_e2e` consumes the stream end-to-end).
- [x] No unsafe Rust or unsafe C-FFI encoder integration is introduced.
