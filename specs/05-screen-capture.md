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

On Linux Wayland sessions, `xcap` capture is not reliably available; the Display source SHALL be disabled with an explanatory error per the platform policy in `01-architecture.md` §8.

### 3.1 Pixel format

`xcap` returns frames in BGRA byte order on current versions. The capture thread SHALL convert each frame to RGBA before writing it to the pipeline, matching the documented `-pix_fmt rgba` input. The exact byte order SHALL be verified against the pinned crate version at implementation time.

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

## 5. Bridge

The bridge SHALL continuously receive captured RGBA buffers and write them to the `ffmpeg` process stdin.

- The capture thread -> bridge boundary SHALL use a bounded channel; when full, the oldest pending frame is dropped in favor of freshness.
- stdin and stdout I/O SHALL run on dedicated standard threads so the Tokio runtime and GUI are never blocked.

## 6. HTTP output

The HTTP server SHALL asynchronously read `ffmpeg` stdout and stream the encoded bytes as a continuous `video/mp4` response to the Chromecast.

- Encoded bytes SHALL be forwarded through a bounded channel; when full, the oldest chunk is dropped (accepting a transient glitch under slow network conditions).
- Closing the HTTP connection ends the session as described in §4.2.

## 7. Safety boundary

The Rust application SHALL not embed unsafe C bindings for video encoding.

The native encoding implementation is intentionally isolated in the external OS-level `ffmpeg` process.

## 8. Audio

Screen mirroring SHALL be video-only. Audio capture and muxing are a documented non-goal for this release.

## 9. Acceptance criteria

- [ ] Capture occurs on a dedicated standard thread.
- [ ] Captured frames are converted to RGBA byte order.
- [ ] Frame size is derived from the selected monitor's resolution.
- [ ] Rust starts `ffmpeg` as a child process.
- [ ] Frames are written to `ffmpeg` stdin.
- [ ] `ffmpeg` is discovered on `PATH`, and a missing binary disables the Display source.
- [ ] `ffmpeg` emits fragmented MP4 on stdout with the `moov` atom written up-front.
- [ ] The fMP4 flag set is validated against a real receiver during integration.
- [ ] H.264 encoding is performed by the child process.
- [ ] Unexpected `ffmpeg` exit stops the pipeline and surfaces an error.
- [ ] Shutdown sends EOF, then kills the process after a timeout.
- [ ] Backpressure drops the oldest frame rather than blocking capture.
- [ ] Encoded bytes can be streamed by the local HTTP server.
- [ ] No unsafe Rust or unsafe C-FFI encoder integration is introduced.
