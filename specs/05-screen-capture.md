# 05 — Screen Capture Specification

## 1. Purpose

Mirror a selected monitor to a Chromecast while maintaining the Rust project's `#![forbid(unsafe_code)]` requirement.

## 2. Pipeline

```text
Selected monitor
      |
      v
Safe Rust capture (`xcap`-like crate)
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

- use a 100% safe Rust capture crate such as `xcap`;
- select the monitor chosen by the GUI;
- repeatedly capture RGBA byte frames;
- run capture polling on a dedicated `std::thread::spawn` thread because OS capture APIs can block.

The exact capture crate/version, frame dimensions discovery, pixel layout verification, monitor enumeration API and capture error behavior are **TBD**.

## 4. ffmpeg subprocess

Rust SHALL launch `ffmpeg` using `std::process::Command`.

The overview's representative configuration is:

```text
-f rawvideo
-pix_fmt rgba
-s 1920x1080
-r 30
-i -
-c:v libx264
-preset ultrafast
-f mp4
-movflags frag_keyframe+empty_moov
pipe:1
```

The subprocess SHALL:

- read raw RGBA frames from stdin;
- encode H.264 using `libx264` as specified by the overview example;
- emit fragmented MP4;
- write the encoded stream to stdout.

The exact frame size SHALL correspond to the selected monitor rather than being assumed to be `1920x1080` unless implementation requirements establish otherwise. This is an implementation detail the overview leaves open.

## 5. Bridge

A Tokio task SHALL continuously receive captured RGBA buffers and write them to the `ffmpeg` process stdin.

## 6. HTTP output

The HTTP server SHALL asynchronously read `ffmpeg` stdout and stream the encoded bytes as a continuous `video/mp4` response to the Chromecast.

## 7. Safety boundary

The Rust application SHALL not embed unsafe C bindings for video encoding.

The native encoding implementation is intentionally isolated in the external OS-level `ffmpeg` process.

## 8. Lifecycle requirements

The overview does not define complete startup, shutdown, broken-pipe, `ffmpeg`-missing, encoder-exit, backpressure, dropped-frame or capture-failure behavior. These are **TBD** and SHALL be specified before production implementation.

## 9. Acceptance criteria

- [ ] Capture occurs on a dedicated standard thread.
- [ ] Captured frames are RGBA buffers.
- [ ] Rust starts `ffmpeg` as a child process.
- [ ] Frames are written to `ffmpeg` stdin.
- [ ] `ffmpeg` emits fragmented MP4 on stdout.
- [ ] H.264 encoding is performed by the child process.
- [ ] Encoded bytes can be streamed by the local HTTP server.
- [ ] No unsafe Rust or unsafe C-FFI encoder integration is introduced.
