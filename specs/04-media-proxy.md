# 04 — Media Proxy Specification

## 1. Purpose

Provide a local HTTP endpoint that a Chromecast can access for media originating from:

1. local files;
2. remote URLs;
3. screen-capture encoding.

The endpoint is:

```text
http://<LAN-IP>:8080/stream
```

### 1.1 Binding and advertisement

- The server SHALL bind `0.0.0.0:8080` so any LAN host (including the Chromecast) can reach it.
- The port SHALL be configurable via application settings, defaulting to `8080`.
- `/stream` is the only route; any other path returns `404`.
- The engine SHALL advertise the endpoint using a local LAN IP selected as follows:
  1. the address of the interface whose subnet contains the selected receiver's IP; otherwise
  2. the address of the interface carrying the default route; otherwise
  3. `127.0.0.1` with a warning, since the Chromecast cannot reach loopback.
- LAN IP selection SHALL re-run when the receiver selection changes.

### 1.2 Active source model

Exactly one source is active at a time. `/stream` serves the active source (local file, remote URL, or screen capture). Switching the active source SHALL terminate any in-flight `/stream` connection and start serving the new source.

## 2. Server implementation

- Use `tokio::net::TcpListener`.
- The server SHALL stream media bytes without requiring the entire media object to be loaded into memory.
- The server SHALL parse the HTTP request line and headers, supporting only `GET` and `HEAD`; other methods return `405`.
- The server SHALL stream responses incrementally; the GUI thread is never involved.

## 3. Local-file serving

When a local file is selected:

1. The HTTP proxy receives the Chromecast request.
2. The server opens the media with `tokio::fs::File`.
3. The server parses the HTTP `Range: bytes=...` request header.
4. It returns `206 Partial Content` for a valid single range.
5. It streams the requested byte range in fixed chunks (64 KiB).

Range support is required because the Chromecast needs seeking and buffering behavior.

### 3.1 Range policy

- No `Range` header -> `200 OK` with full content and `Content-Length`.
- Valid single range (`bytes=a-b`, `bytes=a-`, `bytes=-suffix`) -> `206 Partial Content` with `Content-Range: bytes <start>-<end>/<size>` and `Content-Length: <end>-<start>+1`.
- Multiple ranges -> ignored; the full body is returned as `200 OK`.
- Unsatisfiable or malformed range -> `416 Range Not Satisfiable` with `Content-Range: bytes */<size>`.

### 3.2 Headers

Local-file responses SHALL include:

- `Accept-Ranges: bytes`;
- `Content-Type` from an extension-based MIME map (`mp4` -> `video/mp4`, `webm` -> `video/webm`, `mkv` -> `video/x-matroska`, `mov` -> `video/quicktime`, `mp3` -> `audio/mpeg`, `aac` -> `audio/aac`, `m4a` -> `audio/mp4`, `flac` -> `audio/flac`, `wav` -> `audio/wav`, otherwise `application/octet-stream`);
- `Content-Length` on `200` and `206` responses;
- `Content-Range` on `206` responses;
- `Cache-Control: no-cache`.

`HEAD` requests SHALL return the same headers as `GET` with no body.

## 4. Remote URL proxying

When a Web URL source is active:

1. The Rust application accepts the remote URL, validates it (absolute `http(s)` URL without userinfo) and issues a `GET` via `reqwest` per incoming `/stream` request (a per-request fetch honors each client's `Range` header; invalid URLs get a `400` at request time).
2. The remote response body is streamed into the `/stream` connection.
3. The Chromecast receives the local proxy URL instead of the original remote origin.

### 4.1 Header forwarding

- The `Range` header from the Chromecast SHALL be forwarded to the remote server when present, enabling seek and buffer behavior through the proxy.
- No other client headers are forwarded; the app may add request headers on behalf of the user (none in this release).

### 4.2 Policy

- Redirects: follow up to 5.
- Timeout: 30 s to establish the connection and receive the first bytes; no overall response-time limit while streaming.
- Remote status: non-2xx remote status SHALL be passed through to the Chromecast together with the remote body.
- Remote connection failure SHALL return `502 Bad Gateway` with a plain-text body.
- `Content-Length` is forwarded when the remote provides one; otherwise the body is streamed without `Content-Length` (close-delimited or chunked transfer-encoding).
- No authentication mechanism is included in this release; URLs containing userinfo (`scheme://user:pass@host/`) SHALL be rejected.

### 4.3 SSRF note

The outbound request is initiated by the local desktop user from the UI, not by an incoming HTTP request, so the proxy introduces no URL-driven SSRF surface. The Chromecast only ever receives the local `/stream` URL.

## 5. Screen stream integration

The proxy SHALL also support the encoded output produced by the screen-capture pipeline:

- `Content-Type: video/mp4`;
- a continuous response of unknown length, so no `Content-Length` is sent;
- `Range` is not applicable to the live stream;
- the connection stays open until the stream ends (source switch, stop, or encoder exit).

## 6. HTTP acceptance criteria

- [x] Local HTTP server starts asynchronously bound to `0.0.0.0:8080`.
- [x] The advertised LAN IP is selected from the receiver's subnet or the default route.
- [x] Chromecast can access the advertised local URL.
- [x] Local files are streamed without full-file buffering.
- [x] Single valid `Range` requests return `206 Partial Content` with correct `Content-Range`.
- [x] Invalid or unsatisfiable ranges return `416`.
- [x] Multi-range and missing-range requests return `200` with the full body.
- [x] `HEAD` requests return headers without a body.
- [x] MIME types are detected from the file extension.
- [x] Remote URL GET responses are streamed through the proxy.
- [x] The `Range` header is forwarded to the remote origin.
- [x] Remote failures return `502`.
- [x] Only `/stream` is routed; other paths return `404`.
- [x] Switching source terminates the in-flight stream.
- [x] Screen encoder output is exposed as continuous `video/mp4`.
- [x] Proxy I/O does not block the GUI thread.
