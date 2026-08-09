# 04 — Media Proxy Specification

## 1. Purpose

Provide a local HTTP endpoint that a Chromecast can access for media originating from:

1. local files;
2. remote URLs;
3. screen-capture encoding.

Example endpoint:

```text
http://<PC-IP>:8080/stream
```

The exact bind address, port configuration and route structure are **TBD**; `8080` and `/stream` are the overview's example.

## 2. Server implementation

Use a lightweight asynchronous server based on:

```rust
tokio::net::TcpListener
```

The server SHALL stream media bytes without requiring the entire media object to be loaded into memory.

## 3. Local-file serving

When a local file is selected:

1. The HTTP proxy receives the Chromecast request.
2. The server opens the media with `tokio::fs::File`.
3. The server parses the HTTP `Range: bytes=...` request header.
4. It returns `206 Partial Content` for a valid range.
5. It streams the requested byte range.

Range support is required because the Chromecast needs seeking and buffering behavior.

The overview does not specify:

- response header set;
- MIME type detection;
- single-range vs multi-range support;
- invalid-range response;
- HEAD requests;
- caching headers.

These are **TBD**.

## 4. Remote URL proxying

For a Web URL source:

1. The Rust application accepts the remote URL.
2. The proxy performs a GET using `reqwest`.
3. Remote response bytes are streamed into the local HTTP connection.
4. The Chromecast receives the local proxy URL instead of the original remote origin.

The proxy may handle authentication or request headers on behalf of the PC, as stated by the overview.

The exact forwarded headers, redirect policy, authentication mechanism, timeout policy, response-status handling and Range forwarding are **TBD**.

## 5. Screen stream integration

The proxy SHALL also support the encoded output produced by the screen-capture pipeline.

The overview specifies a continuous `video/mp4` HTTP response for the fMP4 stream.

## 6. HTTP acceptance criteria

- [ ] Local HTTP server starts asynchronously.
- [ ] Chromecast can access the advertised local URL.
- [ ] Local files are streamed without full-file buffering.
- [ ] `Range` requests are parsed.
- [ ] Valid file ranges return `206 Partial Content`.
- [ ] Remote URL GET responses can be streamed through the proxy.
- [ ] Screen encoder output can be exposed as `video/mp4`.
- [ ] Proxy I/O does not block the GUI thread.
