# Google Cast Desktop Application — Specification Set

This directory decomposes `OVERVIEW.md` into implementation-oriented Markdown specifications.

## Specification index

- `01-architecture.md` — architectural boundaries, constraints, safety and dependency policy
- `02-gui.md` — `egui`/`eframe` UI, state model and command flow
- `03-cast-engine.md` — mDNS, TLS, CastV2 framing, Protobuf and namespaces
- `04-media-proxy.md` — local-file serving, URL proxying and HTTP behavior
- `05-screen-capture.md` — safe capture, `ffmpeg` subprocess pipeline and fMP4 streaming
- `06-concurrency.md` — threads, Tokio tasks and channel boundaries
- `07-requirements-and-tests.md` — consolidated requirements, acceptance criteria and test matrix

## Source boundary

These specifications are derived from the supplied project overview. They preserve its architecture, terminology and stated constraints. Items not established by the overview are specified as decisions in the documents above; no open-question register remains.
