// SPDX-License-Identifier: MIT OR Apache-2.0
//! Screen capture pipeline: monitor capture on a dedicated `std::thread`,
//! BGRA→RGBA conversion, `ffmpeg` subprocess encoding, and channel bridges.
//! Owned by `05-screen-capture.md`.

pub mod bgra_rgba;
pub mod bridge;
pub mod capture;
pub mod ffmpeg;
pub mod ffmpeg_discover;
#[cfg(target_os = "linux")]
pub mod pipewire;
#[cfg(target_os = "linux")]
pub mod portal;
pub mod segments;
