// SPDX-License-Identifier: MIT OR Apache-2.0
#![forbid(unsafe_code)]

//! cast-app: a desktop app that discovers Chromecast receivers, streams local
//! files / remote URLs / captured displays to them, and drives a hand-rolled
//! Cast V2 stack plus an external `ffmpeg` subprocess for encoding.

pub mod app;
pub mod cast;
pub mod media;
pub mod runtime;
pub mod screen;
pub mod state;
pub mod util;
