// SPDX-License-Identifier: MIT OR Apache-2.0
//! Shared utilities: shutdown token, exponential-backoff retry policy, and
//! bounded drop-oldest backpressure channels. Owned by `06-concurrency.md`.

pub mod backpressure;
pub mod retry;
pub mod shutdown;
