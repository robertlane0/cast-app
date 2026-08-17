// SPDX-License-Identifier: MIT OR Apache-2.0
#![forbid(unsafe_code)]

//! Cross-platform fake encoder used by `tests/screen_pipeline_tests.rs` to
//! stand in for `ffmpeg` (reads stdin, writes stdout, honors EOF). Because it
//! is a compiled binary, the screen-bridge tests no longer depend on `/bin/sh`,
//! POSIX file descriptors, or `kill` signals and now run on Windows too
//! (ISS-012). Tests locate the executable via
//! `env!("CARGO_BIN_EXE_fake_encoder")`.
//!
//! Modes:
//! - `cat MARKER` — consume stdin until EOF, then create `MARKER`, exit 0
//! - `restart LOG` — append a "started" line to `LOG`, consume stdin, exit 0
//! - `exit CODE` — exit immediately with `CODE`
//! - `sleep SECS` — sleep `SECS` without reading stdin, then exit 0
//! - `stream` — emit 64 KiB chunks on stdout forever while draining stdin;
//!   exiting on stdin EOF kills the emitter
//! - `fmp4 LOG` — append `started <pid>` to `LOG`, then emit a structured
//!   fMP4 stream (init: `ftyp`+`moov` whose payload carries the pid; then
//!   `moof`+`mdat` fragments whose mdat payload repeats the pid bytes) on
//!   stdout forever while draining stdin; exiting on stdin EOF kills the
//!   emitter. The pid markers let tests distinguish encoder generations and
//!   detect stale bytes after a restart.

use std::env;
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::process::ExitCode;
use std::time::Duration;

fn drain_stdin() -> io::Result<()> {
    let mut stdin = io::stdin().lock();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        match stdin.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}

fn emit_forever() {
    let mut stdout = io::stdout().lock();
    let chunk = vec![0u8; 64 * 1024];
    loop {
        if stdout.write_all(&chunk).is_err() {
            std::process::exit(0);
        }
    }
}

/// Size of one fake fragment's `mdat` payload.
const FRAGMENT_MDAT: usize = 256 * 1024;

/// Write one ISO-BMFF box (4-byte BE size + 4-byte type + payload).
fn write_box(writer: &mut impl Write, kind: &[u8; 4], payload: &[u8]) -> io::Result<()> {
    let size = 8u32 + payload.len() as u32;
    writer.write_all(&size.to_be_bytes())?;
    writer.write_all(kind)?;
    writer.write_all(payload)
}

/// Emit the structured fMP4 stream; the init and every fragment carry the
/// 4-byte `pattern` so tests can recognize the stream's generation.
fn emit_fmp4(pattern: [u8; 4]) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    write_box(&mut stdout, b"ftyp", b"isom\x00\x00\x00\x00isom")?;
    let mut moov = vec![0u8; 4096];
    moov[..4].copy_from_slice(&pattern);
    write_box(&mut stdout, b"moov", &moov)?;
    let mut mdat = vec![0u8; FRAGMENT_MDAT];
    for slot in mdat.chunks_exact_mut(4) {
        slot.copy_from_slice(&pattern);
    }
    let moof = vec![0u8; 64];
    loop {
        write_box(&mut stdout, b"moof", &moof)?;
        write_box(&mut stdout, b"mdat", &mdat)?;
    }
}

fn append_started(log: &str) {
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(log) {
        let _ = writeln!(file, "started {}", std::process::id());
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str).unwrap_or("") {
        "cat" => {
            if drain_stdin().is_err() {
                return ExitCode::FAILURE;
            }
            let _ = std::fs::write(&args[1], b"eof");
            ExitCode::SUCCESS
        }
        "restart" => {
            let mut log = OpenOptions::new().create(true).append(true).open(&args[1]);
            if let Ok(file) = log.as_mut() {
                let _ = writeln!(file, "started");
            }
            if drain_stdin().is_err() {
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        "exit" => {
            let code = args[1].parse::<u8>().unwrap_or(1);
            ExitCode::from(code)
        }
        "sleep" => {
            let secs = args[1].parse::<u64>().unwrap_or(1);
            std::thread::sleep(Duration::from_secs(secs));
            ExitCode::SUCCESS
        }
        "stream" => {
            let emitter = std::thread::spawn(emit_forever);
            let _ = drain_stdin();
            let _ = emitter;
            std::process::exit(0);
        }
        "fmp4" => {
            append_started(&args[1]);
            let pid = std::process::id();
            let emitter = std::thread::spawn(move || {
                let _ = emit_fmp4(pid.to_le_bytes());
            });
            let _ = drain_stdin();
            let _ = emitter;
            std::process::exit(0);
        }
        _ => {
            eprintln!(
                "usage: fake-encoder <cat MARKER | restart LOG | exit CODE | sleep SECS | stream | fmp4 LOG>"
            );
            ExitCode::FAILURE
        }
    }
}
