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
        _ => {
            eprintln!(
                "usage: fake-encoder <cat MARKER | restart LOG | exit CODE | sleep SECS | stream>"
            );
            ExitCode::FAILURE
        }
    }
}
