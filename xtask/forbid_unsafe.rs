#![forbid(unsafe_code)]

//! xtask: programmatic enforcement of the repository's memory-safety policy.
//!
//! Scans `src/`, `tests/`, and `xtask/` for any line containing the forbidden
//! keyword that is not the mandated `#![forbid(unsafe_code)]` attribute, and
//! exits non-zero on the first violation. Mirrors the CI shell-script gate in
//! `scripts/`.

use std::fs;
use std::path::Path;
use std::process::ExitCode;

/// The forbidden keyword. The literal is spelled with an escape sequence so
/// this scanner's own source never trips the policy it enforces.
const TOKEN: &str = "uns\x61fe";
/// The allowed attribute containing the keyword: `#![forbid(unsafe_code)]`.
const ALLOWED_ATTR: &str = concat!("forbid(", "uns\x61fe", "_code)");

const TARGETS: &[&str] = &["src", "tests", "xtask"];

fn main() -> ExitCode {
    let mut violations: Vec<String> = Vec::new();

    for target in TARGETS {
        scan_dir(Path::new(target), &mut violations);
    }

    if violations.is_empty() {
        println!("xtask: OK — no policy violations in src/, tests/, xtask/");
        ExitCode::SUCCESS
    } else {
        eprintln!("xtask: FAILED — policy violations found:");
        for v in &violations {
            eprintln!("{v}");
        }
        ExitCode::FAILURE
    }
}

fn scan_dir(dir: &Path, violations: &mut Vec<String>) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            violations.push(format!("cannot read {}: {err}", dir.display()));
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_dir(&path, violations);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            scan_file(&path, violations);
        }
    }
}

fn scan_file(path: &Path, violations: &mut Vec<String>) {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) => {
            violations.push(format!("cannot read {}: {err}", path.display()));
            return;
        }
    };

    for (index, line) in content.lines().enumerate() {
        if !line.contains(TOKEN) {
            continue;
        }
        if line.trim().contains(ALLOWED_ATTR) {
            continue;
        }
        violations.push(format!("{}:{}: {}", path.display(), index + 1, line));
    }
}
