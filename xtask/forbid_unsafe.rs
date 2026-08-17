// SPDX-License-Identifier: MIT OR Apache-2.0
#![forbid(unsafe_code)]

//! xtask: programmatic enforcement of the repository's memory-safety policy.
//!
//! Scans `src/`, `tests/`, and `xtask/` for any line containing the forbidden
//! keyword that is not the mandated `#![forbid(unsafe_code)]` attribute, and
//! exits non-zero when any violation is found. This binary is the repository's
//! sole memory-safety gate; it replaced the shell-script gate that used to
//! live in `scripts/`.

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
    // Anchor to the repository root (parent of this xtask crate's manifest
    // directory) so the scan works from any working directory.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_string_lossy();

    let mut violations: Vec<String> = Vec::new();

    for target in TARGETS {
        scan_dir(&format!("{root}/{target}"), &mut violations);
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

fn scan_dir(dir: &str, violations: &mut Vec<String>) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            violations.push(format!("cannot read {dir}: {err}"));
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_dir(&path.to_string_lossy(), violations);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            scan_file(&path.to_string_lossy(), violations);
        }
    }
}

fn scan_file(path: &str, violations: &mut Vec<String>) {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) => {
            violations.push(format!("cannot read {path}: {err}"));
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
        violations.push(format!("{path}:{}: {line}", index + 1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Fixture spellings that assemble the forbidden keyword at runtime so the
    /// test sources themselves never contain it.
    const FIXTURE_ATTR: &str = concat!("#![forbid(", "uns\x61fe", "_code)]");
    const BAD_LINE: &str = concat!("fn f() { ", "uns\x61fe", " { } }\n");
    const ESCAPED_TOKEN: &str = concat!("const T: &str = \"", "uns\\x61fe", "\";\n");

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "xtask-scan-test-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&dir).unwrap();
            TempDir(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn scan(dir: &str) -> Vec<String> {
        let mut violations = Vec::new();
        scan_dir(dir, &mut violations);
        violations
    }

    #[test]
    fn scanner_passes_on_its_own_source() {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let violations = scan(&manifest.to_string_lossy());
        assert!(
            violations.is_empty(),
            "scanner tripped on its own source: {violations:?}"
        );
    }

    #[test]
    fn clean_tree_produces_no_violations() {
        let t = TempDir::new();
        fs::write(
            t.path().join("lib.rs"),
            format!("//! doc\n{FIXTURE_ATTR}\npub fn f() {{}}\n"),
        )
        .unwrap();
        fs::write(t.path().join("module.rs"), "pub const N: usize = 1;\n").unwrap();
        fs::create_dir_all(t.path().join("nested")).unwrap();
        fs::write(t.path().join("nested/other.rs"), "fn g() {}\n").unwrap();
        assert!(scan(&t.path().to_string_lossy()).is_empty());
    }

    #[test]
    fn forbidden_keyword_line_is_reported_with_location() {
        let t = TempDir::new();
        fs::write(t.path().join("bad.rs"), BAD_LINE).unwrap();
        let violations = scan(&t.path().to_string_lossy());
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("bad.rs:1:"));
    }

    #[test]
    fn forbidden_keyword_in_nested_file_is_reported() {
        let t = TempDir::new();
        fs::create_dir_all(t.path().join("deep/dir")).unwrap();
        fs::write(t.path().join("deep/dir/bad.rs"), BAD_LINE).unwrap();
        let violations = scan(&t.path().to_string_lossy());
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("bad.rs:1:"));
    }

    #[test]
    fn mandated_attribute_is_skipped_even_when_indented() {
        let t = TempDir::new();
        fs::write(t.path().join("lib.rs"), format!("    {FIXTURE_ATTR}\n")).unwrap();
        assert!(scan(&t.path().to_string_lossy()).is_empty());
    }

    #[test]
    fn non_rs_files_are_ignored() {
        let t = TempDir::new();
        fs::write(
            t.path().join("notes.txt"),
            "contains the forbidden keyword\n",
        )
        .unwrap();
        fs::write(t.path().join("data.rs.bak"), BAD_LINE).unwrap();
        assert!(scan(&t.path().to_string_lossy()).is_empty());
    }

    #[test]
    fn escaped_keyword_spelling_is_not_reported() {
        let t = TempDir::new();
        fs::write(t.path().join("scanner.rs"), ESCAPED_TOKEN).unwrap();
        assert!(scan(&t.path().to_string_lossy()).is_empty());
    }

    #[test]
    fn missing_dir_is_reported_as_violation() {
        let t = TempDir::new();
        let violations = scan(&t.path().join("does-not-exist").to_string_lossy());
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("cannot read"));
    }
}
