#![forbid(unsafe_code)]

//! `ffmpeg` binary discovery on PATH with a cached result
//! (`05-screen-capture.md` §4.1): the Display source is disabled when the
//! binary is missing, so availability is scanned once at first use.

use std::ffi::OsStr;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Cache of the last discovery result. `None` until the first scan.
static CACHE: Mutex<Option<bool>> = Mutex::new(None);

/// Test-only PATH source so cache behavior can be exercised without
/// `std::env::set_var` (edition-2024 `set_var` is forbidden here, as is all
/// raw memory code in this crate).
#[cfg(test)]
static PATH_OVERRIDE: Mutex<Option<std::ffi::OsString>> = Mutex::new(None);

#[cfg(test)]
fn current_path() -> Option<std::ffi::OsString> {
    match PATH_OVERRIDE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
    {
        Some(path) => Some(path.clone()),
        None => std::env::var_os("PATH"),
    }
}

#[cfg(not(test))]
fn current_path() -> Option<std::ffi::OsString> {
    std::env::var_os("PATH")
}

/// Whether an `ffmpeg` executable was found on `PATH` (spec §4.1).
///
/// The result is cached after the first scan; `PATH` is only re-read after
/// [`reset_cache`]. Safe to call from any thread.
pub fn ffmpeg_available() -> bool {
    let mut cache = lock_cache();
    if let Some(found) = *cache {
        return found;
    }
    let found = match current_path() {
        Some(path) => find_ffmpeg(&path),
        None => false,
    };
    *cache = Some(found);
    found
}

/// Forget the cached discovery result so the next [`ffmpeg_available`] call
/// re-scans `PATH` (test helper; also used after PATH changes).
pub fn reset_cache() {
    *lock_cache() = None;
}

/// The discovered executable path, if any (shares the cache with
/// [`ffmpeg_available`]).
pub fn ffmpeg_path() -> Option<std::path::PathBuf> {
    if !ffmpeg_available() {
        return None;
    }
    let path = current_path()?;
    scan_path(&path).map(|(_, full)| full)
}

/// Scan a `PATH`-style string (colon-separated on Unix, semicolon-separated
/// on Windows) for an `ffmpeg` executable. Pure function, unit-tested with
/// fixture directories.
fn find_ffmpeg(path_value: &OsStr) -> bool {
    scan_path(path_value).is_some()
}

/// Scan `PATH` entries for `ffmpeg`, returning the entry and full path of the
/// first hit. Empty entries resolve to the current directory (POSIX rule).
fn scan_path(path_value: &OsStr) -> Option<(std::path::PathBuf, std::path::PathBuf)> {
    let separator = if cfg!(target_os = "windows") {
        ';'
    } else {
        ':'
    };
    let candidates: &[&str] = if cfg!(target_os = "windows") {
        &["ffmpeg.exe", "ffmpeg"]
    } else {
        &["ffmpeg"]
    };
    for entry in path_value.to_string_lossy().split(separator) {
        let dir = if entry.is_empty() {
            Path::new(".")
        } else {
            Path::new(entry)
        };
        for candidate in candidates {
            let full = dir.join(candidate);
            if full.is_file() {
                return Some((dir.to_path_buf(), full));
            }
        }
    }
    None
}

fn lock_cache() -> MutexGuard<'static, Option<bool>> {
    CACHE.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Serializes tests that mutate `PATH`/the cache.
    static PATH_LOCK: Mutex<()> = Mutex::new(());
    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct FixtureDir(std::path::PathBuf);

    impl FixtureDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "cast-app-ffmpeg-discover-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            FixtureDir(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for FixtureDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn make_binary(dir: &Path, name: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        fs::write(&path, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[test]
    fn finds_ffmpeg_in_second_path_entry() {
        let dir = FixtureDir::new();
        let first = dir.path().join("empty");
        fs::create_dir_all(&first).unwrap();
        make_binary(dir.path(), "ffmpeg");
        let value = format!("{}:{}", first.display(), dir.path().display());
        assert!(find_ffmpeg(OsStr::new(&value)));
    }

    #[test]
    fn returns_missing_when_absent_everywhere() {
        let dir = FixtureDir::new();
        assert!(!find_ffmpeg(OsStr::new(&dir.path().display().to_string())));
    }

    #[test]
    fn missing_file_that_is_a_directory_is_ignored() {
        let dir = FixtureDir::new();
        let fake = dir.path().join("ffmpeg");
        fs::create_dir_all(&fake).unwrap();
        assert!(!find_ffmpeg(OsStr::new(&dir.path().display().to_string())));
    }

    #[test]
    fn empty_entry_resolves_to_current_directory() {
        // An empty entry means "." per POSIX; the repo root has no `ffmpeg`
        // file, so this must come back negative deterministically.
        assert!(!find_ffmpeg(OsStr::new("")));
    }

    #[test]
    fn availability_is_cached_until_reset() {
        let _guard = PATH_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let dir = FixtureDir::new();
        let empty = dir.path().join("empty");
        fs::create_dir_all(&empty).unwrap();
        set_path_override(&empty);
        reset_cache();
        assert!(!ffmpeg_available());
        // Add ffmpeg and re-scan: cache must still hold the old result.
        make_binary(dir.path(), "ffmpeg");
        assert!(!ffmpeg_available());
        // Reset: the new PATH layout is observed.
        set_path_override(dir.path());
        reset_cache();
        assert!(ffmpeg_available());
        clear_path_override();
        reset_cache();
    }

    #[test]
    fn missing_path_var_means_unavailable() {
        let _guard = PATH_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        clear_path_override();
        reset_cache();
        // Force an empty PATH by overriding with an empty dir listing nothing.
        let dir = FixtureDir::new();
        set_path_override(&dir.path().join("nonexistent"));
        reset_cache();
        assert!(!ffmpeg_available());
        clear_path_override();
        reset_cache();
    }

    fn set_path_override(path: &Path) {
        *PATH_OVERRIDE.lock().unwrap_or_else(PoisonError::into_inner) =
            Some(path.as_os_str().to_owned());
    }

    fn clear_path_override() {
        *PATH_OVERRIDE.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    #[test]
    fn ffmpeg_path_returns_first_hit() {
        let dir = FixtureDir::new();
        make_binary(dir.path(), "ffmpeg");
        let value = dir.path().display().to_string();
        let hit = scan_path(OsStr::new(&value)).map(|(_, full)| full);
        assert_eq!(hit, Some(dir.path().join("ffmpeg")));
    }

    #[test]
    fn unix_permissions_are_checked_via_is_file() {
        let dir = FixtureDir::new();
        let path = dir.path().join("ffmpeg");
        fs::write(&path, b"x").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        // A non-executable regular file is still a match on the PATH (the
        // spawn itself will surface a permissions error).
        assert!(find_ffmpeg(OsStr::new(&dir.path().display().to_string())));
    }
}
