// SPDX-License-Identifier: MIT OR Apache-2.0
//! Trust-on-first-use (TOFU) certificate pinning for Cast receivers
//! (`03-cast-engine.md` §3.1). The SHA-256 of the first-seen receiver
//! certificate is stored per receiver key (the mDNS TXT `id=` when
//! advertised, else `friendlyName+IP`) and compared on subsequent
//! connections: a mismatch is surfaced as a warning, never a block — the
//! SSH host-key model, degrading gracefully instead of refusing to connect.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// SHA-256 digest of a certificate's DER encoding.
pub type Fingerprint = [u8; 32];

/// File name of the persisted pin store inside the platform state directory.
pub const PIN_FILE_NAME: &str = "known_hosts.json";

/// Outcome of comparing a freshly presented certificate against the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinCheck {
    /// No certificate pinning was performed (mock transports in tests).
    Disabled,
    /// First time this receiver key was seen: the certificate is now pinned.
    Pinned,
    /// The certificate matches the stored pin.
    Matched,
    /// The certificate differs from the stored pin. The old pin is kept;
    /// the caller warns and proceeds (SSH host-key semantics).
    Mismatch {
        previous: Fingerprint,
        current: Fingerprint,
    },
}

/// JSON persistence wrapper: `{ "<receiver key>": "<64-char hex SHA-256>" }`.
#[derive(Debug, Default, Serialize, Deserialize)]
struct PinsFile {
    pins: HashMap<String, String>,
}

/// The TOFU pin store: an in-memory `key -> fingerprint` map, optionally
/// persisted to a JSON file on first-seen. All operations are best-effort:
/// load failures degrade to an empty store and save failures are logged —
/// pinning must never break discovery or connection establishment.
#[derive(Debug)]
pub struct TofuStore {
    pins: Mutex<HashMap<String, Fingerprint>>,
    path: Option<PathBuf>,
}

impl TofuStore {
    /// A store that lives only for the process lifetime (tests and
    /// connectors built without a persistence path).
    pub fn in_memory() -> Self {
        Self {
            pins: Mutex::new(HashMap::new()),
            path: None,
        }
    }

    /// The production store: `known_hosts.json` in the platform state
    /// directory (Windows `%LOCALAPPDATA%`, macOS `~/Library/Application
    /// Support`, Linux `$XDG_STATE_HOME`/`~/.local/state`, temp dir as the
    /// last resort), so pins survive restarts like SSH host keys.
    pub fn load_default() -> Self {
        Self::load(default_store_path())
    }

    /// Load a store from `path`; a missing or unreadable file, or a file
    /// with malformed entries, degrades to an empty store (warn + continue).
    pub fn load(path: PathBuf) -> Self {
        let store = Self {
            pins: Mutex::new(HashMap::new()),
            path: Some(path.clone()),
        };
        match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<PinsFile>(&bytes) {
                Ok(file) => {
                    let mut pins = store.pins.lock().expect("no poisoned pin map");
                    for (key, hex) in file.pins {
                        match hex_to_fingerprint(&hex) {
                            Some(fingerprint) => {
                                pins.insert(key, fingerprint);
                            }
                            None => {
                                tracing::warn!(%key, "skipping malformed pin entry");
                            }
                        }
                    }
                    tracing::info!(
                        path = %path.display(),
                        entries = pins.len(),
                        "loaded TOFU certificate pins"
                    );
                }
                Err(error) => {
                    tracing::warn!(path = %path.display(), %error, "corrupt TOFU pin store; starting empty");
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(path = %path.display(), "no TOFU pin store yet");
            }
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "cannot read TOFU pin store; starting empty");
            }
        }
        store
    }

    /// The number of stored pins (test/observability helper).
    pub fn len(&self) -> usize {
        self.pins.lock().expect("no poisoned pin map").len()
    }

    /// Whether the store holds no pins.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Compare `fingerprint` against the stored pin for `key`, applying
    /// TOFU semantics: first-seen fingerprints are pinned (and persisted),
    /// matches are reported, mismatches report both digests and keep the
    /// original pin (`03-cast-engine.md` §3.1).
    pub fn check(&self, key: &str, fingerprint: Fingerprint) -> PinCheck {
        let mut pins = self.pins.lock().expect("no poisoned pin map");
        match pins.get(key) {
            None => {
                pins.insert(key.to_string(), fingerprint);
                drop(pins);
                self.save();
                PinCheck::Pinned
            }
            Some(&previous) if previous == fingerprint => PinCheck::Matched,
            Some(&previous) => PinCheck::Mismatch {
                previous,
                current: fingerprint,
            },
        }
    }

    /// Best-effort persistence of the current map; failures are logged and
    /// never fatal.
    fn save(&self) {
        let Some(path) = self.path.clone() else {
            return;
        };
        let pins = self.pins.lock().expect("no poisoned pin map");
        let file = PinsFile {
            pins: pins
                .iter()
                .map(|(key, fingerprint)| (key.clone(), fingerprint_to_hex(fingerprint)))
                .collect(),
        };
        drop(pins);
        let bytes = match serde_json::to_vec_pretty(&file) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::error!(%error, "cannot serialize TOFU pin store");
                return;
            }
        };
        let parent = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        if let Err(error) = std::fs::create_dir_all(&parent) {
            tracing::warn!(%error, path = %path.display(), "cannot create TOFU pin store directory");
            return;
        }
        // Write to a sibling temp file and rename so a crash mid-write can
        // never truncate the store.
        let tmp = path.with_extension("json.tmp");
        match std::fs::write(&tmp, &bytes) {
            Ok(()) => match std::fs::rename(&tmp, &path) {
                Ok(()) => tracing::debug!(path = %path.display(), "TOFU pin store saved"),
                Err(error) => {
                    let _ = std::fs::remove_file(&tmp);
                    tracing::warn!(%error, path = %path.display(), "cannot move TOFU pin store into place");
                }
            },
            Err(error) => {
                tracing::warn!(%error, path = %tmp.display(), "cannot write TOFU pin store");
            }
        }
    }
}

/// The TOFU key for a receiver (`03-cast-engine.md` §3.1): the stable mDNS
/// TXT `id=` when advertised (survives DHCP address changes), else the
/// friendly name combined with the IP address.
pub fn receiver_key(device_id: Option<&str>, name: &str, ip: Ipv4Addr) -> String {
    match device_id {
        Some(id) if !id.is_empty() => id.to_string(),
        _ => format!("{name}+{ip}"),
    }
}

/// The platform state directory for the pin store, mirroring the log-dir
/// conventions in `main.rs` (Phase 12): Windows `%LOCALAPPDATA%`, macOS
/// `~/Library/Application Support`, Linux `$XDG_STATE_HOME` or
/// `~/.local/state`, temp dir as the last resort.
fn default_store_path() -> PathBuf {
    if let Some(base) = std::env::var_os("LOCALAPPDATA") {
        return PathBuf::from(base).join("cast-app").join(PIN_FILE_NAME);
    }
    if cfg!(target_os = "macos") {
        if let Some(home) = std::env::var_os("HOME") {
            let home = PathBuf::from(home);
            return home
                .join("Library")
                .join("Application Support")
                .join("cast-app")
                .join(PIN_FILE_NAME);
        }
    }
    if let Some(base) = std::env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(base).join("cast-app").join(PIN_FILE_NAME);
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join(".local")
            .join("state")
            .join("cast-app")
            .join(PIN_FILE_NAME);
    }
    std::env::temp_dir().join("cast-app").join(PIN_FILE_NAME)
}

/// Lowercase hex encoding of a fingerprint.
pub fn fingerprint_to_hex(fingerprint: &Fingerprint) -> String {
    let mut out = String::with_capacity(fingerprint.len() * 2);
    for byte in fingerprint {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Decode a lowercase hex string into a fingerprint; `None` for wrong
/// lengths or non-hex characters.
pub fn hex_to_fingerprint(hex: &str) -> Option<Fingerprint> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        let hi = hex.as_bytes()[i * 2];
        let lo = hex.as_bytes()[i * 2 + 1];
        *byte = (hex_nibble(hi)? << 4) | hex_nibble(lo)?;
    }
    Some(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A unique temp path per test call; the file is removed on drop.
    struct TempFile {
        path: PathBuf,
    }

    impl TempFile {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "cast-app-tofu-test-{}-{}",
                std::process::id(),
                TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(self.path.with_extension("json.tmp"));
        }
    }

    fn fingerprint(seed: u8) -> Fingerprint {
        [seed; 32]
    }

    #[test]
    fn receiver_key_prefers_the_txt_id_over_the_fallback() {
        let key = receiver_key(
            Some("1e3c5f6a-9f0c-4b1e-8a2d-7b9c1d2e3f40"),
            "Living Room",
            Ipv4Addr::new(192, 168, 1, 42),
        );
        assert_eq!(key, "1e3c5f6a-9f0c-4b1e-8a2d-7b9c1d2e3f40");
    }

    #[test]
    fn receiver_key_falls_back_to_name_plus_ip() {
        let key = receiver_key(None, "Kitchen TV", Ipv4Addr::new(10, 0, 0, 5));
        assert_eq!(key, "Kitchen TV+10.0.0.5");
    }

    #[test]
    fn receiver_key_ignores_an_empty_txt_id() {
        let key = receiver_key(Some(""), "Den", Ipv4Addr::new(10, 0, 0, 9));
        assert_eq!(key, "Den+10.0.0.9");
    }

    #[test]
    fn first_seen_fingerprint_is_pinned_then_matches() {
        let store = TofuStore::in_memory();
        assert_eq!(store.len(), 0);
        assert_eq!(store.check("key", fingerprint(0xAA)), PinCheck::Pinned);
        assert_eq!(store.len(), 1);
        assert_eq!(store.check("key", fingerprint(0xAA)), PinCheck::Matched);
        assert_eq!(store.len(), 1, "matches never add entries");
    }

    #[test]
    fn mismatch_reports_both_digests_and_keeps_the_original_pin() {
        let store = TofuStore::in_memory();
        assert_eq!(store.check("key", fingerprint(0x11)), PinCheck::Pinned);
        let result = store.check("key", fingerprint(0x22));
        assert_eq!(
            result,
            PinCheck::Mismatch {
                previous: fingerprint(0x11),
                current: fingerprint(0x22),
            }
        );
        // The stored pin is unchanged: the next identical certificate still
        // matches the *first-seen* pin (SSH semantics).
        assert_eq!(store.check("key", fingerprint(0x11)), PinCheck::Matched);
        assert_eq!(
            store.check("key", fingerprint(0x22)),
            PinCheck::Mismatch {
                previous: fingerprint(0x11),
                current: fingerprint(0x22),
            }
        );
    }

    #[test]
    fn pins_are_keyed_independently() {
        let store = TofuStore::in_memory();
        assert_eq!(store.check("a", fingerprint(1)), PinCheck::Pinned);
        assert_eq!(store.check("b", fingerprint(2)), PinCheck::Pinned);
        assert_eq!(store.check("a", fingerprint(1)), PinCheck::Matched);
        assert_eq!(store.check("b", fingerprint(2)), PinCheck::Matched);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn persisted_pins_survive_a_store_reload() {
        let file = TempFile::new();
        {
            let store = TofuStore::load(file.path().to_path_buf());
            assert_eq!(
                store.check("Living Room+10.0.0.5", fingerprint(0x77)),
                PinCheck::Pinned
            );
        }
        let reloaded = TofuStore::load(file.path().to_path_buf());
        assert_eq!(reloaded.len(), 1);
        assert_eq!(
            reloaded.check("Living Room+10.0.0.5", fingerprint(0x77)),
            PinCheck::Matched,
            "the pin survives the store being dropped and reloaded"
        );
        assert_eq!(
            reloaded.check("Living Room+10.0.0.5", fingerprint(0x88)),
            PinCheck::Mismatch {
                previous: fingerprint(0x77),
                current: fingerprint(0x88),
            }
        );
    }

    #[test]
    fn a_missing_store_file_starts_empty() {
        let file = TempFile::new();
        let store = TofuStore::load(file.path().to_path_buf());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn a_corrupt_store_file_degrades_to_empty() {
        let file = TempFile::new();
        std::fs::write(file.path(), b"not json at all").expect("write corrupt store");
        let store = TofuStore::load(file.path().to_path_buf());
        assert_eq!(store.len(), 0, "corruption must not fail the app");
    }

    #[test]
    fn malformed_pin_entries_are_skipped_on_load() {
        let file = TempFile::new();
        std::fs::write(
            file.path(),
            format!(
                r#"{{"pins": {{"good": "{}", "short": "abcd", "badhex": "{}"}}}}"#,
                fingerprint_to_hex(&fingerprint(0x5A)),
                "g".repeat(64)
            ),
        )
        .expect("write store");
        let store = TofuStore::load(file.path().to_path_buf());
        assert_eq!(store.len(), 1);
        assert_eq!(store.check("good", fingerprint(0x5A)), PinCheck::Matched);
    }

    #[test]
    fn fingerprint_hex_round_trip() {
        let fingerprint = fingerprint(0xAB);
        let hex = fingerprint_to_hex(&fingerprint);
        assert_eq!(hex.len(), 64);
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_eq!(hex_to_fingerprint(&hex), Some(fingerprint));
    }

    #[test]
    fn hex_to_fingerprint_rejects_bad_input() {
        assert_eq!(hex_to_fingerprint(""), None);
        assert_eq!(hex_to_fingerprint(&"ab".repeat(31)), None, "too short");
        assert_eq!(hex_to_fingerprint(&"ab".repeat(33)), None, "too long");
        assert_eq!(
            hex_to_fingerprint(&format!("{}G", "a".repeat(63))),
            None,
            "non-hex char"
        );
    }

    #[test]
    fn sha256_of_known_vector_matches() {
        use sha2::Digest;
        let digest = sha2::Sha256::digest(b"abc");
        let expected =
            hex_to_fingerprint("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
                .expect("known vector");
        assert_eq!(digest.as_slice(), &expected);
    }
}
