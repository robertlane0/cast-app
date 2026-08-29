#![no_main]
use libfuzzer_sys::fuzz_target;

use cast_app::media::smb_source::{SmbUrl, is_smb_url};

fuzz_target!(|data: &[u8]| {
    let raw = String::from_utf8_lossy(data);

    // is_smb_url is a pure prefix check and must not panic.
    let is_smb = is_smb_url(&raw);

    // SmbUrl::parse must not panic on any string; it validates scheme,
    // userinfo, percent-encoding, and path segments.
    let parsed = SmbUrl::parse(&raw);
    match parsed {
        Ok(url) => {
            // Successful parses must have non-empty components.
            assert!(!url.host.is_empty());
            assert!(!url.share.is_empty());
            assert!(!url.file_path.is_empty());
            // Host, share, file_path must not contain empty segments.
            assert!(!url.file_path.contains("//"));
            assert!(!url.file_path.starts_with('/'));
            // is_smb_url should be true for any valid smb:// URL.
            assert!(is_smb, "valid smb URL must be recognized by is_smb_url");
        }
        Err(_) => {
            // Invalid URLs are expected for arbitrary input. No further check.
        }
    }

    // Also exercise mime mapping on any file path that might have been parsed,
    // and on raw input as an extension.
    let _ = cast_app::media::mime::mime_for_extension(&raw);
});
