// SPDX-License-Identifier: MIT OR Apache-2.0
//! MIME-type detection tests (`04-media-proxy.md` §3.2).
//! Gate: `cargo test --test mime_tests`.

#![forbid(unsafe_code)]

use std::path::Path;

use cast_app::media::mime::{DEFAULT_MIME, MIME_TABLE, mime_for_extension, mime_for_path};

#[test]
fn every_table_entry_is_consistent() {
    for (extension, mime) in MIME_TABLE {
        assert_eq!(mime_for_extension(extension), *mime);
        assert_eq!(
            mime_for_path(Path::new(&format!("/media/clip.{extension}"))),
            *mime
        );
    }
}

#[test]
fn table_covers_all_spec_extensions() {
    let expected = [
        ("mp4", "video/mp4"),
        ("webm", "video/webm"),
        ("mkv", "video/x-matroska"),
        ("mov", "video/quicktime"),
        ("mp3", "audio/mpeg"),
        ("aac", "audio/aac"),
        ("m4a", "audio/mp4"),
        ("flac", "audio/flac"),
        ("wav", "audio/wav"),
    ];
    for (extension, mime) in expected {
        assert_eq!(mime_for_extension(extension), mime);
    }
}

#[test]
fn unknown_extensions_fall_back_to_octet_stream() {
    assert_eq!(mime_for_extension("xyz"), DEFAULT_MIME);
    assert_eq!(mime_for_extension(""), DEFAULT_MIME);
    assert_eq!(
        mime_for_path(Path::new("/media/clip.unknown")),
        DEFAULT_MIME
    );
}

#[test]
fn extension_matching_is_case_insensitive() {
    assert_eq!(mime_for_extension("MP4"), "video/mp4");
    assert_eq!(mime_for_extension("WebM"), "video/webm");
    assert_eq!(mime_for_extension("Mkv"), "video/x-matroska");
    assert_eq!(
        mime_for_path(Path::new("/media/CAPTAIN.FLAC")),
        "audio/flac"
    );
}

#[test]
fn extensionless_paths_fall_back() {
    assert_eq!(mime_for_path(Path::new("/media/video")), DEFAULT_MIME);
    assert_eq!(mime_for_path(Path::new("noextension")), DEFAULT_MIME);
}

#[test]
fn dotted_filenames_use_the_final_extension() {
    assert_eq!(
        mime_for_path(Path::new("dir.with.dots/file.mp3")),
        "audio/mpeg"
    );
    assert_eq!(mime_for_path(Path::new("backup.tar.mp4")), "video/mp4");
}
