//! File-extension to MIME-type mapping for media serving
//! (`04-media-proxy.md` §3.2).

use std::path::Path;

/// Extension -> MIME map (`04-media-proxy.md` §3.2). Unknown extensions map
/// to `application/octet-stream`.
pub const MIME_TABLE: &[(&str, &str)] = &[
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

/// The MIME type for any unrecognized extension.
pub const DEFAULT_MIME: &str = "application/octet-stream";

/// MIME type for a file path, from its lowercase extension
/// (`04-media-proxy.md` §3.2, FR-014).
pub fn mime_for_path(path: &Path) -> &'static str {
    let Some(extension) = path.extension().and_then(|ext| ext.to_str()) else {
        return DEFAULT_MIME;
    };
    mime_for_extension(extension)
}

/// MIME type for an extension string, case-insensitive
/// (`04-media-proxy.md` §3.2).
pub fn mime_for_extension(extension: &str) -> &'static str {
    MIME_TABLE
        .iter()
        .find(|(ext, _)| ext.eq_ignore_ascii_case(extension))
        .map(|(_, mime)| *mime)
        .unwrap_or(DEFAULT_MIME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_extensions_map_to_expected_mimes() {
        for (ext, mime) in MIME_TABLE {
            assert_eq!(mime_for_extension(ext), *mime, "extension {ext}");
        }
    }

    #[test]
    fn unknown_extension_maps_to_octet_stream() {
        assert_eq!(mime_for_extension("xyz"), DEFAULT_MIME);
        assert_eq!(mime_for_extension(""), DEFAULT_MIME);
    }

    #[test]
    fn extension_lookup_is_case_insensitive() {
        assert_eq!(mime_for_extension("MP4"), "video/mp4");
        assert_eq!(mime_for_extension("WebM"), "video/webm");
        assert_eq!(mime_for_extension("MKV"), "video/x-matroska");
        assert_eq!(mime_for_extension("FLAC"), "audio/flac");
    }

    #[test]
    fn path_without_extension_maps_to_octet_stream() {
        assert_eq!(mime_for_path(Path::new("/media/video")), DEFAULT_MIME);
        assert_eq!(mime_for_path(Path::new("file")), DEFAULT_MIME);
    }

    #[test]
    fn path_uses_final_extension() {
        assert_eq!(mime_for_path(Path::new("/media/clip.mov.mp4")), "video/mp4");
        assert_eq!(
            mime_for_path(Path::new("dir.with.dots/file.mp3")),
            "audio/mpeg"
        );
    }
}
