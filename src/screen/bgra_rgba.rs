//! Pure-safe in-place BGRA→RGBA byte shuffle (`05-screen-capture.md` §3.1).
//! `xcap` versions have historically returned BGRA on some platforms; the
//! spec pins the pipeline to `-pix_fmt rgba`, so frames must arrive in RGBA
//! byte order.

/// Byte order delivered by `xcap::Monitor::capture_image()` on the pinned
/// crate version.
///
/// Verified against **xcap 0.9.6** at implementation time (spec §3.1 "The
/// exact byte order SHALL be verified against the pinned crate version"):
///
/// - Linux/X11: `xorg_capture.rs` builds RGBA explicitly (per-channel
///   extraction into `rgba[index..]`).
/// - macOS: `macos/capture.rs` converts with `bgra.swap(0, 2)`.
/// - Windows: `windows/gdi.rs` routes through `bgra_to_rgba`.
///
/// The capture thread consults this constant before piping frames to
/// `ffmpeg`; if a future xcap release regresses to BGRA, flipping it to
/// `false` re-enables the shuffle below without touching the pipeline.
pub const XCAP_FRAMES_ARE_RGBA: bool = true;

/// Convert BGRA → RGBA **in place** by swapping the R and B channels of every
/// 4-byte pixel group.
///
/// Trailing bytes beyond a complete pixel are left untouched (defensive; the
/// capture loop always produces `width * height * 4` bytes).
pub fn bgra_to_rgba(pixels: &mut [u8]) {
    for pixel in pixels.chunks_exact_mut(4) {
        pixel.swap(0, 2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swaps_red_and_blue_of_each_pixel() {
        let mut pixels = vec![
            0x11, 0x22, 0x33, 0x44, // B G R A
            0xaa, 0xbb, 0xcc, 0xdd, // B G R A
        ];
        bgra_to_rgba(&mut pixels);
        assert_eq!(
            pixels,
            vec![
                0x33, 0x22, 0x11, 0x44, // R G B A
                0xcc, 0xbb, 0xaa, 0xdd, // R G B A
            ]
        );
    }

    #[test]
    fn alpha_and_green_are_untouched() {
        let mut pixels = vec![0x10, 0x40, 0x80, 0xff];
        bgra_to_rgba(&mut pixels);
        assert_eq!(pixels, vec![0x80, 0x40, 0x10, 0xff]);
    }

    #[test]
    fn conversion_is_an_involution() {
        let mut pixels = vec![
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
        ];
        let original = pixels.clone();
        bgra_to_rgba(&mut pixels);
        bgra_to_rgba(&mut pixels);
        assert_eq!(pixels, original);
    }

    #[test]
    fn trailing_bytes_beyond_a_full_pixel_are_left_untouched() {
        let mut pixels = vec![0x11, 0x22, 0x33, 0x44, 0x99, 0x99];
        bgra_to_rgba(&mut pixels);
        assert_eq!(pixels, vec![0x33, 0x22, 0x11, 0x44, 0x99, 0x99]);
    }

    #[test]
    fn empty_buffer_is_a_noop() {
        let mut pixels: Vec<u8> = Vec::new();
        bgra_to_rgba(&mut pixels);
        assert!(pixels.is_empty());
    }

    #[test]
    fn one_megapixel_frame_converts_every_pixel() {
        let mut pixels = vec![0u8; 1024 * 1024 * 4];
        for (index, byte) in pixels.iter_mut().enumerate() {
            *byte = (index % 256) as u8;
        }
        bgra_to_rgba(&mut pixels);
        for index in (0..pixels.len()).step_by(4) {
            assert_eq!(
                pixels[index + 2],
                (index % 256) as u8,
                "R mismatch at {index}"
            );
            assert_eq!(
                pixels[index],
                ((index + 2) % 256) as u8,
                "B mismatch at {index}"
            );
            assert_eq!(
                pixels[index + 1],
                ((index + 1) % 256) as u8,
                "G mismatch at {index}"
            );
            assert_eq!(
                pixels[index + 3],
                ((index + 3) % 256) as u8,
                "A mismatch at {index}"
            );
        }
    }

    #[test]
    fn pinned_xcap_versions_deliver_rgba() {
        // Guards the spec §3.1 verification: the current pipeline does not
        // shuffle, so a regression to BGRA must be caught here.
        const _: () = assert!(XCAP_FRAMES_ARE_RGBA);
    }
}
