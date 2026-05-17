// crates/kestrel-agent/src/capabilities/screen.rs
use anyhow::Context;
use image::{DynamicImage, ImageFormat};
use kestrel_proto::Rect;
use std::io::Cursor;
use xcap::Monitor;

/// Returns `(monitor_index, width_px, height_px)` for each display.
pub fn list_displays() -> Vec<(usize, u32, u32)> {
    Monitor::all()
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .filter_map(|(i, m)| {
            let w = m.width().ok()?;
            let h = m.height().ok()?;
            Some((i, w, h))
        })
        .collect()
}

/// Capture the full display at `idx` and return PNG bytes.
pub fn capture_display(idx: usize) -> anyhow::Result<Vec<u8>> {
    let monitors = Monitor::all().context("xcap Monitor::all failed")?;
    let monitor = monitors
        .into_iter()
        .nth(idx)
        .ok_or_else(|| anyhow::anyhow!("display index {} out of range", idx))?;
    let img = monitor.capture_image().context("capture_image failed")?;
    encode_png(DynamicImage::ImageRgba8(img))
}

/// Capture a normalized region `rect` of display `idx` and return PNG bytes.
/// `rect` coordinates are 0.0..1.0 relative to the display dimensions.
pub fn capture_region(idx: usize, rect: &Rect) -> anyhow::Result<Vec<u8>> {
    let monitors = Monitor::all().context("xcap Monitor::all failed")?;
    let monitor = monitors
        .into_iter()
        .nth(idx)
        .ok_or_else(|| anyhow::anyhow!("display index {} out of range", idx))?;
    let w = monitor.width().context("width")?;
    let h = monitor.height().context("height")?;
    let rx = (rect.x * w as f64).round() as u32;
    let ry = (rect.y * h as f64).round() as u32;
    let rw = (rect.w * w as f64).round() as u32;
    let rh = (rect.h * h as f64).round() as u32;
    let img = monitor.capture_region(rx, ry, rw, rh).context("capture_region failed")?;
    encode_png(DynamicImage::ImageRgba8(img))
}

fn encode_png(img: DynamicImage) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    img.write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
        .context("PNG encode failed")?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_display_0_returns_valid_png() {
        let png = capture_display(0).expect("capture should succeed on a machine with a display");
        assert!(!png.is_empty(), "PNG bytes must not be empty");
        assert_eq!(&png[..4], &[0x89, 0x50, 0x4E, 0x47], "bytes must start with PNG magic");
    }

    #[test]
    fn list_displays_returns_at_least_one() {
        let displays = list_displays();
        assert!(!displays.is_empty(), "must find at least one display");
        let (_, w, h) = displays[0];
        assert!(w > 0 && h > 0, "primary display must have non-zero dimensions");
    }
}
