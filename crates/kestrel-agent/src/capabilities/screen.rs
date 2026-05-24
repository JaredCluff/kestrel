// crates/kestrel-agent/src/capabilities/screen.rs
use anyhow::Context;
use image::{DynamicImage, ImageFormat};
use kestrel_proto::Rect;
use std::io::Cursor;
use xcap::Monitor;

/// Returns `(monitor_index, width_px, height_px)` for each display whose
/// dimensions can be queried. **The `monitor_index` is the position in
/// `xcap::Monitor::all()` (i.e. what `capture_display(idx)` will pass to
/// `.nth(idx)`), NOT the filtered position.** Without this, a monitor whose
/// `width()`/`height()` happened to fail would shift every subsequent
/// monitor's reported id by one — and a `capture_display(2)` call would land
/// on a different physical display than the one the hub validated against.
pub fn list_displays() -> Vec<(usize, u32, u32)> {
    let monitors = match Monitor::all() {
        Ok(m) => m,
        Err(e) => {
            // Surface this — operators staring at `displays: []` in the
            // dashboard should be able to tell the difference between
            // "really no monitors" and "xcap couldn't enumerate them".
            tracing::warn!("xcap Monitor::all failed: {}", e);
            return Vec::new();
        }
    };
    monitors
        .into_iter()
        .enumerate()
        .filter_map(|(idx, m)| {
            let w = m.width().ok()?;
            let h = m.height().ok()?;
            Some((idx, w, h))
        })
        .collect()
}

/// Returns `(width, height)` for the primary display. Falls back to
/// 1920x1080 when no display is detected (headless CI / sandboxed
/// agents). Shared by the input-injection path (which needs to
/// denormalize 0.0..1.0 mouse coords) and the WebRTC capture path.
pub fn primary_display_dims() -> (u32, u32) {
    list_displays()
        .into_iter()
        .next()
        .map(|(_, w, h)| (w, h))
        .unwrap_or((1920, 1080))
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
    anyhow::ensure!(
        rect.x >= 0.0 && rect.y >= 0.0 && rect.w > 0.0 && rect.h > 0.0
            && rect.x + rect.w <= 1.0 && rect.y + rect.h <= 1.0,
        "invalid rect: x={} y={} w={} h={} (all values must be in [0,1] with x+w≤1, y+h≤1)",
        rect.x, rect.y, rect.w, rect.h
    );
    let monitors = Monitor::all().context("xcap Monitor::all failed")?;
    let monitor = monitors
        .into_iter()
        .nth(idx)
        .ok_or_else(|| anyhow::anyhow!("display index {} out of range", idx))?;
    let w = monitor.width().context("width")?;
    let h = monitor.height().context("height")?;
    let rx = (rect.x * w as f64).round() as u32;
    let ry = (rect.y * h as f64).round() as u32;
    let rw = ((rect.w * w as f64).round() as u32).min(w.saturating_sub(rx));
    let rh = ((rect.h * h as f64).round() as u32).min(h.saturating_sub(ry));
    anyhow::ensure!(rw > 0 && rh > 0, "computed region is zero-sized (rw={}, rh={})", rw, rh);
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
    #[ignore = "requires Screen Recording permission (macOS TCC); run manually"]
    fn capture_display_0_returns_valid_png() {
        let png = capture_display(0).expect("capture should succeed on a machine with a display");
        assert!(!png.is_empty(), "PNG bytes must not be empty");
        assert_eq!(&png[..4], &[0x89, 0x50, 0x4E, 0x47], "bytes must start with PNG magic");
    }

    #[test]
    #[ignore = "requires Screen Recording permission (macOS TCC); run manually"]
    fn list_displays_returns_at_least_one() {
        let displays = list_displays();
        assert!(!displays.is_empty(), "must find at least one display");
        let (_, w, h) = displays[0];
        assert!(w > 0 && h > 0, "primary display must have non-zero dimensions");
    }
}
