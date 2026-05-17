// crates/kestrel-agent/src/capabilities/clipboard.rs
use anyhow::Context;
use arboard::Clipboard;
use image::{DynamicImage, ImageFormat, RgbaImage};
use kestrel_proto::ClipboardContent;
use std::borrow::Cow;
use std::io::Cursor;

pub fn read_clipboard() -> anyhow::Result<ClipboardContent> {
    let mut cb = Clipboard::new().context("arboard init")?;
    match cb.get_text() {
        Ok(text) => return Ok(ClipboardContent::Text(text)),
        Err(_) => {}
    }
    let img_data = cb.get_image().context("clipboard get_image")?;
    let width = img_data.width as u32;
    let height = img_data.height as u32;
    let rgba = RgbaImage::from_raw(width, height, img_data.bytes.into_owned())
        .ok_or_else(|| anyhow::anyhow!("clipboard image data is invalid (wrong buffer size)"))?;
    let mut png_bytes = Vec::new();
    DynamicImage::ImageRgba8(rgba)
        .write_to(&mut Cursor::new(&mut png_bytes), ImageFormat::Png)
        .context("PNG encode for clipboard image")?;
    Ok(ClipboardContent::Image { png_bytes, width, height })
}

pub fn write_clipboard(content: ClipboardContent) -> anyhow::Result<()> {
    let mut cb = Clipboard::new().context("arboard init")?;
    match content {
        ClipboardContent::Text(text) => {
            cb.set_text(text).context("clipboard set_text")
        }
        ClipboardContent::Image { png_bytes, width, height } => {
            let img = image::load_from_memory(&png_bytes).context("PNG decode for clipboard write")?;
            let rgba = img.to_rgba8();
            let data = arboard::ImageData {
                width: width as usize,
                height: height as usize,
                bytes: Cow::Owned(rgba.into_raw()),
            };
            cb.set_image(data).context("clipboard set_image")
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore = "requires display server / clipboard daemon; run manually"]
    fn clipboard_text_roundtrip() {
        use super::*;
        use kestrel_proto::ClipboardContent;
        write_clipboard(ClipboardContent::Text("kestrel-test-xyz".into())).unwrap();
        let got = read_clipboard().unwrap();
        assert_eq!(got, ClipboardContent::Text("kestrel-test-xyz".into()));
    }
}
