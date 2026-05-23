use anyhow::Context;
use enigo::{Axis, Button as EnigoButton, Coordinate, Direction, Enigo, Key, Keyboard, Mouse, Settings};
use kestrel_proto::{Button, KeyCode, Modifiers, PressRelease};

// `parse_key_str` lives in `kestrel_proto::keys` — re-exported via the crate
// root for callers that want a single source of truth for key-name parsing.

pub fn normalize_to_pixels(x: f64, y: f64, width: u32, height: u32) -> (i32, i32) {
    let px = ((x * width as f64).round() as u32).min(width.saturating_sub(1)) as i32;
    let py = ((y * height as f64).round() as u32).min(height.saturating_sub(1)) as i32;
    (px, py)
}

fn to_enigo_key(kc: &KeyCode) -> Key {
    match kc {
        KeyCode::Char(c) => Key::Unicode(*c),
        KeyCode::Return => Key::Return,
        KeyCode::Backspace => Key::Backspace,
        KeyCode::Tab => Key::Tab,
        KeyCode::Escape => Key::Escape,
        KeyCode::Delete => Key::Delete,
        KeyCode::Home => Key::Home,
        KeyCode::End => Key::End,
        KeyCode::PageUp => Key::PageUp,
        KeyCode::PageDown => Key::PageDown,
        KeyCode::Up => Key::UpArrow,
        KeyCode::Down => Key::DownArrow,
        KeyCode::Left => Key::LeftArrow,
        KeyCode::Right => Key::RightArrow,
        KeyCode::Space => Key::Space,
        KeyCode::Control => Key::Control,
        KeyCode::Shift => Key::Shift,
        KeyCode::Alt => Key::Alt,
        KeyCode::Meta => Key::Meta,
        KeyCode::F1 => Key::F1,
        KeyCode::F2 => Key::F2,
        KeyCode::F3 => Key::F3,
        KeyCode::F4 => Key::F4,
        KeyCode::F5 => Key::F5,
        KeyCode::F6 => Key::F6,
        KeyCode::F7 => Key::F7,
        KeyCode::F8 => Key::F8,
        KeyCode::F9 => Key::F9,
        KeyCode::F10 => Key::F10,
        KeyCode::F11 => Key::F11,
        KeyCode::F12 => Key::F12,
        KeyCode::CapsLock => Key::CapsLock,
        // Key::Numlock is not available on macOS; use Other(0) as a no-op fallback
        #[cfg(not(target_os = "macos"))]
        KeyCode::NumLock => Key::Numlock,
        #[cfg(target_os = "macos")]
        KeyCode::NumLock => Key::Other(0),
    }
}

fn to_enigo_button(b: &Button) -> EnigoButton {
    match b {
        Button::Left => EnigoButton::Left,
        Button::Right => EnigoButton::Right,
        Button::Middle => EnigoButton::Middle,
    }
}

fn to_enigo_dir(a: &PressRelease) -> Direction {
    match a {
        PressRelease::Press => Direction::Press,
        PressRelease::Release => Direction::Release,
        PressRelease::Click => Direction::Click,
    }
}

pub async fn inject_key_event(
    key: KeyCode,
    mods: Modifiers,
    action: PressRelease,
) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut enigo = Enigo::new(&Settings::default())
            .map_err(|e| anyhow::anyhow!("enigo init: {e:?}"))?;
        let dir = to_enigo_dir(&action);
        let is_ctrl  = matches!(key, KeyCode::Control);
        let is_shift = matches!(key, KeyCode::Shift);
        let is_alt   = matches!(key, KeyCode::Alt);
        let is_meta  = matches!(key, KeyCode::Meta);
        if matches!(action, PressRelease::Press | PressRelease::Click) {
            if mods.ctrl  && !is_ctrl  { enigo.key(Key::Control, Direction::Press).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.shift && !is_shift { enigo.key(Key::Shift,   Direction::Press).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.alt   && !is_alt   { enigo.key(Key::Alt,     Direction::Press).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.meta  && !is_meta  { enigo.key(Key::Meta,    Direction::Press).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
        }
        enigo.key(to_enigo_key(&key), dir).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        if matches!(action, PressRelease::Release | PressRelease::Click) {
            if mods.meta  && !is_meta  { enigo.key(Key::Meta,    Direction::Release).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.alt   && !is_alt   { enigo.key(Key::Alt,     Direction::Release).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.shift && !is_shift { enigo.key(Key::Shift,   Direction::Release).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.ctrl  && !is_ctrl  { enigo.key(Key::Control, Direction::Release).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
        }
        Ok(())
    }).await.context("spawn_blocking panic")??;
    Ok(())
}

pub async fn inject_text(text: String) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut enigo = Enigo::new(&Settings::default())
            .map_err(|e| anyhow::anyhow!("enigo init: {e:?}"))?;
        enigo.text(&text).map_err(|e| anyhow::anyhow!("{e:?}"))
    }).await.context("spawn_blocking panic")??;
    Ok(())
}

pub async fn inject_mouse_move(x: f64, y: f64, display_w: u32, display_h: u32) -> anyhow::Result<()> {
    let (px, py) = normalize_to_pixels(x, y, display_w, display_h);
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut enigo = Enigo::new(&Settings::default())
            .map_err(|e| anyhow::anyhow!("enigo init: {e:?}"))?;
        enigo.move_mouse(px, py, Coordinate::Abs).map_err(|e| anyhow::anyhow!("{e:?}"))
    }).await.context("spawn_blocking panic")??;
    Ok(())
}

pub async fn inject_mouse_button(
    button: Button,
    action: PressRelease,
    x: f64,
    y: f64,
    display_w: u32,
    display_h: u32,
) -> anyhow::Result<()> {
    let (px, py) = normalize_to_pixels(x, y, display_w, display_h);
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut enigo = Enigo::new(&Settings::default())
            .map_err(|e| anyhow::anyhow!("enigo init: {e:?}"))?;
        enigo.move_mouse(px, py, Coordinate::Abs).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        enigo.button(to_enigo_button(&button), to_enigo_dir(&action))
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }).await.context("spawn_blocking panic")??;
    Ok(())
}

pub async fn inject_scroll(dx: f64, dy: f64) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut enigo = Enigo::new(&Settings::default())
            .map_err(|e| anyhow::anyhow!("enigo init: {e:?}"))?;
        if dy != 0.0 {
            enigo.scroll(dy.round() as i32, Axis::Vertical).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }
        if dx != 0.0 {
            enigo.scroll(dx.round() as i32, Axis::Horizontal).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }
        Ok(())
    }).await.context("spawn_blocking panic")??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // `parse_key_str` tests live in `kestrel_proto::keys` now.

    #[test]
    fn normalize_coords() {
        let (px, py) = normalize_to_pixels(0.5, 0.25, 1920, 1080);
        assert_eq!(px, 960);
        assert_eq!(py, 270);
    }

    #[test]
    fn normalize_coords_clamp() {
        let (px, py) = normalize_to_pixels(1.0, 1.0, 1920, 1080);
        assert_eq!(px, 1919);
        assert_eq!(py, 1079);
    }
}
