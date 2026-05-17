use anyhow::Context;
use enigo::{Axis, Button as EnigoButton, Coordinate, Direction, Enigo, Key, Keyboard, Mouse, Settings};
use kestrel_proto::{Button, KeyCode, Modifiers, PressRelease};

pub fn normalize_to_pixels(x: f64, y: f64, width: u32, height: u32) -> (i32, i32) {
    let px = (x * width as f64).round() as i32;
    let py = (y * height as f64).round() as i32;
    (px, py)
}

pub fn parse_key_str(s: &str) -> anyhow::Result<KeyCode> {
    Ok(match s.to_lowercase().as_str() {
        "ctrl" | "control" => KeyCode::Control,
        "shift" => KeyCode::Shift,
        "alt" | "option" => KeyCode::Alt,
        "meta" | "cmd" | "command" | "super" | "win" => KeyCode::Meta,
        "return" | "enter" => KeyCode::Return,
        "backspace" => KeyCode::Backspace,
        "tab" => KeyCode::Tab,
        "escape" | "esc" => KeyCode::Escape,
        "delete" | "del" => KeyCode::Delete,
        "home" => KeyCode::Home,
        "end" => KeyCode::End,
        "pageup" | "pgup" => KeyCode::PageUp,
        "pagedown" | "pgdn" => KeyCode::PageDown,
        "up" => KeyCode::Up,
        "down" => KeyCode::Down,
        "left" => KeyCode::Left,
        "right" => KeyCode::Right,
        "space" => KeyCode::Space,
        "f1" => KeyCode::F1,
        "f2" => KeyCode::F2,
        "f3" => KeyCode::F3,
        "f4" => KeyCode::F4,
        "f5" => KeyCode::F5,
        "f6" => KeyCode::F6,
        "f7" => KeyCode::F7,
        "f8" => KeyCode::F8,
        "f9" => KeyCode::F9,
        "f10" => KeyCode::F10,
        "f11" => KeyCode::F11,
        "f12" => KeyCode::F12,
        s if s.chars().count() == 1 => KeyCode::Char(s.chars().next().unwrap()),
        other => anyhow::bail!("unknown key: {}", other),
    })
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
    display_w: u32,
    display_h: u32,
) -> anyhow::Result<()> {
    let _ = (display_w, display_h);
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut enigo = Enigo::new(&Settings::default())
            .map_err(|e| anyhow::anyhow!("enigo init: {e:?}"))?;
        let dir = to_enigo_dir(&action);
        if matches!(action, PressRelease::Press | PressRelease::Click) {
            if mods.ctrl  { enigo.key(Key::Control, Direction::Press).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.shift { enigo.key(Key::Shift,   Direction::Press).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.alt   { enigo.key(Key::Alt,     Direction::Press).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.meta  { enigo.key(Key::Meta,    Direction::Press).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
        }
        enigo.key(to_enigo_key(&key), dir).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        if matches!(action, PressRelease::Release | PressRelease::Click) {
            if mods.meta  { enigo.key(Key::Meta,    Direction::Release).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.alt   { enigo.key(Key::Alt,     Direction::Release).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.shift { enigo.key(Key::Shift,   Direction::Release).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
            if mods.ctrl  { enigo.key(Key::Control, Direction::Release).map_err(|e| anyhow::anyhow!("{e:?}"))?; }
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
    use kestrel_proto::KeyCode;

    #[test]
    fn key_string_parsing() {
        assert!(matches!(parse_key_str("ctrl"), Ok(KeyCode::Control)));
        assert!(matches!(parse_key_str("shift"), Ok(KeyCode::Shift)));
        assert!(matches!(parse_key_str("return"), Ok(KeyCode::Return)));
        assert!(matches!(parse_key_str("escape"), Ok(KeyCode::Escape)));
        assert!(matches!(parse_key_str("a"), Ok(KeyCode::Char('a'))));
        assert!(parse_key_str("notakey_xyz").is_err());
    }

    #[test]
    fn normalize_coords() {
        let (px, py) = normalize_to_pixels(0.5, 0.25, 1920, 1080);
        assert_eq!(px, 960);
        assert_eq!(py, 270);
    }

    #[test]
    fn normalize_coords_clamp() {
        let (px, py) = normalize_to_pixels(1.0, 1.0, 1920, 1080);
        assert_eq!(px, 1920);
        assert_eq!(py, 1080);
    }
}
