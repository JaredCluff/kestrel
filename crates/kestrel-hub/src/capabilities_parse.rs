// crates/kestrel-hub/src/capabilities_parse.rs
use kestrel_proto::KeyCode;

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
        "f1"  => KeyCode::F1,  "f2"  => KeyCode::F2,  "f3"  => KeyCode::F3,
        "f4"  => KeyCode::F4,  "f5"  => KeyCode::F5,  "f6"  => KeyCode::F6,
        "f7"  => KeyCode::F7,  "f8"  => KeyCode::F8,  "f9"  => KeyCode::F9,
        "f10" => KeyCode::F10, "f11" => KeyCode::F11, "f12" => KeyCode::F12,
        s if s.chars().count() == 1 => KeyCode::Char(s.chars().next().unwrap()),
        other => anyhow::bail!("unknown key: {}", other),
    })
}
