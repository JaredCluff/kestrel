// crates/kestrel-proto/src/keys.rs
//
// Single source of truth for parsing operator-supplied key strings (e.g. from
// the MCP `key_combo` tool or the hub CLI). Lived in two places (the agent's
// input.rs and the hub's capabilities_parse.rs) before this — keeping it in
// the shared proto crate prevents the two parsers from drifting and producing
// subtly different chord behavior between MCP input and CLI-injected input.

use crate::KeyCode;

/// Parse an operator-supplied key string into a `KeyCode`. Accepts:
///
/// - Modifier names: `ctrl`/`control`, `shift`, `alt`/`option`,
///   `meta`/`cmd`/`command`/`super`/`win`.
/// - Special keys: `return`/`enter`, `backspace`, `tab`, `escape`/`esc`,
///   `delete`/`del`, `home`, `end`, `pageup`/`pgup`, `pagedown`/`pgdn`,
///   `up`, `down`, `left`, `right`, `space`, `capslock`/`caps_lock`/`caps`,
///   `numlock`/`num_lock`/`numpad_lock`.
/// - Function keys: `f1`..`f12`.
/// - Single Unicode characters: e.g. `a`, `c`, `0`, `=`.
///
/// Case-insensitive. Returns `Err` for anything else.
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
        "capslock" | "caps_lock" | "caps" => KeyCode::CapsLock,
        "numlock" | "num_lock" | "numpad_lock" => KeyCode::NumLock,
        s if s.chars().count() == 1 => KeyCode::Char(s.chars().next().unwrap()),
        other => anyhow::bail!("unknown key: {}", other),
    })
}

/// Returns true if `kc` is one of the four modifier keys recognized by the
/// protocol's `Modifiers` struct. The hub's `key_combo` uses this to split an
/// operator's key list into modifiers (folded into a `Modifiers` set) vs.
/// non-modifiers (sent as Click events with the modifier set held).
pub fn is_modifier(kc: &KeyCode) -> bool {
    matches!(
        kc,
        KeyCode::Control | KeyCode::Shift | KeyCode::Alt | KeyCode::Meta
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modifier_aliases() {
        assert!(matches!(parse_key_str("ctrl"), Ok(KeyCode::Control)));
        assert!(matches!(parse_key_str("control"), Ok(KeyCode::Control)));
        assert!(matches!(parse_key_str("cmd"), Ok(KeyCode::Meta)));
        assert!(matches!(parse_key_str("command"), Ok(KeyCode::Meta)));
        assert!(matches!(parse_key_str("win"), Ok(KeyCode::Meta)));
        assert!(matches!(parse_key_str("option"), Ok(KeyCode::Alt)));
    }

    #[test]
    fn special_keys() {
        assert!(matches!(parse_key_str("return"), Ok(KeyCode::Return)));
        assert!(matches!(parse_key_str("enter"), Ok(KeyCode::Return)));
        assert!(matches!(parse_key_str("escape"), Ok(KeyCode::Escape)));
        assert!(matches!(parse_key_str("esc"), Ok(KeyCode::Escape)));
        assert!(matches!(parse_key_str("pgup"), Ok(KeyCode::PageUp)));
    }

    #[test]
    fn function_keys() {
        // Pin every variant — a regression that swapped match arms
        // (e.g. "f1" => KeyCode::F12) would silently route the wrong key
        // into the agent without an Ok(_)-only assertion catching it.
        let expected = [
            KeyCode::F1, KeyCode::F2, KeyCode::F3, KeyCode::F4,
            KeyCode::F5, KeyCode::F6, KeyCode::F7, KeyCode::F8,
            KeyCode::F9, KeyCode::F10, KeyCode::F11, KeyCode::F12,
        ];
        for (n, want) in (1u8..=12).zip(expected) {
            let got = parse_key_str(&format!("f{n}")).unwrap();
            assert_eq!(
                std::mem::discriminant(&got),
                std::mem::discriminant(&want),
                "f{n} should parse to F{n}, got {:?}",
                got
            );
        }
    }

    #[test]
    fn char_keys() {
        assert!(matches!(parse_key_str("a"), Ok(KeyCode::Char('a'))));
        assert!(matches!(parse_key_str("Z"), Ok(KeyCode::Char('z'))));
        assert!(matches!(parse_key_str("0"), Ok(KeyCode::Char('0'))));
    }

    #[test]
    fn unknown_errors() {
        assert!(parse_key_str("notakey_xyz").is_err());
        assert!(parse_key_str("").is_err());
        assert!(parse_key_str("ctrl-c").is_err());
    }

    #[test]
    fn is_modifier_classifies_correctly() {
        assert!(is_modifier(&KeyCode::Control));
        assert!(is_modifier(&KeyCode::Shift));
        assert!(is_modifier(&KeyCode::Alt));
        assert!(is_modifier(&KeyCode::Meta));
        assert!(!is_modifier(&KeyCode::Char('c')));
        assert!(!is_modifier(&KeyCode::Return));
        assert!(!is_modifier(&KeyCode::F1));
    }
}
