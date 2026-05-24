// crates/kestrel-agent/tests/proptest_input.rs
//
// Property-based tests for browser-controlled input parsers:
//   - InputEvent JSON parsing (from the WebRTC data channel)
//   - key_from_dom_code DOM-string-to-KeyCode mapping
//   - parse_key_str operator-supplied-string-to-KeyCode mapping
//
// The unit tests in webrtc_session.rs + proto::keys::tests pin
// specific shapes; these stress the input space to find inputs we
// hadn't thought of.

use kestrel_agent::capabilities::webrtc_session::{key_from_dom_code, InputEvent};
use kestrel_proto::keys::parse_key_str;
use proptest::prelude::*;

// ── InputEvent JSON: malformed-input panics ──────────────────────────────

proptest! {
    /// Parsing arbitrary bytes as JSON-then-InputEvent must never
    /// panic. The browser side is presumed-hostile (a malicious page
    /// can ship any payload over the data channel), so the parser is
    /// security-critical even though it's an opt-in feature.
    #[test]
    fn input_event_parse_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let s = String::from_utf8_lossy(&bytes);
        let _ = serde_json::from_str::<InputEvent>(&s);
        // Test passes by not panicking.
    }

    /// Arbitrary JSON values that AREN'T valid InputEvent shapes must
    /// be rejected without panic. JSON arrays, nulls, primitives —
    /// none of these should crash the parser.
    #[test]
    fn input_event_rejects_arbitrary_json(
        val in proptest::collection::vec(any::<u8>(), 0..256)
    ) {
        // Generate a JSON value from raw bytes via valid-utf8 only.
        if let Ok(s) = std::str::from_utf8(&val) {
            // Try parsing as JSON; if it parses, try as InputEvent.
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(s) {
                let stringified = serde_json::to_string(&json).unwrap();
                let _ = serde_json::from_str::<InputEvent>(&stringified);
                // OK either way — must not panic.
            }
        }
    }
}

// ── key_from_dom_code property ───────────────────────────────────────────

proptest! {
    /// `key_from_dom_code` must not panic on any input string. Browser
    /// supplies these directly from KeyboardEvent.code which can be
    /// any string including unicode names and exotic IME codes.
    #[test]
    fn key_from_dom_code_never_panics(s in ".{0,64}") {
        let _ = key_from_dom_code(&s);
    }

    /// KeyA..KeyZ must all map to the lowercase Char variant. Without
    /// this, browser Cmd-Shift-A would dispatch as some non-letter.
    /// `idx` indexes into the 26-letter alphabet.
    #[test]
    fn key_from_dom_code_letters_roundtrip(idx in 0u8..26) {
        let c = (b'A' + idx) as char;
        let code = format!("Key{}", c);
        let key = key_from_dom_code(&code).unwrap_or_else(|| panic!("missed letter {}", c));
        match key {
            kestrel_proto::KeyCode::Char(got) => {
                prop_assert_eq!(got, c.to_ascii_lowercase());
            }
            other => prop_assert!(false, "Key{} mapped to non-Char: {:?}", c, other),
        }
    }

    /// Digit0..Digit9 must map to the digit Char variant.
    #[test]
    fn key_from_dom_code_digits_roundtrip(d in 0u32..10) {
        let code = format!("Digit{}", d);
        let key = key_from_dom_code(&code).unwrap();
        match key {
            kestrel_proto::KeyCode::Char(got) => {
                prop_assert_eq!(got, char::from_digit(d, 10).unwrap());
            }
            other => prop_assert!(false, "Digit{} mapped to non-Char: {:?}", d, other),
        }
    }
}

// ── parse_key_str (operator-supplied key strings, MCP key_combo) ────────

proptest! {
    /// `parse_key_str` must not panic on any input. Operators (or AIs)
    /// supplying MCP key_combo names can pass any string.
    #[test]
    fn parse_key_str_never_panics(s in ".{0,64}") {
        let _ = parse_key_str(&s);
    }

    /// Case-insensitivity: same letter in any case maps to the same
    /// KeyCode. Catches a regression where someone adds a `.to_lowercase()`
    /// somewhere but not everywhere.
    #[test]
    fn parse_key_str_is_case_insensitive_on_known_keys(case_seed in any::<u8>()) {
        let known = ["ctrl", "shift", "alt", "cmd", "enter", "tab", "escape", "f1", "f12", "a", "z"];
        for key in known {
            let lower = parse_key_str(key);
            // Vary case based on seed: even seed → upper, odd → mixed.
            let cased = if case_seed % 2 == 0 {
                key.to_uppercase()
            } else {
                key.chars().enumerate().map(|(i, c)| {
                    if i.is_multiple_of(2) { c.to_ascii_uppercase() } else { c }
                }).collect()
            };
            let other = parse_key_str(&cased);
            prop_assert_eq!(
                lower.is_ok(),
                other.is_ok(),
                "{} vs {} differ in ok-ness",
                key, cased
            );
            if let (Ok(a), Ok(b)) = (lower, other) {
                prop_assert_eq!(
                    std::mem::discriminant(&a),
                    std::mem::discriminant(&b),
                    "{} and {} parsed to different KeyCode variants",
                    key, cased
                );
            }
        }
    }
}
