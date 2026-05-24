// crates/kestrel-proto/tests/proptest_wire.rs
//
// Property-based tests for the wire-facing parsers in kestrel-proto.
// These exercise the round-trip property (decode(encode(x)) == x) and
// the failure path (no input causes a panic).
//
// The hand-written `roundtrip_*` unit tests in src/message.rs pin
// specific variants we care about; these complement them by generating
// arbitrary instances and stressing every path the encoder takes.

use kestrel_proto::{
    AccessibilityNode, Button, Capabilities, ClipboardContent, ClipboardKind,
    ClipboardMetadata, DisplayInfo, ErrorCode, FocusedApp, KestrelMessage,
    KeyCode, Modifiers, MousePosition, MsgKind, OsInfo, Payload, PressRelease,
    Rect, ShellSession, WorldState,
};
use proptest::prelude::*;

fn encode(msg: &KestrelMessage) -> Vec<u8> {
    bincode::serde::encode_to_vec(msg, bincode::config::standard()).expect("encode")
}

fn decode(bytes: &[u8]) -> KestrelMessage {
    let (msg, _) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard()).expect("decode");
    msg
}

// ── Strategy helpers ──────────────────────────────────────────────────────

fn any_msgkind() -> impl Strategy<Value = MsgKind> {
    prop_oneof![
        Just(MsgKind::Request),
        Just(MsgKind::Response),
        Just(MsgKind::Event),
        Just(MsgKind::Ack),
    ]
}

fn any_keycode() -> impl Strategy<Value = KeyCode> {
    prop_oneof![
        // Pin the special variants explicitly.
        Just(KeyCode::Return), Just(KeyCode::Backspace), Just(KeyCode::Tab),
        Just(KeyCode::Escape), Just(KeyCode::Delete), Just(KeyCode::Home),
        Just(KeyCode::End), Just(KeyCode::PageUp), Just(KeyCode::PageDown),
        Just(KeyCode::Up), Just(KeyCode::Down), Just(KeyCode::Left),
        Just(KeyCode::Right), Just(KeyCode::Space),
        Just(KeyCode::Control), Just(KeyCode::Alt), Just(KeyCode::Shift),
        Just(KeyCode::Meta), Just(KeyCode::CapsLock), Just(KeyCode::NumLock),
        Just(KeyCode::F1), Just(KeyCode::F2), Just(KeyCode::F12),
        // And cover Char(c) over the full printable ASCII range.
        any::<char>().prop_filter("non-control", |c| !c.is_control()).prop_map(KeyCode::Char),
    ]
}

fn any_modifiers() -> impl Strategy<Value = Modifiers> {
    (any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>())
        .prop_map(|(shift, ctrl, alt, meta)| Modifiers { shift, ctrl, alt, meta })
}

fn any_pressrelease() -> impl Strategy<Value = PressRelease> {
    prop_oneof![
        Just(PressRelease::Press),
        Just(PressRelease::Release),
        Just(PressRelease::Click),
    ]
}

fn any_button() -> impl Strategy<Value = Button> {
    prop_oneof![Just(Button::Left), Just(Button::Right), Just(Button::Middle)]
}

fn any_errorcode() -> impl Strategy<Value = ErrorCode> {
    prop_oneof![
        Just(ErrorCode::InvalidArgument),
        Just(ErrorCode::Unsupported),
        Just(ErrorCode::PermissionDenied),
        Just(ErrorCode::NotFound),
        Just(ErrorCode::Internal),
    ]
}

fn any_clipboard_content() -> impl Strategy<Value = ClipboardContent> {
    prop_oneof![
        // Arbitrary unicode strings — bincode + serde handle UTF-8 fine.
        ".{0,256}".prop_map(ClipboardContent::Text),
        (any::<u32>(), any::<u32>(), prop::collection::vec(any::<u8>(), 0..64))
            .prop_map(|(width, height, png_bytes)| ClipboardContent::Image {
                png_bytes, width, height
            }),
    ]
}

// Variants that don't carry nested complex state are the easy ones —
// generating arbitrary payloads for every variant would balloon this
// file. We cover a representative subset, including each "shape" of
// payload (unit, primitive, string, nested struct, vec).
fn any_payload() -> impl Strategy<Value = Payload> {
    prop_oneof![
        Just(Payload::Ping),
        Just(Payload::Pong),
        any::<[u8; 32]>().prop_map(|nonce| Payload::Challenge { nonce }),
        (any::<[u8; 32]>(), ".{0,64}").prop_map(|(mac, node_id)| Payload::AuthResponse { mac, node_id }),
        (any_keycode(), any_modifiers(), any_pressrelease())
            .prop_map(|(key, modifiers, action)| Payload::KeyEvent { key, modifiers, action }),
        ".{0,256}".prop_map(|text| Payload::TypeText { text }),
        (any::<f64>(), any::<f64>())
            .prop_filter("finite", |(x, y)| x.is_finite() && y.is_finite())
            .prop_map(|(x, y)| Payload::MouseMove { x, y }),
        (any_button(), any_pressrelease(), any::<f64>(), any::<f64>())
            .prop_filter("finite", |(_, _, x, y)| x.is_finite() && y.is_finite())
            .prop_map(|(button, action, x, y)| Payload::MouseButton { button, action, x, y }),
        (any::<f64>(), any::<f64>())
            .prop_filter("finite", |(dx, dy)| dx.is_finite() && dy.is_finite())
            .prop_map(|(dx, dy)| Payload::Scroll { dx, dy }),
        (any::<u8>(), prop::option::of(rect())).prop_map(|(display, region)|
            Payload::ScreenshotReq { display, region }
        ),
        prop::collection::vec(any::<u8>(), 0..1024)
            .prop_map(|png_bytes| Payload::ScreenshotResp { png_bytes }),
        any_clipboard_content().prop_map(|content| Payload::ClipboardReadResp { content }),
        any_clipboard_content().prop_map(|content| Payload::ClipboardWriteReq { content }),
        Just(Payload::ClipboardReadReq),
        Just(Payload::ClipboardWriteAck),
        (any_errorcode(), ".{0,128}")
            .prop_map(|(code, message)| Payload::Error { code, message }),
        (".{0,32}", ".{0,4096}").prop_map(|(session_id, sdp)|
            Payload::WebRtcOffer { session_id, sdp }
        ),
        (".{0,32}", ".{0,4096}").prop_map(|(session_id, sdp)|
            Payload::WebRtcAnswer { session_id, sdp }
        ),
        (".{0,32}", ".{0,512}").prop_map(|(session_id, candidate)|
            Payload::WebRtcIce { session_id, candidate }
        ),
    ]
}

fn rect() -> impl Strategy<Value = Rect> {
    (any::<f64>(), any::<f64>(), any::<f64>(), any::<f64>())
        .prop_filter("finite", |(a, b, c, d)| {
            a.is_finite() && b.is_finite() && c.is_finite() && d.is_finite()
        })
        .prop_map(|(x, y, w, h)| Rect { x, y, w, h })
}

fn any_message() -> impl Strategy<Value = KestrelMessage> {
    (any::<u32>(), any_msgkind(), any_payload())
        .prop_map(|(stream_id, kind, payload)| KestrelMessage { stream_id, kind, payload })
}

// ── Properties ────────────────────────────────────────────────────────────

proptest! {
    /// Encoding and decoding any KestrelMessage must round-trip
    /// byte-for-byte. If this ever fails, the wire format silently
    /// changed shape — a real disaster for cross-version compat.
    #[test]
    fn roundtrip_any_message(msg in any_message()) {
        let bytes = encode(&msg);
        let decoded = decode(&bytes);
        prop_assert_eq!(decoded, msg);
    }

    /// Decoding random bytes either succeeds with a valid message
    /// or returns an error — must never panic. This is the fuzz-y
    /// half of the property: an attacker who can inject arbitrary
    /// bytes into our WebSocket channel can't crash the agent or hub
    /// just by sending garbage.
    #[test]
    fn decode_random_bytes_does_not_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let _ = bincode::serde::decode_from_slice::<KestrelMessage, _>(
            &bytes,
            bincode::config::standard(),
        );
        // Test passes by not panicking.
    }
}

// ── Smaller targeted property tests ──────────────────────────────────────

proptest! {
    /// `derive_per_node_psk` must be deterministic for any (master,
    /// node_id) input. We already pin this with hardcoded inputs in
    /// auth.rs::tests; the property check covers the input space.
    #[test]
    fn derive_psk_is_deterministic(
        master in prop::collection::vec(any::<u8>(), 1..64),
        node_id in ".{0,128}",
    ) {
        let a = kestrel_proto::derive_per_node_psk(&master, &node_id);
        let b = kestrel_proto::derive_per_node_psk(&master, &node_id);
        prop_assert_eq!(a, b);
    }

    /// Different node_ids under the same master must produce different
    /// PSKs. HKDF guarantees this with overwhelming probability for
    /// any reasonable input — we sample to exercise it.
    #[test]
    fn derive_psk_differs_per_node_id(
        master in prop::collection::vec(any::<u8>(), 1..64),
        a in ".{1,32}",
        b in ".{1,32}",
    ) {
        prop_assume!(a != b);
        let pa = kestrel_proto::derive_per_node_psk(&master, &a);
        let pb = kestrel_proto::derive_per_node_psk(&master, &b);
        prop_assert_ne!(pa, pb);
    }

    /// HMAC verification must accept correct (psk, nonce, exporter,
    /// mac) tuples for any inputs the caller supplies, and must reject
    /// when the exporter (or anything else) differs.
    #[test]
    fn hmac_verify_roundtrip(
        psk in prop::collection::vec(any::<u8>(), 16..64),
        nonce in any::<[u8; 32]>(),
        exporter in any::<[u8; 32]>(),
    ) {
        let mac = kestrel_proto::hmac_response(&psk, &nonce, &exporter);
        prop_assert!(kestrel_proto::verify_response(&psk, &nonce, &exporter, &mac));
    }

    /// MITM defense at the property level: if the exporter the
    /// verifier sees differs from the one the MAC was computed under,
    /// verification MUST fail.
    #[test]
    fn hmac_rejects_swapped_exporter(
        psk in prop::collection::vec(any::<u8>(), 16..64),
        nonce in any::<[u8; 32]>(),
        e1 in any::<[u8; 32]>(),
        e2 in any::<[u8; 32]>(),
    ) {
        prop_assume!(e1 != e2);
        let mac = kestrel_proto::hmac_response(&psk, &nonce, &e1);
        prop_assert!(!kestrel_proto::verify_response(&psk, &nonce, &e2, &mac));
    }
}

// Suppress some unused-imports warnings when proptest's macros
// expand to references that don't materialize in every variant.
#[allow(dead_code)]
fn _suppress_unused() -> (
    OsInfo, DisplayInfo, AccessibilityNode, FocusedApp, MousePosition,
    ShellSession, ClipboardMetadata, ClipboardKind, Capabilities, WorldState,
) {
    unimplemented!()
}
