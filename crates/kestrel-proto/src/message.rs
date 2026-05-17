// crates/kestrel-proto/src/message.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KestrelMessage {
    pub stream_id: u32,
    pub kind: MsgKind,
    pub payload: Payload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MsgKind { Request, Response, Event, Ack }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Payload {
    // Phase 1 — Auth + System (variants 0-4, discriminants unchanged)
    Challenge { nonce: [u8; 32] },
    AuthResponse { mac: [u8; 32], node_id: String },
    SystemInfo { os: OsInfo, displays: Vec<DisplayInfo>, hostname: String },
    Ping,
    Pong,
    // Phase 2 — Input (variants 5-9)
    KeyEvent { key: KeyCode, modifiers: Modifiers, action: PressRelease },
    TypeText { text: String },
    MouseMove { x: f64, y: f64 },
    MouseButton { button: Button, action: PressRelease, x: f64, y: f64 },
    Scroll { dx: f64, dy: f64 },
    // Phase 2 — Screen (variants 10-13)
    ScreenshotReq { display: u8, region: Option<Rect> },
    ScreenshotResp { png_bytes: Vec<u8> },
    DescribeReq { display: u8 },
    DescribeResp { tree: AccessibilityNode },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OsInfo { pub name: String, pub version: String }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DisplayInfo { pub id: u8, pub width: u32, pub height: u32 }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KeyCode {
    Char(char),
    Return, Backspace, Tab, Escape, Delete,
    Home, End, PageUp, PageDown,
    Up, Down, Left, Right,
    F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12,
    Control, Alt, Shift, Meta,
    Space, CapsLock, NumLock,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Modifiers {
    pub shift: bool,
    pub ctrl: bool,
    pub alt: bool,
    pub meta: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PressRelease { Press, Release, Click }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Button { Left, Right, Middle }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rect { pub x: f64, pub y: f64, pub w: f64, pub h: f64 }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessibilityNode {
    pub role: String,
    pub label: Option<String>,
    pub value: Option<String>,
    pub focused: bool,
    pub enabled: bool,
    pub bounds: Option<Rect>,
    pub children: Vec<AccessibilityNode>,
    /// True when the real AX tree was unavailable; caller should use screenshot instead.
    pub fallback: bool,
}

impl AccessibilityNode {
    pub fn unavailable() -> Self {
        AccessibilityNode {
            role: "root".into(),
            label: None,
            value: None,
            focused: false,
            enabled: true,
            bounds: None,
            children: vec![],
            fallback: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: &KestrelMessage) -> KestrelMessage {
        let bytes = bincode::serde::encode_to_vec(msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        decoded
    }

    #[test]
    fn roundtrip_ping() {
        let msg = KestrelMessage { stream_id: 1, kind: MsgKind::Request, payload: Payload::Ping };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_system_info() {
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::SystemInfo {
                os: OsInfo { name: "linux".into(), version: "6.8".into() },
                displays: vec![DisplayInfo { id: 0, width: 1920, height: 1080 }],
                hostname: "dev-box".into(),
            },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_auth_challenge() {
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Request,
            payload: Payload::Challenge { nonce: [0xABu8; 32] },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_auth_response() {
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Response,
            payload: Payload::AuthResponse { mac: [0xDEu8; 32], node_id: "hub".into() },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_key_event() {
        let msg = KestrelMessage {
            stream_id: 1,
            kind: MsgKind::Request,
            payload: Payload::KeyEvent {
                key: KeyCode::Char('a'),
                modifiers: Modifiers::default(),
                action: PressRelease::Click,
            },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_mouse_move() {
        let msg = KestrelMessage {
            stream_id: 2,
            kind: MsgKind::Request,
            payload: Payload::MouseMove { x: 0.5, y: 0.3 },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_screenshot_req_resp() {
        let req = KestrelMessage {
            stream_id: 3,
            kind: MsgKind::Request,
            payload: Payload::ScreenshotReq { display: 0, region: None },
        };
        let resp = KestrelMessage {
            stream_id: 3,
            kind: MsgKind::Response,
            payload: Payload::ScreenshotResp { png_bytes: vec![137, 80, 78, 71] },
        };
        for msg in [req, resp] {
            assert_eq!(roundtrip(&msg), msg);
        }
    }

    #[test]
    fn roundtrip_type_text() {
        let msg = KestrelMessage {
            stream_id: 4,
            kind: MsgKind::Request,
            payload: Payload::TypeText { text: "hello world".into() },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_screenshot_with_region() {
        let msg = KestrelMessage {
            stream_id: 5,
            kind: MsgKind::Request,
            payload: Payload::ScreenshotReq {
                display: 0,
                region: Some(Rect { x: 0.1, y: 0.1, w: 0.8, h: 0.8 }),
            },
        };
        assert_eq!(roundtrip(&msg), msg);
    }
}
