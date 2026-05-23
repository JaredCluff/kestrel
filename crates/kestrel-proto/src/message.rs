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
    // Phase 3 — Clipboard (variants 14-17)
    ClipboardReadReq,
    ClipboardReadResp { content: ClipboardContent },
    ClipboardWriteReq { content: ClipboardContent },
    ClipboardWriteAck,
    // Phase 3 — Shell (variants 18-23)
    ShellSpawn { shell: Option<String>, cols: u16, rows: u16 },
    ShellSpawned { pty_id: u32 },
    ShellWrite { pty_id: u32, data: Vec<u8> },
    ShellOutput { pty_id: u32, data: Vec<u8> },
    ShellResize { pty_id: u32, cols: u16, rows: u16 },
    ShellClose { pty_id: u32 },
    /// Typed error response. Agents emit this in lieu of a normal
    /// response when an operation fails (e.g. shell_run on an unknown
    /// pty_id, write to a closed PTY, describe on a node without an
    /// AX backend). Replaces ad-hoc anyhow stringification on the wire
    /// — callers get a stable code they can switch on plus a human
    /// message they can surface to operators.
    Error { code: ErrorCode, message: String },
    /// Phase 6 — World state event push. Variant 25.
    /// Emitted by the agent's `WorldObserver` every ~2s when its
    /// observation diverges from the last sent state. Hub consumes
    /// these to maintain a per-node `WorldState` cache exposed via
    /// the `world_state` and `world_diff_since` MCP tools.
    WorldUpdate { state: crate::world::WorldState },
    /// Phase 8 — Capability advertisement. Variant 26.
    /// Agent sends this once after `SystemInfo` on every (re)connect.
    /// The hub aggregates per-node capabilities so the AI can ask
    /// for "give me a node with a GPU and a connected display" via
    /// the `fleet_find` MCP tool.
    Capabilities { caps: Capabilities },
    /// Phase 12b — List the plugins available on this node. Variant 27.
    /// Hub sends; agent replies with PluginList.
    PluginListReq,
    PluginListResp { plugins: Vec<PluginInfoWire> },
    /// Phase 12b — Invoke a plugin tool. Variant 29.
    /// args_json carries the tool args as a JSON string so the proto
    /// stays agnostic about plugin schemas.
    PluginCallReq { plugin: String, tool: String, args_json: String },
    PluginCallResp { result_json: String },
}

/// Wire-side mirror of agent's PluginInfo. Lives in proto so both
/// sides can serialize/deserialize without crossing the kestrel-agent
/// dependency boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginInfoWire {
    pub name: String,
    pub version: String,
    pub description: String,
    pub tools: Vec<String>,
}

/// What an agent can do — coarse boolean flags + the OS name. The
/// AI uses these as predicates to find a suitable node without
/// hardcoding `node_id`s. Wire-stable: add new flags only by adding
/// new optional fields with defaults, never re-arrange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Capabilities {
    /// Lowercase OS name: "macos", "linux", "windows", "freebsd", ...
    pub os: String,
    pub has_gpu: bool,
    pub has_display: bool,
    pub has_sudo: bool,
    pub has_docker: bool,
}

/// Stable error codes used in [`Payload::Error`]. Wire-stable: never
/// re-number an existing variant; add new ones at the end. Codes are
/// intentionally coarse — they're meant for the requester to decide
/// "retry vs give up vs ask the operator", not for fine-grained
/// programmatic recovery (the human message carries the detail).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    /// Inputs were structurally invalid (unknown pty_id, malformed
    /// region, etc.). Retrying with the same inputs won't help.
    InvalidArgument,
    /// The agent doesn't support this operation on this platform
    /// (e.g. AX describe on a Linux/Windows agent without a real
    /// backend). Caller should fall back if possible.
    Unsupported,
    /// Permission denied by the OS — Accessibility, clipboard daemon,
    /// PTY spawn, etc. Operator action required to fix.
    PermissionDenied,
    /// Resource not found — pty_id closed, display index out of
    /// range, etc.
    NotFound,
    /// Catchall for unexpected agent-side errors. Look at `message`
    /// for the detail.
    Internal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OsInfo { pub name: String, pub version: String }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClipboardContent {
    Text(String),
    Image { png_bytes: Vec<u8>, width: u32, height: u32 },
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

    #[test]
    fn roundtrip_clipboard_text() {
        let msg = KestrelMessage {
            stream_id: 1,
            kind: MsgKind::Request,
            payload: Payload::ClipboardReadResp {
                content: ClipboardContent::Text("hello clipboard".into()),
            },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_shell_output() {
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::ShellOutput { pty_id: 7, data: b"$ ls\n".to_vec() },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    // The variants below were uncovered by Pass 5; without roundtrip tests,
    // a future reorder/insert of `Payload` variants could silently change a
    // discriminant and break wire-compat without any test failing.

    #[test]
    fn roundtrip_pong() {
        let msg = KestrelMessage {
            stream_id: 9,
            kind: MsgKind::Response,
            payload: Payload::Pong,
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_mouse_button() {
        let msg = KestrelMessage {
            stream_id: 11,
            kind: MsgKind::Request,
            payload: Payload::MouseButton {
                button: Button::Right,
                action: PressRelease::Click,
                x: 0.75,
                y: 0.25,
            },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_scroll() {
        let msg = KestrelMessage {
            stream_id: 12,
            kind: MsgKind::Request,
            payload: Payload::Scroll { dx: -1.5, dy: 3.0 },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_describe_req_resp() {
        let req = KestrelMessage {
            stream_id: 13,
            kind: MsgKind::Request,
            payload: Payload::DescribeReq { display: 0 },
        };
        let resp = KestrelMessage {
            stream_id: 13,
            kind: MsgKind::Response,
            payload: Payload::DescribeResp { tree: AccessibilityNode::unavailable() },
        };
        for msg in [req, resp] {
            assert_eq!(roundtrip(&msg), msg);
        }
    }

    #[test]
    fn roundtrip_clipboard_read_req() {
        let msg = KestrelMessage {
            stream_id: 14,
            kind: MsgKind::Request,
            payload: Payload::ClipboardReadReq,
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_clipboard_write_req_ack() {
        let req = KestrelMessage {
            stream_id: 15,
            kind: MsgKind::Request,
            payload: Payload::ClipboardWriteReq {
                content: ClipboardContent::Text("paste-me".into()),
            },
        };
        let ack = KestrelMessage {
            stream_id: 15,
            kind: MsgKind::Response,
            payload: Payload::ClipboardWriteAck,
        };
        for msg in [req, ack] {
            assert_eq!(roundtrip(&msg), msg);
        }
    }

    #[test]
    fn roundtrip_clipboard_image() {
        let msg = KestrelMessage {
            stream_id: 16,
            kind: MsgKind::Response,
            payload: Payload::ClipboardReadResp {
                content: ClipboardContent::Image {
                    png_bytes: vec![137, 80, 78, 71, 13, 10, 26, 10],
                    width: 800,
                    height: 600,
                },
            },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_shell_spawn_and_spawned() {
        let spawn = KestrelMessage {
            stream_id: 17,
            kind: MsgKind::Request,
            payload: Payload::ShellSpawn {
                shell: Some("/bin/zsh".into()),
                cols: 120,
                rows: 40,
            },
        };
        let spawned = KestrelMessage {
            stream_id: 17,
            kind: MsgKind::Response,
            payload: Payload::ShellSpawned { pty_id: 42 },
        };
        for msg in [spawn, spawned] {
            assert_eq!(roundtrip(&msg), msg);
        }
    }

    #[test]
    fn roundtrip_shell_write_resize_close() {
        let write = KestrelMessage {
            stream_id: 18,
            kind: MsgKind::Request,
            payload: Payload::ShellWrite { pty_id: 42, data: b"echo hi\n".to_vec() },
        };
        let resize = KestrelMessage {
            stream_id: 19,
            kind: MsgKind::Request,
            payload: Payload::ShellResize { pty_id: 42, cols: 80, rows: 24 },
        };
        let close = KestrelMessage {
            stream_id: 20,
            kind: MsgKind::Request,
            payload: Payload::ShellClose { pty_id: 42 },
        };
        for msg in [write, resize, close] {
            assert_eq!(roundtrip(&msg), msg);
        }
    }

    #[test]
    fn roundtrip_world_update_full_and_empty() {
        use crate::world::*;
        // Empty world state — bootstrap shape, all-None / empty vecs.
        let empty = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::WorldUpdate { state: WorldState::empty() },
        };
        assert_eq!(roundtrip(&empty), empty);

        // Fully populated world state — exercises every field
        // including the optional / vec slots. If a field gets added
        // to WorldState in the future without a corresponding line
        // here, this test still passes (additive change) — that's
        // fine; the per-field PartialEq tests in world.rs catch
        // serialization gaps.
        let full = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::WorldUpdate {
                state: WorldState {
                    focused_app: Some(FocusedApp {
                        name: "Safari".into(),
                        pid: 1234,
                        window_title: Some("Inbox — claude.ai".into()),
                    }),
                    mouse: Some(MousePosition { x: 800, y: 600, display: 1 }),
                    displays: vec![
                        DisplayInfo { id: 0, width: 1920, height: 1080 },
                        DisplayInfo { id: 1, width: 2560, height: 1440 },
                    ],
                    clipboard: Some(ClipboardMetadata {
                        kind: ClipboardKind::Text,
                        byte_len: 12,
                        fingerprint_hex: "0123456789abcdef".into(),
                    }),
                    shells: vec![ShellSession {
                        pty_id: 7,
                        alive: true,
                        buffered_bytes: 4096,
                        last_write_unix: 1748000000,
                    }],
                    screen_fingerprint: Some("aabbccddeeff0011".into()),
                    last_observed_unix: 1748010000,
                },
            },
        };
        assert_eq!(roundtrip(&full), full);
    }

    #[test]
    fn roundtrip_error_each_code() {
        // Pin that every ErrorCode variant survives a wire roundtrip.
        // Adding a new code without updating this test would silently
        // give us a variant nothing exercises.
        for code in [
            ErrorCode::InvalidArgument,
            ErrorCode::Unsupported,
            ErrorCode::PermissionDenied,
            ErrorCode::NotFound,
            ErrorCode::Internal,
        ] {
            let msg = KestrelMessage {
                stream_id: 99,
                kind: MsgKind::Response,
                payload: Payload::Error {
                    code: code.clone(),
                    message: format!("test message for {:?}", code),
                },
            };
            assert_eq!(roundtrip(&msg), msg);
        }
    }
}
