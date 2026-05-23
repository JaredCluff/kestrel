// crates/kestrel-proto/src/world.rs
//
// World-state wire types for Phase 6. The agent periodically observes
// local OS state (focused app, mouse position, clipboard metadata,
// open shell sessions) and pushes a `Payload::WorldUpdate { state }`
// event when anything has changed. The hub caches the latest per node
// and exposes it via MCP `world_state` / `world_diff_since` tools so
// the AI doesn't have to re-screenshot or re-walk the AX tree just to
// know "what's currently happening" on a machine.
//
// Schema invariants:
//   - No payload bytes in this struct. Clipboard content is summarized
//     as (kind, length, fingerprint); screen content is never carried
//     in world state. The point is to be cheap to query.
//   - `last_observed_unix` is set by the agent at the moment of
//     observation, in seconds since epoch. Used by `world_diff_since`
//     to short-circuit unchanged queries.
//   - All sub-structs and enums are `#[derive(PartialEq)]` so the hub
//     can de-dupe identical states defensively even though the agent
//     already side-checks before sending.

use serde::{Deserialize, Serialize};

/// Top-level snapshot of an agent's observable state at a point in
/// time. Cheap to query from the hub; never contains payload bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorldState {
    pub focused_app: Option<FocusedApp>,
    pub mouse: Option<MousePosition>,
    /// Display geometry as the agent sees it locally. Matches the
    /// `displays` field reported on the `SystemInfo` handshake but
    /// allows the AI to read it back without storing it itself.
    pub displays: Vec<crate::message::DisplayInfo>,
    pub clipboard: Option<ClipboardMetadata>,
    pub shells: Vec<ShellSession>,
    /// Phase 6 follow-up: a coarse "what does the screen look like"
    /// fingerprint. Lets the AI detect screen change without paying
    /// the cost of a full screenshot. 16 hex chars of SHA-256 over
    /// a downsampled luminance grid (8×8 = 64 cells); two screens
    /// with the same average brightness in the same regions get the
    /// same fingerprint. None when no display is available.
    pub screen_fingerprint: Option<String>,
    /// Unix seconds at the moment the agent observed this state.
    /// Used by `world_diff_since(t)` to return null when t > this.
    pub last_observed_unix: u64,
}

impl WorldState {
    /// Empty / unobserved state. Used as the agent's bootstrap state
    /// before the first observation succeeds and as the hub's
    /// placeholder for never-observed nodes.
    pub fn empty() -> Self {
        Self::default()
    }
}

/// The application currently holding keyboard focus on the agent's
/// machine. Includes the process pid for unambiguous identification
/// (window titles change; bundle IDs can collide across helper
/// processes) and the best-effort window title for human-readable
/// context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusedApp {
    pub name: String,
    pub pid: u32,
    pub window_title: Option<String>,
}

/// Mouse cursor location at observation time. Coordinates are
/// per-display in the agent's native pixel space (NOT normalized 0-1
/// like the MCP `mouse_move` tool — that normalization is for the AI
/// to drive the cursor, this is for reading where it currently is).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MousePosition {
    pub x: i32,
    pub y: i32,
    pub display: u8,
}

/// Clipboard contents summary. Never carries the bytes themselves —
/// the point of world state is that it's cheap and leak-resistant.
/// If the AI needs the contents, it calls `clipboard_read`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClipboardMetadata {
    pub kind: ClipboardKind,
    pub byte_len: u64,
    /// First 16 hex chars of SHA-256(contents). Lets the AI detect
    /// "the clipboard changed since last turn" without seeing what
    /// changed to. Truncated rather than full hash so the metadata
    /// is small and the fingerprint can't be reversed.
    pub fingerprint_hex: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClipboardKind {
    Text,
    Image,
}

/// An open interactive PTY session known to the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShellSession {
    pub pty_id: u32,
    /// True iff the agent thinks the child process is still running.
    /// On disagreement with reality (race on exit), the next
    /// `shell_write` will surface the close path.
    pub alive: bool,
    /// Bytes currently sitting in the hub-side buffer ready to be
    /// drained by `shell_read`. The agent doesn't know this exactly
    /// — it reports its own "bytes written since last drain" estimate
    /// from the PTY reader thread.
    pub buffered_bytes: u64,
    /// Unix seconds of the last successful `shell_write` to this PTY.
    pub last_write_unix: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_world_state_is_all_none_or_empty() {
        let s = WorldState::empty();
        assert!(s.focused_app.is_none());
        assert!(s.mouse.is_none());
        assert!(s.displays.is_empty());
        assert!(s.clipboard.is_none());
        assert!(s.shells.is_empty());
        assert_eq!(s.last_observed_unix, 0);
    }

    #[test]
    fn world_state_eq_distinguishes_changes() {
        // Pins that we can rely on PartialEq for the "did anything
        // change?" check on the agent's side and the hub's defensive
        // recheck. Two states that differ in any single field MUST
        // compare unequal.
        let base = WorldState {
            focused_app: Some(FocusedApp {
                name: "Safari".into(),
                pid: 42,
                window_title: Some("Inbox".into()),
            }),
            mouse: Some(MousePosition { x: 100, y: 200, display: 0 }),
            displays: vec![],
            clipboard: Some(ClipboardMetadata {
                kind: ClipboardKind::Text,
                byte_len: 7,
                fingerprint_hex: "deadbeefdeadbeef".into(),
            }),
            screen_fingerprint: None,
            shells: vec![ShellSession {
                pty_id: 1,
                alive: true,
                buffered_bytes: 0,
                last_write_unix: 1700000000,
            }],
            last_observed_unix: 1700000000,
        };
        assert_eq!(base, base.clone());

        let mut changed_app = base.clone();
        changed_app.focused_app.as_mut().unwrap().name = "Mail".into();
        assert_ne!(base, changed_app);

        let mut changed_mouse = base.clone();
        changed_mouse.mouse.as_mut().unwrap().x = 999;
        assert_ne!(base, changed_mouse);

        let mut changed_clipboard = base.clone();
        changed_clipboard.clipboard.as_mut().unwrap().fingerprint_hex = "0000000000000000".into();
        assert_ne!(base, changed_clipboard);

        let mut changed_shells = base.clone();
        changed_shells.shells[0].alive = false;
        assert_ne!(base, changed_shells);

        let mut changed_time = base.clone();
        changed_time.last_observed_unix = 1700000001;
        assert_ne!(base, changed_time);
    }

    #[test]
    fn clipboard_kind_serializes_lowercase() {
        // The schema's clipboard.kind field is serialized as "text"
        // or "image" (lowercase). Pin that so dashboard JSON consumers
        // can rely on the casing.
        let text = serde_json::to_string(&ClipboardKind::Text).unwrap();
        let image = serde_json::to_string(&ClipboardKind::Image).unwrap();
        assert_eq!(text, "\"text\"");
        assert_eq!(image, "\"image\"");
    }
}
