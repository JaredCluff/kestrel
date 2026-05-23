// crates/kestrel-hub/src/router.rs
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use kestrel_proto::{AccessibilityNode, Button, ClipboardContent, KeyCode, Modifiers, OsInfo, PressRelease, Rect};
use tokio::sync::RwLock;

use crate::events::{NodeEvent, NodeState, NodeStatus};
use crate::transport::NodeHandle;

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub os: OsInfo,
}

#[derive(Clone)]
pub struct NodeRegistry {
    nodes: Arc<RwLock<HashMap<String, NodeHandle>>>,
    status: Arc<RwLock<HashMap<String, NodeStatus>>>,
    event_tx: tokio::sync::broadcast::Sender<NodeEvent>,
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeRegistry {
    pub fn new() -> Self {
        let (event_tx, _) = tokio::sync::broadcast::channel(64);
        NodeRegistry {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            status: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
        }
    }

    pub async fn register(&self, handle: NodeHandle) {
        let node_id = handle.node_id.clone();
        let os = handle.os_info.clone();
        // Insert the handle FIRST so that any consumer reacting to the
        // `Connected` event (or polling `list()` after seeing `Online` status)
        // finds the handle available for MCP calls. Previously the order was
        // status → nodes → broadcast, which left a microsecond window where
        // `status_snapshot()` reported Online but `screenshot()` etc. errored
        // with "node 'X' not connected".
        self.nodes.write().await.insert(node_id.clone(), handle);
        self.status.write().await.insert(node_id.clone(), NodeStatus {
            node_id: node_id.clone(),
            state: NodeState::Online,
            os: Some(os.clone()),
            latency_ms: None,
            last_seen: SystemTime::now(),
            next_retry_in: None,
        });
        // Broadcast errors only when there are no subscribers — fine, ignore.
        let _ = self.event_tx.send(NodeEvent::Connected { node_id, os });
    }

    pub async fn list(&self) -> Vec<NodeInfo> {
        self.nodes.read().await.values()
            .map(|h| NodeInfo { node_id: h.node_id.clone(), os: h.os_info.clone() })
            .collect()
    }

    /// Sync version for tests that can't be async.
    pub fn list_sync(&self) -> Vec<NodeInfo> {
        self.nodes.try_read()
            .map(|g| g.values().map(|h| NodeInfo { node_id: h.node_id.clone(), os: h.os_info.clone() }).collect())
            .unwrap_or_default()
    }

    async fn get(&self, node_id: &str) -> anyhow::Result<NodeHandle> {
        self.nodes.read().await.get(node_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("node '{}' not connected", node_id))
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<NodeEvent> {
        self.event_tx.subscribe()
    }

    pub async fn status_snapshot(&self) -> Vec<NodeStatus> {
        let mut v: Vec<NodeStatus> = self.status.read().await.values().cloned().collect();
        v.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        v
    }

    /// Best-effort synchronous snapshot for callers that must NOT yield
    /// (e.g. tests pinning "the row exists at this exact point in time, no
    /// scheduler yields allowed in between"). Returns `None` if the lock is
    /// currently held by a writer.
    pub fn try_status_snapshot(&self) -> Option<Vec<NodeStatus>> {
        let guard = self.status.try_read().ok()?;
        let mut v: Vec<NodeStatus> = guard.values().cloned().collect();
        v.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Some(v)
    }

    pub async fn mark_disconnected(&self, node_id: &str, attempt: u32, next_retry_in: Duration) {
        // Remove the dead handle so MCP calls fail fast.
        self.nodes.write().await.remove(node_id);
        if let Some(s) = self.status.write().await.get_mut(node_id) {
            s.state = NodeState::Offline;
            s.next_retry_in = Some(next_retry_in);
            s.last_seen = SystemTime::now();
        }
        let _ = self.event_tx.send(NodeEvent::Disconnected {
            node_id: node_id.to_string(),
            attempt,
            next_retry_in,
        });
    }

    pub async fn mark_reconnecting(&self, node_id: &str, attempt: u32) {
        {
            let mut status = self.status.write().await;
            if let Some(s) = status.get_mut(node_id) {
                s.state = NodeState::Reconnecting;
            } else {
                status.insert(node_id.to_string(), NodeStatus {
                    node_id: node_id.to_string(),
                    state: NodeState::Reconnecting,
                    os: None,
                    latency_ms: None,
                    last_seen: SystemTime::now(),
                    next_retry_in: None,
                });
            }
        }
        let _ = self.event_tx.send(NodeEvent::Reconnecting {
            node_id: node_id.to_string(),
            attempt,
        });
    }

    /// Remove a node from both `nodes` and `status` maps and broadcast a
    /// terminal `Disconnected` event. Idempotent — repeated calls for the same
    /// node_id are safe and each still emits the event.
    pub async fn forget_node(&self, node_id: &str) {
        self.nodes.write().await.remove(node_id);
        self.status.write().await.remove(node_id);
        let _ = self.event_tx.send(NodeEvent::Disconnected {
            node_id: node_id.to_string(),
            attempt: 0,
            next_retry_in: Duration::from_secs(0),
        });
    }

    // ── Phase 2 ───────────────────────────────────────────────────────────────

    pub async fn screenshot(&self, node_id: &str, display: u8, region: Option<Rect>) -> anyhow::Result<Vec<u8>> {
        let handle = self.get(node_id).await?;
        // Pre-flight: validate the display index against what the agent
        // reported at connect time. Without this, an out-of-range index ends
        // up as a generic empty-PNG / "screenshot returned empty bytes" error
        // that doesn't tell the caller why.
        if !handle.displays.is_empty()
            && !handle.displays.iter().any(|d| d.id == display)
        {
            let known: Vec<u8> = handle.displays.iter().map(|d| d.id).collect();
            anyhow::bail!(
                "display {} out of range on '{}' (available: {:?})",
                display,
                node_id,
                known
            );
        }
        handle.screenshot(display, region).await
    }

    pub async fn type_text(&self, node_id: &str, text: String) -> anyhow::Result<()> {
        self.get(node_id).await?.send_type_text(text).await
    }

    /// Press a key combination. Modifier names (ctrl, shift, alt, meta and
    /// their aliases) are folded into a `Modifiers` set and sent as held
    /// state alongside each non-modifier key. The agent's input layer handles
    /// modifier press/release framing atomically per event, so a call like
    /// `key_combo(["ctrl", "c"])` produces a single `KeyEvent` with
    /// `Modifiers { ctrl: true, .. }` and `action: Click`, not four separate
    /// keypresses across the network.
    pub async fn key_combo(&self, node_id: &str, keys: Vec<KeyCode>) -> anyhow::Result<()> {
        let h = self.get(node_id).await?;

        let mut mods = Modifiers::default();
        let mut non_modifiers: Vec<KeyCode> = Vec::new();
        for key in keys {
            // Partition with the shared is_modifier helper so both the
            // proto-level definition and the router stay in sync.
            if kestrel_proto::is_modifier(&key) {
                match key {
                    KeyCode::Control => mods.ctrl = true,
                    KeyCode::Shift => mods.shift = true,
                    KeyCode::Alt => mods.alt = true,
                    KeyCode::Meta => mods.meta = true,
                    _ => unreachable!("is_modifier returned true for a non-modifier KeyCode"),
                }
            } else {
                non_modifiers.push(key);
            }
        }

        if non_modifiers.is_empty() {
            // Modifier-only call (e.g. just `["shift"]`) — fire each as a
            // standalone Click event with no held modifiers.
            for (active, k) in [
                (mods.ctrl, KeyCode::Control),
                (mods.shift, KeyCode::Shift),
                (mods.alt, KeyCode::Alt),
                (mods.meta, KeyCode::Meta),
            ] {
                if active {
                    h.send_key_event(k, Modifiers::default(), PressRelease::Click).await?;
                }
            }
        } else {
            // Send each non-modifier as a Click while holding the modifier
            // set. The agent's `inject_key_event` brackets each event with the
            // modifier press/release so chords like Cmd+Shift+T work.
            for k in non_modifiers {
                h.send_key_event(k, mods.clone(), PressRelease::Click).await?;
            }
        }
        Ok(())
    }

    pub async fn mouse_move(&self, node_id: &str, x: f64, y: f64) -> anyhow::Result<()> {
        self.get(node_id).await?.send_mouse_move(x, y).await
    }

    pub async fn mouse_click(&self, node_id: &str, button: Button, x: f64, y: f64) -> anyhow::Result<()> {
        self.get(node_id).await?.send_mouse_button(button, PressRelease::Click, x, y).await
    }

    pub async fn scroll(&self, node_id: &str, dx: f64, dy: f64) -> anyhow::Result<()> {
        self.get(node_id).await?.send_scroll(dx, dy).await
    }

    pub fn fire_mouse_move(&self, node_id: &str, x: f64, y: f64) {
        let registry = self.clone();
        let node_id = node_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = registry.mouse_move(&node_id, x, y).await {
                tracing::warn!("KVM mouse_move to {} failed: {}", node_id, e);
            }
        });
    }

    // ── Phase 3 clipboard ─────────────────────────────────────────────────────

    pub async fn clipboard_read(&self, node_id: &str) -> anyhow::Result<ClipboardContent> {
        self.get(node_id).await?.clipboard_read().await
    }

    pub async fn clipboard_write(&self, node_id: &str, content: ClipboardContent) -> anyhow::Result<()> {
        self.get(node_id).await?.clipboard_write(content).await
    }

    // ── Phase 4 accessibility ─────────────────────────────────────────────────

    pub async fn describe(&self, node_id: &str, display: u8) -> anyhow::Result<AccessibilityNode> {
        self.get(node_id).await?.describe(display).await
    }

    // ── Phase 3 shell ─────────────────────────────────────────────────────────

    pub async fn run_shell(&self, node_id: &str, command: &str) -> anyhow::Result<String> {
        self.get(node_id).await?.run_shell(command).await
    }

    pub async fn shell_open(&self, node_id: &str, shell: Option<String>, cols: u16, rows: u16) -> anyhow::Result<u32> {
        self.get(node_id).await?.spawn_shell(shell, cols, rows).await
    }

    pub async fn shell_write(&self, node_id: &str, pty_id: u32, data: Vec<u8>) -> anyhow::Result<()> {
        self.get(node_id).await?.write_shell(pty_id, data).await
    }

    pub async fn shell_read(&self, node_id: &str, pty_id: u32) -> anyhow::Result<Vec<u8>> {
        self.get(node_id).await?.read_shell_buffer(pty_id).await
    }

    pub async fn shell_close(&self, node_id: &str, pty_id: u32) -> anyhow::Result<()> {
        self.get(node_id).await?.close_shell(pty_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{NodeEvent, NodeState};
    use std::time::Duration;

    #[test]
    fn registry_starts_empty() {
        let r = NodeRegistry::new();
        assert!(r.list_sync().is_empty());
    }

    // `registry_has_describe_method` was a Phase 4 compile-check whose
    // closure body created an unawaited Future and never asserted real
    // behavior — `NodeRegistry::describe` is exercised by every test that
    // calls the MCP `describe` tool through the registry. Removed in Pass 2.

    #[tokio::test]
    async fn subscribe_receives_disconnect_event() {
        let r = NodeRegistry::new();
        let mut rx = r.subscribe();
        r.mark_disconnected("a", 2, Duration::from_secs(4)).await;
        let evt = rx.recv().await.unwrap();
        match evt {
            NodeEvent::Disconnected { node_id, attempt, next_retry_in } => {
                assert_eq!(node_id, "a");
                assert_eq!(attempt, 2);
                assert_eq!(next_retry_in, Duration::from_secs(4));
            }
            _ => panic!("expected Disconnected, got {:?}", evt),
        }
    }

    #[tokio::test]
    async fn status_snapshot_includes_reconnecting() {
        let r = NodeRegistry::new();
        r.mark_reconnecting("a", 1).await;
        let snap = r.status_snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].node_id, "a");
        assert_eq!(snap[0].state, NodeState::Reconnecting);
    }

    #[tokio::test]
    async fn status_snapshot_sorted_by_node_id() {
        let r = NodeRegistry::new();
        r.mark_reconnecting("b", 1).await;
        r.mark_reconnecting("a", 1).await;
        r.mark_reconnecting("c", 1).await;
        let snap = r.status_snapshot().await;
        let ids: Vec<&str> = snap.iter().map(|s| s.node_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn forget_node_removes_from_status_and_emits_disconnect() {
        let r = NodeRegistry::new();
        let mut rx = r.subscribe();
        r.mark_reconnecting("a", 1).await;
        let _ = rx.recv().await; // consume Reconnecting

        r.forget_node("a").await;

        // Status row gone.
        let snap = r.status_snapshot().await;
        assert!(snap.iter().all(|s| s.node_id != "a"));

        // Disconnected event broadcast.
        let evt = rx.recv().await.unwrap();
        match evt {
            NodeEvent::Disconnected { node_id, .. } => assert_eq!(node_id, "a"),
            other => panic!("expected Disconnected, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn forget_node_is_idempotent() {
        let r = NodeRegistry::new();
        r.forget_node("ghost").await; // no panic; just emits an event with attempt=0
        r.forget_node("ghost").await; // still no panic
    }
}
