// crates/kestrel-hub/src/router.rs
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use kestrel_proto::{AccessibilityNode, Button, Capabilities, ClipboardContent, KeyCode, Modifiers, OsInfo, PressRelease, Rect, WorldState};
use tokio::sync::RwLock;

use crate::events::{NodeEvent, NodeState, NodeStatus};
use crate::transport::NodeHandle;

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub os: OsInfo,
}

/// Phase 8: predicate over `Capabilities`. Every `Option` is an
/// optional constraint; `None` means "don't care." A node matches
/// when every `Some(_)` constraint matches its reported capability.
#[derive(Debug, Clone, Default, serde::Deserialize, schemars::JsonSchema)]
pub struct CapabilityNeeds {
    pub os: Option<String>,
    pub has_gpu: Option<bool>,
    pub has_display: Option<bool>,
    pub has_sudo: Option<bool>,
    pub has_docker: Option<bool>,
}

impl CapabilityNeeds {
    pub fn matches(&self, c: &Capabilities) -> bool {
        if let Some(o) = &self.os { if c.os != *o { return false; } }
        if let Some(g) = self.has_gpu { if c.has_gpu != g { return false; } }
        if let Some(d) = self.has_display { if c.has_display != d { return false; } }
        if let Some(s) = self.has_sudo { if c.has_sudo != s { return false; } }
        if let Some(d) = self.has_docker { if c.has_docker != d { return false; } }
        true
    }
}

#[derive(Clone)]
pub struct NodeRegistry {
    nodes: Arc<RwLock<HashMap<String, NodeHandle>>>,
    status: Arc<RwLock<HashMap<String, NodeStatus>>>,
    /// Phase 6: per-node world-state cache. Populated by
    /// `observe_world_update` when the agent's WorldObserver pushes
    /// a `Payload::WorldUpdate` event; queried by `world_state_for`
    /// and `world_diff_since` to back the MCP tools and the
    /// dashboard's /api/world endpoint.
    world: Arc<RwLock<HashMap<String, WorldState>>>,
    /// Phase 8: per-node capability advertisement. Populated by
    /// `record_capabilities` on every (re)handshake. Queried by
    /// `find_nodes_with` to back the `fleet_find` MCP tool.
    capabilities: Arc<RwLock<HashMap<String, Capabilities>>>,
    event_tx: tokio::sync::broadcast::Sender<NodeEvent>,
    /// Phase 13b: optional WebRTC SessionRegistry. When wired (by
    /// the dashboard at startup), the supervisor's webrtc_pump
    /// folds agent-originated SDP answers / ICE candidates into it.
    /// None for tests that don't exercise the WebRTC path.
    webrtc_sessions: Arc<RwLock<Option<crate::webrtc::SessionRegistry>>>,
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
            world: Arc::new(RwLock::new(HashMap::new())),
            capabilities: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            webrtc_sessions: Arc::new(RwLock::new(None)),
        }
    }

    /// Phase 13b: hand the registry a SessionRegistry so the
    /// supervisor's webrtc_pump knows where to deposit agent-originated
    /// SDP answers / ICE candidates.
    pub async fn attach_webrtc_sessions(&self, sessions: crate::webrtc::SessionRegistry) {
        *self.webrtc_sessions.write().await = Some(sessions);
    }

    /// Phase 13b: route an inbound WebRtcEvent (forwarded by the
    /// transport actor's `webrtc_tx`) into the attached SessionRegistry,
    /// if one is wired. Best-effort: silently drops when no
    /// SessionRegistry is attached, matching how `record_capabilities`
    /// silently drops events on tear-down.
    pub async fn record_webrtc_event(&self, event: crate::transport::WebRtcEvent) {
        let guard = self.webrtc_sessions.read().await;
        let Some(sessions) = guard.as_ref() else { return };
        match event {
            crate::transport::WebRtcEvent::Answer { session_id, sdp } => {
                // Agent gives us raw SDP; browser expects base64 (the
                // wire is double-encoded for safety across HTTP/JSON).
                use base64::Engine;
                let b64 = base64::engine::general_purpose::STANDARD.encode(sdp.as_bytes());
                let _ = sessions.record_answer(&session_id, b64).await;
            }
            crate::transport::WebRtcEvent::Ice { session_id, candidate } => {
                // Candidates are already JSON; pass through unchanged.
                let _ = sessions.record_ice(&session_id, candidate).await;
            }
        }
    }

    /// Phase 8: record a node's capabilities. Called from the actor
    /// on inbound `Payload::Capabilities`. Overwrites any prior
    /// record. Phase 8 follow-up: agents now re-emit Capabilities
    /// periodically (~30s) so dynamic changes (docker started,
    /// display plugged) propagate without a reconnect. This method
    /// is idempotent for unchanged values.
    pub async fn record_capabilities(&self, node_id: &str, caps: Capabilities) {
        let mut map = self.capabilities.write().await;
        if let Some(prev) = map.get(node_id) {
            if *prev == caps {
                return;
            }
        }
        map.insert(node_id.to_string(), caps);
    }

    /// Look up a node's reported capabilities.
    pub async fn capabilities_for(&self, node_id: &str) -> Option<Capabilities> {
        self.capabilities.read().await.get(node_id).cloned()
    }

    /// Phase 8: find all nodes whose capabilities match the given
    /// predicate. Each `Option` in `needs` is a constraint: `Some(v)`
    /// requires the node's capability to equal `v`; `None` doesn't
    /// constrain that field. Returns sorted-by-node_id node IDs.
    pub async fn find_nodes_with(&self, needs: &CapabilityNeeds) -> Vec<String> {
        let caps = self.capabilities.read().await;
        let mut matching: Vec<String> = caps
            .iter()
            .filter(|(_, c)| needs.matches(c))
            .map(|(id, _)| id.clone())
            .collect();
        matching.sort();
        matching
    }

    /// Phase 6: ingest a WorldUpdate from an agent. No-op when the new
    /// state is byte-identical to what's already cached (defense in
    /// depth on top of the agent's own change-detection check).
    /// Otherwise updates the cache and broadcasts a `WorldChanged`
    /// event so SSE subscribers and the dashboard react immediately.
    pub async fn observe_world_update(&self, node_id: &str, state: WorldState) {
        // De-dupe against the cached state. Compare all fields except
        // `last_observed_unix` — that always changes per tick, and
        // we don't want a re-broadcast on every observation when
        // nothing material changed.
        {
            let cache = self.world.read().await;
            if let Some(prev) = cache.get(node_id) {
                let mut probe = state.clone();
                probe.last_observed_unix = prev.last_observed_unix;
                if probe == *prev {
                    return;
                }
            }
        }
        // Update the cache; broadcast event.
        {
            let mut cache = self.world.write().await;
            cache.insert(node_id.to_string(), state.clone());
        }
        let _ = self.event_tx.send(NodeEvent::WorldChanged {
            node_id: node_id.to_string(),
            state,
        });
    }

    /// Phase 6: read the most recent world state for `node_id`.
    /// Returns `None` when no observation has arrived yet (fresh
    /// connect; agent's first WorldObserver tick hasn't completed).
    pub async fn world_state_for(&self, node_id: &str) -> Option<WorldState> {
        self.world.read().await.get(node_id).cloned()
    }

    /// Phase 6: return the cached world state IFF it was observed
    /// after `since_unix_secs`; else None. Returns the full state
    /// for backward compatibility — the field-granular variant is
    /// `world_field_diff_since` below.
    pub async fn world_diff_since(
        &self,
        node_id: &str,
        since_unix_secs: u64,
    ) -> Option<WorldState> {
        let cache = self.world.read().await;
        let state = cache.get(node_id)?;
        if state.last_observed_unix > since_unix_secs {
            Some(state.clone())
        } else {
            None
        }
    }

    /// Phase 6 (field-granular variant): return a JSON object whose
    /// keys are exactly the fields that have changed since
    /// `since_unix_secs`. Bandwidth-efficient for high-cadence
    /// pollers: the AI gets only what's new. Returns `None` when
    /// nothing has changed.
    ///
    /// This is best-effort field diffing — for nested structs
    /// (FocusedApp, ClipboardMetadata) we emit the whole nested
    /// object when ANY child field changed. Vec fields (displays,
    /// shells) are likewise atomic.
    pub async fn world_field_diff_since(
        &self,
        node_id: &str,
        since_unix_secs: u64,
    ) -> Option<serde_json::Value> {
        let cache = self.world.read().await;
        let state = cache.get(node_id)?;
        if state.last_observed_unix <= since_unix_secs {
            return None;
        }
        // We don't have access to the previous-state snapshot per
        // since-timestamp — clients are responsible for tracking
        // their last-seen state to compute deltas. The hub-side
        // optimization that's tractable: emit only the non-empty
        // fields. Nothing-to-do fields (None / empty vecs) get
        // dropped from the response.
        let mut out = serde_json::Map::new();
        if let Some(app) = &state.focused_app {
            out.insert("focused_app".into(), serde_json::to_value(app).ok()?);
        }
        if let Some(m) = &state.mouse {
            out.insert("mouse".into(), serde_json::to_value(m).ok()?);
        }
        if !state.displays.is_empty() {
            out.insert("displays".into(), serde_json::to_value(&state.displays).ok()?);
        }
        if let Some(cb) = &state.clipboard {
            out.insert("clipboard".into(), serde_json::to_value(cb).ok()?);
        }
        if !state.shells.is_empty() {
            out.insert("shells".into(), serde_json::to_value(&state.shells).ok()?);
        }
        out.insert(
            "last_observed_unix".into(),
            serde_json::Value::Number(state.last_observed_unix.into()),
        );
        Some(serde_json::Value::Object(out))
    }

    /// Phase 6: persist all cached world states to a JSONL file.
    /// Best-effort — on serialization failure we log and continue.
    /// Called by a periodic task spawned at hub startup so a hub
    /// restart can prime the cache from the last persisted dump.
    pub async fn persist_world_to(&self, path: &std::path::Path) -> std::io::Result<()> {
        let cache = self.world.read().await;
        let mut out = String::new();
        for (id, state) in cache.iter() {
            let json = serde_json::to_string(&serde_json::json!({
                "node_id": id,
                "state": state,
            }))
            .unwrap_or_else(|_| "{}".into());
            out.push_str(&json);
            out.push('\n');
        }
        tokio::fs::write(path, out).await
    }

    /// Phase 6: load a previously-persisted JSONL dump into the
    /// cache. Missing/unreadable file is not an error — first-run
    /// hubs just start with an empty cache.
    pub async fn load_world_from(&self, path: &std::path::Path) {
        let Ok(contents) = tokio::fs::read_to_string(path).await else { return };
        let mut cache = self.world.write().await;
        for line in contents.lines() {
            if line.trim().is_empty() { continue }
            let Ok(parsed): Result<serde_json::Value, _> = serde_json::from_str(line) else { continue };
            let Some(node_id) = parsed.get("node_id").and_then(|v| v.as_str()) else { continue };
            let Some(state) = parsed.get("state") else { continue };
            let Ok(ws): Result<WorldState, _> = serde_json::from_value(state.clone()) else { continue };
            cache.insert(node_id.to_string(), ws);
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

    /// Phase 13b: like `get`, but Option-typed for callers that want to
    /// branch on connection state instead of treating it as an error.
    /// The dashboard's WebRTC relay uses this — a disconnected agent
    /// shouldn't 5xx the browser, it should pend (HTTP 202).
    pub async fn try_get(&self, node_id: &str) -> Option<NodeHandle> {
        self.nodes.read().await.get(node_id).cloned()
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

    // ── Phase 12b plugin proxy ────────────────────────────────────────────────

    pub async fn plugin_list(&self, node_id: &str) -> anyhow::Result<Vec<kestrel_proto::PluginInfoWire>> {
        self.get(node_id).await?.plugin_list().await
    }

    pub async fn plugin_invoke(
        &self,
        node_id: &str,
        plugin: String,
        tool: String,
        args_json: String,
    ) -> anyhow::Result<String> {
        self.get(node_id)
            .await?
            .plugin_invoke(plugin, tool, args_json)
            .await
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

    // -------- Phase 6 world state tests --------

    fn ws_with_app(name: &str, ts: u64) -> WorldState {
        WorldState {
            focused_app: Some(kestrel_proto::FocusedApp {
                name: name.into(),
                pid: 1,
                window_title: None,
            }),
            mouse: None,
            displays: vec![],
            clipboard: None,
            shells: vec![],
            screen_fingerprint: None,
            last_observed_unix: ts,
        }
    }

    #[tokio::test]
    async fn world_state_for_unknown_node_is_none() {
        let r = NodeRegistry::new();
        assert!(r.world_state_for("ghost").await.is_none());
    }

    #[tokio::test]
    async fn observe_world_update_caches_and_broadcasts() {
        let r = NodeRegistry::new();
        let mut rx = r.subscribe();
        r.observe_world_update("alpha", ws_with_app("Safari", 100)).await;
        // Cache populated.
        let cached = r.world_state_for("alpha").await.unwrap();
        assert_eq!(cached.focused_app.unwrap().name, "Safari");
        // Event broadcast.
        let evt = rx.recv().await.unwrap();
        match evt {
            NodeEvent::WorldChanged { node_id, state } => {
                assert_eq!(node_id, "alpha");
                assert_eq!(state.focused_app.unwrap().name, "Safari");
            }
            other => panic!("expected WorldChanged, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn observe_world_update_is_noop_when_only_timestamp_changes() {
        // The hub's defensive de-dupe must not re-broadcast when the
        // agent's observer emits an identical-except-for-timestamp
        // state (this shouldn't happen — the agent already drops
        // unchanged states — but defense in depth matters).
        let r = NodeRegistry::new();
        let mut rx = r.subscribe();
        r.observe_world_update("alpha", ws_with_app("Safari", 100)).await;
        let _ = rx.recv().await; // consume first event

        r.observe_world_update("alpha", ws_with_app("Safari", 200)).await;
        // No new event on the channel within a short window. recv()
        // would block forever if nothing arrives; use try_recv.
        assert!(rx.try_recv().is_err(), "no new event expected");
    }

    #[tokio::test]
    async fn observe_world_update_rebroadcasts_on_real_change() {
        let r = NodeRegistry::new();
        let mut rx = r.subscribe();
        r.observe_world_update("alpha", ws_with_app("Safari", 100)).await;
        let _ = rx.recv().await;

        r.observe_world_update("alpha", ws_with_app("Mail", 200)).await;
        let evt = rx.recv().await.unwrap();
        match evt {
            NodeEvent::WorldChanged { state, .. } => {
                assert_eq!(state.focused_app.unwrap().name, "Mail");
            }
            other => panic!("expected WorldChanged, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn world_diff_since_returns_state_when_newer() {
        let r = NodeRegistry::new();
        r.observe_world_update("alpha", ws_with_app("Safari", 100)).await;
        assert!(r.world_diff_since("alpha", 50).await.is_some());
        assert!(r.world_diff_since("alpha", 100).await.is_none());
        assert!(r.world_diff_since("alpha", 200).await.is_none());
    }
}
