// crates/kestrel-hub/src/router.rs
use std::collections::HashMap;
use std::sync::Arc;
use kestrel_proto::{Button, KeyCode, Modifiers, OsInfo, PressRelease, Rect};
use tokio::sync::RwLock;

use crate::transport::NodeHandle;

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub os: OsInfo,
}

#[derive(Clone)]
pub struct NodeRegistry {
    nodes: Arc<RwLock<HashMap<String, NodeHandle>>>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        NodeRegistry { nodes: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub async fn register(&self, handle: NodeHandle) {
        self.nodes.write().await.insert(handle.node_id.clone(), handle);
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

    pub async fn screenshot(&self, node_id: &str, display: u8, region: Option<Rect>) -> anyhow::Result<Vec<u8>> {
        self.get(node_id).await?.screenshot(display, region).await
    }

    pub async fn type_text(&self, node_id: &str, text: String) -> anyhow::Result<()> {
        self.get(node_id).await?.send_type_text(text).await
    }

    pub async fn key_combo(&self, node_id: &str, keys: Vec<KeyCode>) -> anyhow::Result<()> {
        let h = self.get(node_id).await?;
        for key in &keys {
            h.send_key_event(key.clone(), Modifiers::default(), PressRelease::Press).await?;
        }
        for key in keys.iter().rev() {
            h.send_key_event(key.clone(), Modifiers::default(), PressRelease::Release).await?;
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

    /// Fire-and-forget mouse move — used by KVM, does not block.
    pub fn fire_mouse_move(&self, node_id: &str, x: f64, y: f64) {
        let registry = self.clone();
        let node_id = node_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = registry.mouse_move(&node_id, x, y).await {
                tracing::warn!("KVM mouse_move to {} failed: {}", node_id, e);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_starts_empty() {
        let r = NodeRegistry::new();
        assert!(r.list_sync().is_empty());
    }
}
