// crates/kestrel-hub/src/client.rs
use std::time::Duration;

use anyhow::Context;
use futures::stream::StreamExt;

use crate::dashboard::api::{AddNodeBody, LayoutBody, NodeEventDto, NodeStatusDto};

/// HTTP client for a running kestrel-hub's JSON API. Used by both the TUI
/// (read-only: fetch_nodes + subscribe_events) and the CLI (mutating:
/// add_node + remove_node).
#[derive(Clone)]
pub struct HubClient {
    base_url: String,
    http: reqwest::Client,
    /// Bearer token sent on mutation endpoints. Read-only endpoints don't require it.
    token: Option<String>,
}

impl HubClient {
    /// General-purpose client with a 10s timeout — fine for the TUI which runs
    /// against an already-known-reachable hub.
    pub fn new(base_url: impl Into<String>) -> Self {
        HubClient {
            base_url: base_url.into(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
            token: None,
        }
    }

    /// Quick-fail client for the CLI's HTTP-first fallback path.
    /// Returns connection errors within ~1s so the CLI can fall back to file mutation.
    pub fn with_quick_timeout(base_url: impl Into<String>) -> Self {
        HubClient {
            base_url: base_url.into(),
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_millis(1000))
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            token: None,
        }
    }

    /// Builder-style: attach a Bearer token used on POST/DELETE mutation calls.
    pub fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }

    pub async fn fetch_nodes(&self) -> anyhow::Result<Vec<NodeStatusDto>> {
        let url = format!("{}/api/nodes", self.base_url);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {}", url))?;
        let nodes: Vec<NodeStatusDto> = resp
            .json()
            .await
            .with_context(|| format!("decode JSON from {}", url))?;
        Ok(nodes)
    }

    /// Subscribe to /api/events. Returns a stream of parsed `NodeEventDto`s.
    /// Connection errors during the stream surface as `Err` items and the stream ends.
    pub fn subscribe_events(
        &self,
    ) -> impl futures::stream::Stream<Item = anyhow::Result<NodeEventDto>> {
        let url = format!("{}/api/events", self.base_url);
        let client = eventsource_client::ClientBuilder::for_url(&url)
            .expect("valid URL")
            .build();
        eventsource_client::Client::stream(&client).filter_map(|item| async move {
            match item {
                Ok(eventsource_client::SSE::Event(evt)) if evt.event_type == "event" => Some(
                    serde_json::from_str::<NodeEventDto>(&evt.data).map_err(|e| {
                        anyhow::anyhow!("JSON decode failed: {} (body: {})", e, evt.data)
                    }),
                ),
                Ok(_) => None, // comments, other event types, connect frames
                Err(e) => Some(Err(anyhow::anyhow!("SSE error: {:?}", e))),
            }
        })
    }

    /// POST /api/nodes — returns the created node's initial status.
    pub async fn add_node(&self, node_id: &str, address: &str) -> anyhow::Result<NodeStatusDto> {
        let url = format!("{}/api/nodes", self.base_url);
        let mut req = self.http.post(&url).json(&AddNodeBody {
            node_id: node_id.into(),
            address: address.into(),
        });
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await.with_context(|| format!("POST {}", url))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "hub returned {} {}: {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or(""),
                body
            );
        }
        let dto: NodeStatusDto = resp
            .json()
            .await
            .with_context(|| format!("decode JSON from {}", url))?;
        Ok(dto)
    }

    /// DELETE /api/nodes/{node_id}.
    pub async fn remove_node(&self, node_id: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/nodes/{}", self.base_url, node_id);
        let mut req = self.http.delete(&url);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await.with_context(|| format!("DELETE {}", url))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "hub returned {} {}: {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or(""),
                body
            );
        }
        Ok(())
    }

    /// POST /api/layout — set or update a node's KVM grid position.
    /// Idempotent: re-posting the same node_id with new (col, row) moves it.
    pub async fn set_layout(&self, node_id: &str, col: i64, row: i64) -> anyhow::Result<()> {
        let url = format!("{}/api/layout", self.base_url);
        let mut req = self.http.post(&url).json(&LayoutBody {
            node_id: node_id.into(),
            col,
            row,
        });
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await.with_context(|| format!("POST {}", url))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "hub returned {} {}: {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or(""),
                body
            );
        }
        Ok(())
    }

    /// DELETE /api/layout/{node_id} — remove a node from the KVM grid.
    pub async fn remove_layout(&self, node_id: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/layout/{}", self.base_url, node_id);
        let mut req = self.http.delete(&url);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await.with_context(|| format!("DELETE {}", url))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "hub returned {} {}: {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or(""),
                body
            );
        }
        Ok(())
    }
}
