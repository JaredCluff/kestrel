// crates/kestrel-hub/src/tui/client.rs
use anyhow::Context;
use futures::stream::StreamExt;

use crate::dashboard::api::{NodeEventDto, NodeStatusDto};

/// HTTP client for a running kestrel-hub's JSON API.
pub struct HubClient {
    base_url: String,
    http: reqwest::Client,
}

impl HubClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        HubClient {
            base_url: base_url.into(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        }
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
}
