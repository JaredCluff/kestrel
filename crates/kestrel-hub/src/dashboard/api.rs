// crates/kestrel-hub/src/dashboard/api.rs
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use axum::{
    Json,
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
};
use futures::stream::{Stream, StreamExt};
use tokio_stream::wrappers::BroadcastStream;

use crate::events::{NodeEvent, NodeState, NodeStatus};
use crate::router::NodeRegistry;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeStatusDto {
    pub node_id: String,
    pub state: NodeStateDto,
    pub os_name: Option<String>,
    pub latency_ms: Option<u32>,
    pub last_seen_unix: u64,
    pub next_retry_in_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStateDto {
    Online,
    Offline,
    Reconnecting,
}

impl From<&NodeStatus> for NodeStatusDto {
    fn from(s: &NodeStatus) -> Self {
        NodeStatusDto {
            node_id: s.node_id.clone(),
            state: match s.state {
                NodeState::Online => NodeStateDto::Online,
                NodeState::Offline => NodeStateDto::Offline,
                NodeState::Reconnecting => NodeStateDto::Reconnecting,
            },
            os_name: s.os.as_ref().map(|o| o.name.clone()),
            latency_ms: s.latency_ms,
            last_seen_unix: s
                .last_seen
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            next_retry_in_ms: s.next_retry_in.map(|d| d.as_millis() as u64),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeEventDto {
    Connected {
        node_id: String,
        os_name: String,
    },
    Disconnected {
        node_id: String,
        attempt: u32,
        next_retry_in_ms: u64,
    },
    Reconnecting {
        node_id: String,
        attempt: u32,
    },
}

impl From<&NodeEvent> for NodeEventDto {
    fn from(e: &NodeEvent) -> Self {
        match e {
            NodeEvent::Connected { node_id, os } => NodeEventDto::Connected {
                node_id: node_id.clone(),
                os_name: os.name.clone(),
            },
            NodeEvent::Disconnected {
                node_id,
                attempt,
                next_retry_in,
            } => NodeEventDto::Disconnected {
                node_id: node_id.clone(),
                attempt: *attempt,
                next_retry_in_ms: next_retry_in.as_millis() as u64,
            },
            NodeEvent::Reconnecting { node_id, attempt } => NodeEventDto::Reconnecting {
                node_id: node_id.clone(),
                attempt: *attempt,
            },
        }
    }
}

pub async fn nodes_json(
    State(registry): State<Arc<NodeRegistry>>,
) -> Json<Vec<NodeStatusDto>> {
    let snap = registry.status_snapshot().await;
    Json(snap.iter().map(NodeStatusDto::from).collect())
}

pub fn events_stream(
    registry: Arc<NodeRegistry>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = registry.subscribe();
    let updates = BroadcastStream::new(rx).filter_map(|msg| async move {
        match msg {
            Ok(evt) => {
                let dto: NodeEventDto = (&evt).into();
                let json = serde_json::to_string(&dto).unwrap_or_else(|_| "{}".into());
                Some(Ok(Event::default().event("event").data(json)))
            }
            Err(_) => None,
        }
    });
    Sse::new(updates).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

pub async fn events_handler(
    State(registry): State<Arc<NodeRegistry>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    events_stream(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{NodeState, NodeStatus};
    use std::time::{Duration, SystemTime};

    fn sample() -> NodeStatus {
        NodeStatus {
            node_id: "a".into(),
            state: NodeState::Online,
            os: None,
            latency_ms: Some(12),
            last_seen: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            next_retry_in: None,
        }
    }

    #[test]
    fn node_status_dto_serializes_with_unix_timestamp() {
        let dto: NodeStatusDto = (&sample()).into();
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains(r#""node_id":"a""#));
        assert!(json.contains(r#""state":"online""#));
        assert!(json.contains(r#""latency_ms":12"#));
        assert!(json.contains(r#""last_seen_unix":1700000000"#));
    }

    #[test]
    fn node_event_dto_round_trips() {
        let evt = NodeEvent::Reconnecting {
            node_id: "x".into(),
            attempt: 3,
        };
        let dto: NodeEventDto = (&evt).into();
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains(r#""type":"reconnecting""#));
        assert!(json.contains(r#""attempt":3"#));
        assert!(json.contains(r#""node_id":"x""#));
    }
}
