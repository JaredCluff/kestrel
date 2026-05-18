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

use axum::extract::Path;
use axum::http::StatusCode;

use crate::config::{add_node, load_doc, remove_node, save_doc};
use crate::dashboard::AppState;
use crate::supervisor;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct AddNodeBody {
    pub node_id: String,
    pub address: String,
}

/// POST /api/nodes — body: { node_id, address }
/// Atomically (under config_write_lock): mutates config file, spawns supervisor.
pub async fn post_node(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::Json(body): axum::Json<AddNodeBody>,
) -> Result<(StatusCode, axum::Json<NodeStatusDto>), (StatusCode, String)> {
    let address: std::net::SocketAddr = body.address.parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid address: {}", e)))?;

    let _lock = state.config_write_lock.lock().await;

    let mut doc = load_doc(&state.config_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    add_node(&mut doc, &body.node_id, address)
        .map_err(|e| (StatusCode::CONFLICT, e.to_string()))?;
    save_doc(&state.config_path, &doc)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let handle = supervisor::spawn(
        crate::config::NodeConfig { node_id: body.node_id.clone(), address },
        state.registry.clone(),
        state.psk.clone(),
    );
    state.supervisors.write().await.insert(body.node_id.clone(), handle);

    let snap_status = NodeStatusDto {
        node_id: body.node_id.clone(),
        state: NodeStateDto::Reconnecting,
        os_name: None,
        latency_ms: None,
        last_seen_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
        next_retry_in_ms: None,
    };
    Ok((StatusCode::CREATED, axum::Json(snap_status)))
}

/// DELETE /api/nodes/:node_id
/// Atomically (under config_write_lock): mutates config file, aborts supervisor, forgets node.
pub async fn delete_node(
    axum::extract::State(state): axum::extract::State<AppState>,
    Path(node_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let _lock = state.config_write_lock.lock().await;

    let mut doc = load_doc(&state.config_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    remove_node(&mut doc, &node_id)
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    save_doc(&state.config_path, &doc)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if let Some(handle) = state.supervisors.write().await.remove(&node_id) {
        handle.abort();
    }
    state.registry.forget_node(&node_id).await;

    Ok(StatusCode::NO_CONTENT)
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
