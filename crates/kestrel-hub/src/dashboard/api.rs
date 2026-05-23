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

/// Check the `Authorization: Bearer <token>` header against the configured
/// control token. If the state has no `control_token`, auth is disabled.
fn check_auth(
    state: &AppState,
    headers: &axum::http::HeaderMap,
) -> Result<(), (StatusCode, String)> {
    let Some(expected) = state.control_token.as_deref() else {
        return Ok(()); // auth disabled
    };
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    match provided {
        Some(t) if t == expected => Ok(()),
        Some(_) => Err((StatusCode::UNAUTHORIZED, "invalid control token".into())),
        None => Err((StatusCode::UNAUTHORIZED, "missing Authorization: Bearer header".into())),
    }
}

/// POST /api/nodes — body: { node_id, address }
/// Atomically (under config_write_lock): mutates config file, spawns supervisor.
/// Requires `Authorization: Bearer <control_token>` when auth is enabled.
pub async fn post_node(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    axum::Json(body): axum::Json<AddNodeBody>,
) -> Result<(StatusCode, axum::Json<NodeStatusDto>), (StatusCode, String)> {
    check_auth(&state, &headers)?;
    let address: std::net::SocketAddr = body.address.parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid address: {}", e)))?;

    let _lock = state.config_write_lock.lock().await;

    let mut doc = load_doc(&state.config_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    add_node(&mut doc, &body.node_id, address)
        .map_err(|e| (StatusCode::CONFLICT, e.to_string()))?;
    save_doc(&state.config_path, &doc)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Seed registry status synchronously before spawning the supervisor so
    // an immediate follow-up `GET /api/nodes` includes the new row. Without
    // this, the supervisor's own `mark_reconnecting` call happens
    // asynchronously after this handler returns, leaving a race window where
    // the POST response says "Reconnecting" but a racing GET sees no node.
    state.registry.mark_reconnecting(&body.node_id, 0).await;

    let handle = supervisor::spawn(
        crate::config::NodeConfig { node_id: body.node_id.clone(), address },
        state.registry.clone(),
        state.psk.clone(),
    );
    state.supervisors.write().await.insert(body.node_id.clone(), handle);

    // Now that the registry has been seeded, return its actual view (not a
    // fabricated DTO) so the client sees the same state a follow-up
    // `GET /api/nodes` would return for this row.
    let snap = state.registry.status_snapshot().await;
    let dto = snap
        .iter()
        .find(|s| s.node_id == body.node_id)
        .map(NodeStatusDto::from)
        .unwrap_or_else(|| NodeStatusDto {
            // Defensive fallback — should be unreachable now that we seeded.
            node_id: body.node_id.clone(),
            state: NodeStateDto::Reconnecting,
            os_name: None,
            latency_ms: None,
            last_seen_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
            next_retry_in_ms: None,
        });
    Ok((StatusCode::CREATED, axum::Json(dto)))
}

/// DELETE /api/nodes/:node_id
/// Atomically (under config_write_lock): mutates config file, aborts supervisor, forgets node.
/// Requires `Authorization: Bearer <control_token>` when auth is enabled.
pub async fn delete_node(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    Path(node_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    let _lock = state.config_write_lock.lock().await;

    let mut doc = load_doc(&state.config_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let in_config = remove_node(&mut doc, &node_id).is_ok();
    if in_config {
        save_doc(&state.config_path, &doc)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    // Tear down live state (supervisor task + registry entry) regardless of
    // whether the config-file row was present. A node that was added live
    // but later edited out of the config externally would otherwise leak a
    // running supervisor. We need at least ONE side to have evidence of the
    // node for this to be a 204; if neither has it, the request was bogus.
    let supervisor_removed = state.supervisors.write().await.remove(&node_id);
    let had_live_state = supervisor_removed.is_some();
    if let Some(handle) = supervisor_removed {
        handle.abort();
    }
    if had_live_state {
        state.registry.forget_node(&node_id).await;
    }

    if !in_config && !had_live_state {
        return Err((
            StatusCode::NOT_FOUND,
            format!("node '{}' not found in config or live state", node_id),
        ));
    }

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
