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

use crate::config::{add_node, load_doc, save_doc, try_remove_node};
use crate::dashboard::AppState;
use crate::supervisor;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct AddNodeBody {
    pub node_id: String,
    pub address: String,
}

/// Constant-time bytestring equality. Returns true iff `a` and `b` have the
/// same length AND the same bytes. Runtime is `O(max(a.len(), b.len()))`
/// regardless of where (or whether) the first mismatch occurs — no
/// short-circuit, so an on-path observer can't learn the bytes of `b` via
/// timing. Mirrors the constant-time discipline `kestrel-proto::auth` uses
/// for HMAC verification.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        // Length leak is unavoidable (we have to read both) and acceptable;
        // tokens are fixed-size hex strings in practice.
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Check authentication on a mutation request. Accepts EITHER of:
///   - `Authorization: Bearer <control_token>` (the CLI flow). Constant-
///     time compared against the configured token.
///   - `Cookie: kestrel_session=<signed>` (the dashboard browser flow).
///     Verified against `state.session_key` with constant-time HMAC.
///
/// If the state has no `control_token`, auth is disabled and the request
/// is accepted regardless. This preserves the legacy/no-auth setup path
/// for installs that haven't yet run `kestrel-hub init`.
///
/// The bearer path is checked first because it's the only path CLI
/// callers can take, and we want the bearer error to be the visible one
/// when both header families are absent (the cookie path's "missing"
/// state is just "no session yet, go to /login").
fn check_auth(
    state: &AppState,
    headers: &axum::http::HeaderMap,
) -> Result<(), (StatusCode, String)> {
    let Some(expected) = state.control_token.as_deref() else {
        return Ok(()); // auth disabled
    };

    // Path 1: Authorization: Bearer <token>. Constant-time compared.
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if let Some(t) = bearer {
        return if ct_eq(t.as_bytes(), expected.as_bytes()) {
            Ok(())
        } else {
            Err((StatusCode::UNAUTHORIZED, "invalid control token".into()))
        };
    }

    // Path 2: signed session cookie.
    if let Some(cookie_header) = headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(value) = super::session::extract_cookie(cookie_header) {
            return match super::session::verify(
                &state.session_key,
                value,
                super::session::now_unix_secs(),
            ) {
                Ok(_) => Ok(()),
                Err(super::session::VerifyError::Expired) => Err((
                    StatusCode::UNAUTHORIZED,
                    "session expired; sign in again at /login".into(),
                )),
                Err(_) => Err((
                    StatusCode::UNAUTHORIZED,
                    "invalid session cookie".into(),
                )),
            };
        }
    }

    Err((
        StatusCode::UNAUTHORIZED,
        "missing Authorization: Bearer header or kestrel_session cookie".into(),
    ))
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
        state.master_secret.clone(),
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
    // Distinguish "node not present" (Ok(false)) from structural config
    // errors (Err). The latter should surface as 500, not as a misleading
    // 404 — the operator needs to know their config is broken.
    let in_config = try_remove_node(&mut doc, &node_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
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
        // Await the aborted task so it can't perform any further writes into
        // the registry maps after this point. On the multi-threaded runtime
        // `abort()` only takes effect at the task's next yield, so without
        // this await a supervisor mid-`register`/`mark_reconnecting` could
        // race past our forget_node call and leave a ghost row behind.
        // Cancelled tasks return JoinError; treat that as success.
        let _ = handle.await;
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

// -------- /login + /logout --------------------------------------------------
//
// Two-stage flow:
//   1. Browser GETs /login → renders an HTML form prompting for the control
//      token. The token is the same one the CLI uses; operators paste it
//      once and the resulting cookie covers subsequent visits.
//   2. Browser POSTs the form. On a valid token the server issues a signed
//      session cookie via `Set-Cookie` and redirects to `/`. On invalid,
//      it re-renders the form with an error.
//
// Logout is a POST (never a GET — GET is reserved for non-state-changing
// requests, and cross-site GETs are how CSRF would bypass our SameSite
// guard if logout were idempotent over GET).

#[derive(serde::Deserialize)]
pub struct LoginForm {
    pub token: String,
}

pub async fn login_form(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    // If auth is disabled, the dashboard's write actions are already open;
    // sending the user to a login form would be confusing. Redirect home.
    if state.control_token.is_none() {
        return axum::response::Redirect::to("/").into_response();
    }
    crate::dashboard::templates::login_page(None).into_response()
}

pub async fn login_submit(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::Form(form): axum::Form<LoginForm>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(expected) = state.control_token.as_deref() else {
        // Auth disabled — accept the form and send home. Don't bother
        // setting a cookie; nothing checks it.
        return axum::response::Redirect::to("/").into_response();
    };

    if !ct_eq(form.token.as_bytes(), expected.as_bytes()) {
        // Re-render the form with an error. 401 keeps automation honest;
        // a successful redirect would be misleading.
        return (
            StatusCode::UNAUTHORIZED,
            crate::dashboard::templates::login_page(Some("Invalid token.")),
        )
            .into_response();
    }

    // Token matched. Issue a signed cookie for the configured TTL.
    let (cookie, _expiry) = super::session::set_cookie_header(
        &state.session_key,
        super::session::DEFAULT_SESSION_TTL_SECS,
    );
    let mut response = axum::response::Redirect::to("/").into_response();
    if let Ok(hv) = axum::http::HeaderValue::from_str(&cookie) {
        response
            .headers_mut()
            .insert(axum::http::header::SET_COOKIE, hv);
    }
    response
}

pub async fn logout() -> axum::response::Response {
    use axum::response::IntoResponse;
    let cookie = super::session::clear_cookie_header();
    let mut response = axum::response::Redirect::to("/login").into_response();
    if let Ok(hv) = axum::http::HeaderValue::from_str(&cookie) {
        response
            .headers_mut()
            .insert(axum::http::header::SET_COOKIE, hv);
    }
    response
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
    fn ct_eq_matches_equal_and_rejects_different_inputs() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(ct_eq(b"", b""));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"abcd"));
        assert!(!ct_eq(b"abcd", b"abc"));
        // Common operationally — full-length token vs. one-byte-off.
        let a = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let b = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdee";
        assert!(!ct_eq(a.as_bytes(), b.as_bytes()));
        assert!(ct_eq(a.as_bytes(), a.as_bytes()));
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
