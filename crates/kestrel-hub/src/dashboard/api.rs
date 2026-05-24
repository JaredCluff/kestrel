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
    /// Phase 6: agent's WorldObserver reported a state change.
    /// Carries only the node_id; clients GET /api/world/:id (or
    /// call the world_state MCP tool) for the new state. Keeps SSE
    /// payloads small and avoids re-encoding the WorldState into
    /// every SSE frame.
    WorldChanged {
        node_id: String,
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
            NodeEvent::WorldChanged { node_id, .. } => NodeEventDto::WorldChanged {
                node_id: node_id.clone(),
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

/// Internal SSE stream builder for the `/api/events` route. Not part
/// of the public API — `pub(crate)` lets `events_handler` and any
/// future internal caller use it, but downstream crates can't.
pub(crate) fn events_stream(
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

// -------- Phase 11b OIDC dashboard endpoints -------------------------------
//
// GET  /oauth/<provider>/login     → 303 redirect to provider's
//                                      authorize URL; stores PKCE + nonce
//                                      keyed by csrf state
// GET  /oauth/<provider>/callback  → exchanges the auth code + ID token,
//                                      issues a kestrel_session cookie via
//                                      Phase 11 session.rs, 303 to /

#[derive(serde::Deserialize)]
pub struct OauthCallbackQuery {
    pub code: String,
    pub state: String,
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub async fn oauth_login(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    axum::extract::Path(provider_name): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(provider) = state
        .oidc_providers
        .iter()
        .find(|p| p.name == provider_name)
        .cloned()
    else {
        return (StatusCode::NOT_FOUND, format!("unknown provider '{}'", provider_name)).into_response();
    };
    let init = match crate::oidc::begin_login(&provider).await {
        Ok(i) => i,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("oidc init: {}", e)).into_response();
        }
    };
    // Store the PKCE verifier + nonce so the callback can verify.
    {
        let mut map = state.oidc_pending.lock().await;
        map.insert(
            init.csrf_state.clone(),
            crate::dashboard::OidcPending {
                provider: provider_name.clone(),
                pkce_verifier: init.pkce_verifier,
                nonce: init.nonce,
                created_unix: now_unix_secs(),
            },
        );
        // Best-effort GC: drop entries older than 10 minutes.
        map.retain(|_, v| now_unix_secs().saturating_sub(v.created_unix) < 600);
    }
    axum::response::Redirect::to(&init.authorize_url).into_response()
}

pub async fn oauth_callback(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    axum::extract::Path(provider_name): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<OauthCallbackQuery>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(provider) = state
        .oidc_providers
        .iter()
        .find(|p| p.name == provider_name)
        .cloned()
    else {
        return (StatusCode::NOT_FOUND, format!("unknown provider '{}'", provider_name)).into_response();
    };
    // Look up + remove the pending state.
    let pending = {
        let mut map = state.oidc_pending.lock().await;
        map.remove(&q.state)
    };
    let Some(pending) = pending else {
        return (StatusCode::BAD_REQUEST, "no pending OIDC flow for this state".to_string()).into_response();
    };
    // Verify the provider on the callback matches the one stored at
    // begin_login — defense against a craftily-constructed callback
    // URL using the wrong provider's path.
    if pending.provider != provider_name {
        return (StatusCode::BAD_REQUEST, "provider mismatch".to_string()).into_response();
    }
    let user_id = match crate::oidc::complete_login(
        &provider,
        q.code,
        pending.pkce_verifier,
        pending.nonce,
    )
    .await
    {
        Ok(u) => u,
        Err(e) => {
            return (StatusCode::UNAUTHORIZED, format!("oidc login failed: {}", e)).into_response();
        }
    };
    // Optional: verify the user_id exists in policy.users before
    // minting a session. Without policy, accept any OIDC-verified
    // user (degenerate single-tenant mode).
    if let Some(policy) = state.policy.as_ref() {
        if !policy.users.iter().any(|u| u.user_id == user_id) {
            return (
                StatusCode::FORBIDDEN,
                format!("OIDC user '{}' not in kestrel-policy.toml", user_id),
            )
                .into_response();
        }
    }
    // Mint the session cookie via Phase 11 session.rs.
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

// -------- Phase 11b approval-queue endpoints -------------------------------
//
// Operators sign in to the dashboard, click "Approvals" in the
// header, see pending approval requests with metadata (who's asking,
// what op, what node, when it expires), and click approve / deny.
// The actor blocking inside policy.decide() unblocks immediately.
//
// All endpoints require auth.

pub async fn approvals_list(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    headers: axum::http::HeaderMap,
) -> Result<axum::Json<Vec<crate::policy::ApprovalDto>>, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    Ok(axum::Json(state.approvals.pending().await))
}

pub async fn approvals_approve(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    if state
        .approvals
        .resolve(&id, crate::policy::ApprovalOutcome::Approved)
        .await
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            format!("approval '{}' not pending", id),
        ))
    }
}

pub async fn approvals_deny(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    if state
        .approvals
        .resolve(&id, crate::policy::ApprovalOutcome::Denied)
        .await
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            format!("approval '{}' not pending", id),
        ))
    }
}

// -------- Phase 13 WebRTC signalling endpoints -----------------------------
//
// Browsers driving a WebRTC session against an agent POST through
// these endpoints to set up the SDP offer/answer + ICE candidate
// exchange. The actual RTP pipeline lives in the (deferred) agent-
// side capture and hub-side SFU code; these are the signalling
// primitives that establish the session.
//
// Auth: gated through check_auth — streaming is operator-only and
// any active session can transmit substantial bandwidth.

#[derive(serde::Deserialize)]
pub struct WebrtcCreateBody {
    pub node_id: String,
}

#[derive(serde::Deserialize)]
pub struct WebrtcSdpBody {
    pub sdp_b64: String,
}

#[derive(serde::Deserialize)]
pub struct WebrtcIceBody {
    pub candidate_json: String,
}

pub async fn webrtc_create_session(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    headers: axum::http::HeaderMap,
    axum::Json(body): axum::Json<WebrtcCreateBody>,
) -> Result<axum::Json<serde_json::Value>, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    let id = state.webrtc_sessions.create(body.node_id).await;
    Ok(axum::Json(serde_json::json!({ "session_id": id })))
}

pub async fn webrtc_post_offer(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
    axum::Json(body): axum::Json<WebrtcSdpBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    // Store the offer locally so /api/webrtc/session/:id GET shows it
    // even before the agent answers.
    if !state.webrtc_sessions.record_offer(&id, body.sdp_b64.clone()).await {
        return Err((StatusCode::NOT_FOUND, format!("session '{}' not found", id)));
    }
    // Phase 13b: forward to the target agent so it can negotiate.
    let Some(session) = state.webrtc_sessions.get(&id).await else {
        return Err((StatusCode::NOT_FOUND, format!("session '{}' not found", id)));
    };
    let Some(handle) = state.registry.try_get(&session.node_id).await else {
        // Agent not connected — session sits in OfferReceived. Browser
        // can poll and timeout / show the error.
        return Ok(StatusCode::ACCEPTED);
    };
    if let Err(e) = handle.send_webrtc_offer(id.clone(), body.sdp_b64).await {
        tracing::warn!("forward offer to agent {}: {}", session.node_id, e);
        return Err((StatusCode::BAD_GATEWAY, format!("forward to agent: {}", e)));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn webrtc_post_answer(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
    axum::Json(body): axum::Json<WebrtcSdpBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    if state.webrtc_sessions.record_answer(&id, body.sdp_b64).await {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, format!("session '{}' not found", id)))
    }
}

pub async fn webrtc_post_ice(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
    axum::Json(body): axum::Json<WebrtcIceBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    if !state.webrtc_sessions.record_ice(&id, body.candidate_json.clone()).await {
        return Err((StatusCode::NOT_FOUND, format!("session '{}' not found", id)));
    }
    // Phase 13b: forward browser ICE candidates to the agent too.
    let Some(session) = state.webrtc_sessions.get(&id).await else {
        return Err((StatusCode::NOT_FOUND, format!("session '{}' not found", id)));
    };
    if let Some(handle) = state.registry.try_get(&session.node_id).await {
        if let Err(e) = handle.send_webrtc_ice(id, body.candidate_json).await {
            tracing::warn!("forward ICE to agent {}: {}", session.node_id, e);
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn webrtc_get_session(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<axum::Json<crate::webrtc::Session>, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    match state.webrtc_sessions.get(&id).await {
        Some(s) => Ok(axum::Json(s)),
        None => Err((StatusCode::NOT_FOUND, format!("session '{}' not found", id))),
    }
}

/// `GET /api/world/:node_id` — returns the latest world state for
/// `node_id` as JSON. 404 when no observation has arrived yet (fresh
/// connect; WorldObserver hasn't ticked). Read-only; no auth required
/// (matches the rest of the read-only API surface — `/api/nodes`,
/// `/api/events`).
pub async fn world_handler(
    State(registry): State<Arc<NodeRegistry>>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
) -> Result<axum::Json<kestrel_proto::WorldState>, (StatusCode, String)> {
    match registry.world_state_for(&node_id).await {
        Some(state) => Ok(axum::Json(state)),
        None => Err((
            StatusCode::NOT_FOUND,
            format!("no world state cached for '{}' (observer hasn't ticked yet)", node_id),
        )),
    }
}

/// TTL for cached screenshots. After this, a fetch triggers a fresh
/// capture from the agent. Operators viewing the dashboard see at most
/// `SCREENSHOT_TTL` of staleness per node — generous enough to keep
/// the per-node MCP load low when the dashboard is open.
pub const SCREENSHOT_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// `GET /api/screenshot/:node_id` — returns the most recent PNG for
/// `node_id`. Refreshes from the agent if the cache is stale or
/// missing. Returns 404 if the node isn't connected or the agent
/// rejects the screenshot call.
///
/// Auth: gated through `check_auth` — screenshots can contain
/// passwords, sensitive emails, etc. Read-only / un-authenticated
/// viewers get 401; the browser shows a broken-image icon, which is
/// the right UX signal.
pub async fn screenshot_handler(
    axum::extract::State(state): axum::extract::State<crate::dashboard::AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(node_id): axum::extract::Path<String>,
) -> Result<axum::response::Response, (StatusCode, String)> {
    check_auth(&state, &headers)?;

    // Fast path: serve from cache when fresh.
    {
        let cache = state.screenshots.read().await;
        if let Some(entry) = cache.get(&node_id) {
            if entry.captured_at.elapsed() < SCREENSHOT_TTL {
                return Ok(png_response(entry.png.clone()));
            }
        }
    }

    // Cache miss / stale: request fresh from the agent.
    let png = state
        .registry
        .screenshot(&node_id, 0, None)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, format!("screenshot on '{}': {}", node_id, e)))?;
    let png = std::sync::Arc::new(png);

    // Write-back under the briefest possible exclusive lock. Other
    // readers waiting on read() will get the fresh entry.
    {
        let mut cache = state.screenshots.write().await;
        cache.insert(
            node_id.clone(),
            crate::dashboard::CachedScreenshot {
                png: png.clone(),
                captured_at: std::time::Instant::now(),
            },
        );
    }
    Ok(png_response(png))
}

fn png_response(png: std::sync::Arc<Vec<u8>>) -> axum::response::Response {
    use axum::response::IntoResponse;
    // Cache-Control mirrors our TTL so reasonable browsers don't
    // re-fetch faster than the server is willing to recompute.
    let headers = [
        (axum::http::header::CONTENT_TYPE, "image/png"),
        (
            axum::http::header::CACHE_CONTROL,
            "private, max-age=30",
        ),
    ];
    // Arc<Vec<u8>> → Vec<u8> via clone for axum's Body. Cost is one
    // O(N) copy per response which is fine for screenshot-sized
    // payloads.
    (headers, (*png).clone()).into_response()
}

use axum::extract::Path;
use axum::http::StatusCode;

use crate::config::{
    add_node, load_doc, save_doc, set_layout as cfg_set_layout, try_remove_layout, try_remove_node,
    NodeLayout,
};
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

/// Core add-node logic shared by `POST /api/nodes` (JSON) and the UI form
/// handler. Returns the seeded NodeStatusDto on success. Errors are
/// (status, message) tuples for direct return.
///
/// The `address` argument is the already-parsed SocketAddr — callers parse
/// from their own input shape (JSON body, form field) and surface
/// appropriate errors. Everything else (config lock, file mutation,
/// registry seed, supervisor spawn) is identical across callers.
async fn add_node_impl(
    state: &AppState,
    node_id: &str,
    address: std::net::SocketAddr,
) -> Result<NodeStatusDto, (StatusCode, String)> {
    let _lock = state.config_write_lock.lock().await;

    let mut doc = load_doc(&state.config_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    add_node(&mut doc, node_id, address)
        .map_err(|e| (StatusCode::CONFLICT, e.to_string()))?;
    save_doc(&state.config_path, &doc)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Seed registry status synchronously before spawning the supervisor so
    // an immediate follow-up `GET /api/nodes` includes the new row. Without
    // this, the supervisor's own `mark_reconnecting` call happens
    // asynchronously after this handler returns, leaving a race window where
    // the POST response says "Reconnecting" but a racing GET sees no node.
    state.registry.mark_reconnecting(node_id, 0).await;

    let handle = supervisor::spawn(
        crate::config::NodeConfig { node_id: node_id.into(), address },
        state.registry.clone(),
        state.master_secret.clone(),
    );
    state.supervisors.write().await.insert(node_id.into(), handle);

    let snap = state.registry.status_snapshot().await;
    let dto = snap
        .iter()
        .find(|s| s.node_id == node_id)
        .map(NodeStatusDto::from)
        .unwrap_or_else(|| NodeStatusDto {
            // Defensive fallback — should be unreachable now that we seeded.
            node_id: node_id.into(),
            state: NodeStateDto::Reconnecting,
            os_name: None,
            latency_ms: None,
            last_seen_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
            next_retry_in_ms: None,
        });
    Ok(dto)
}

/// POST /api/nodes — body: { node_id, address }
/// Atomically (under config_write_lock): mutates config file, spawns supervisor.
/// Requires `Authorization: Bearer <control_token>` (or a valid session
/// cookie) when auth is enabled.
pub async fn post_node(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    axum::Json(body): axum::Json<AddNodeBody>,
) -> Result<(StatusCode, axum::Json<NodeStatusDto>), (StatusCode, String)> {
    check_auth(&state, &headers)?;
    enforce_policy(&state, &headers, "node.add", &body.node_id).await?;
    let address: std::net::SocketAddr = body.address.parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid address: {}", e)))?;
    let dto = add_node_impl(&state, &body.node_id, address).await?;
    Ok((StatusCode::CREATED, axum::Json(dto)))
}

/// DELETE /api/nodes/:node_id
/// Atomically (under config_write_lock): mutates config file, aborts supervisor, forgets node.
/// Requires `Authorization: Bearer <control_token>` when auth is enabled.
/// Core delete-node logic shared by `DELETE /api/nodes/:id` and the UI
/// form handler. Errors are (status, message) tuples. The supervisor
/// abort-then-await ordering (the Pass-9 fix) is preserved here — see
/// the original handler comments for the race that prevents.
async fn delete_node_impl(
    state: &AppState,
    node_id: &str,
) -> Result<(), (StatusCode, String)> {
    let _lock = state.config_write_lock.lock().await;

    let mut doc = load_doc(&state.config_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let in_config = try_remove_node(&mut doc, node_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if in_config {
        save_doc(&state.config_path, &doc)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    let supervisor_removed = state.supervisors.write().await.remove(node_id);
    let had_live_state = supervisor_removed.is_some();
    if let Some(handle) = supervisor_removed {
        handle.abort();
        let _ = handle.await;
    }
    if had_live_state {
        state.registry.forget_node(node_id).await;
    }

    if !in_config && !had_live_state {
        return Err((
            StatusCode::NOT_FOUND,
            format!("node '{}' not found in config or live state", node_id),
        ));
    }
    Ok(())
}

pub async fn delete_node(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    Path(node_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    enforce_policy(&state, &headers, "node.delete", &node_id).await?;
    delete_node_impl(&state, &node_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// -------- /api/layout (hot-reload of the KVM grid) --------------------------

#[derive(serde::Serialize, serde::Deserialize)]
pub struct LayoutBody {
    pub node_id: String,
    pub col: i64,
    pub row: i64,
}

/// Apply a layout edit to BOTH the on-disk config and the live KVM
/// state. Order matters:
///   1. Take `config_write_lock` so no other writer can interleave.
///   2. Mutate the TOML doc + save (file is the source of truth on
///      restart).
///   3. Take `layout.write()` and apply the same edit. If save_doc fails
///      we never touch the in-memory layout, so the two views stay
///      consistent.
async fn set_layout_impl(
    state: &AppState,
    body: &LayoutBody,
) -> Result<(), (StatusCode, String)> {
    let _lock = state.config_write_lock.lock().await;
    let mut doc = load_doc(&state.config_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    cfg_set_layout(&mut doc, &body.node_id, body.col, body.row)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    save_doc(&state.config_path, &doc)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // In-memory mutation: replace the matching node_id's entry, or
    // append if none. Mirrors what config::set_layout does to the TOML.
    // NodeLayout's col/row are i32 (grid coordinates fit easily); the
    // wire/TOML side uses i64 to match TOML's native integer width.
    // Cast at the boundary.
    let mut layout = state.layout.write().await;
    layout.retain(|l| l.node_id != body.node_id);
    layout.push(NodeLayout {
        node_id: body.node_id.clone(),
        col: body.col as i32,
        row: body.row as i32,
    });
    Ok(())
}

async fn delete_layout_impl(
    state: &AppState,
    node_id: &str,
) -> Result<(), (StatusCode, String)> {
    let _lock = state.config_write_lock.lock().await;
    let mut doc = load_doc(&state.config_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Distinguish "well-formed but absent" (Ok(false) → 404) from
    // "hub.layout is malformed" (Err → 500). Mirrors the try_remove_node
    // pattern so the operator gets actionable feedback instead of a
    // misleading 404.
    let removed = try_remove_layout(&mut doc, node_id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if !removed {
        return Err((
            StatusCode::NOT_FOUND,
            format!("layout entry '{}' not found", node_id),
        ));
    }
    save_doc(&state.config_path, &doc)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut layout = state.layout.write().await;
    layout.retain(|l| l.node_id != node_id);
    Ok(())
}

/// POST /api/layout — body: { node_id, col, row }
/// Idempotent: re-setting an existing node_id moves it to the new (col,
/// row) without erroring. Returns 204 on success.
pub async fn post_layout(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    axum::Json(body): axum::Json<LayoutBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    enforce_policy(&state, &headers, "layout.set", &body.node_id).await?;
    set_layout_impl(&state, &body).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /api/layout/:node_id
/// Removes the named node from the KVM grid. 404 if not present.
pub async fn delete_layout(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    Path(node_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_auth(&state, &headers)?;
    enforce_policy(&state, &headers, "layout.delete", &node_id).await?;
    delete_layout_impl(&state, &node_id).await?;
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

/// Quick boolean variant of `check_auth` for callers that only need to
/// decide which header section / form controls to render. Returns `true`
/// when auth is disabled (so write controls are usable for a no-auth
/// install) OR when a valid bearer/cookie credential is present.
pub fn is_authenticated(state: &AppState, headers: &axum::http::HeaderMap) -> bool {
    check_auth(state, headers).is_ok()
}

/// Resolve the calling user from `Authorization: Bearer <token>`
/// against the policy. Returns the matched user_id, or `None` if
/// policy is absent (legacy single-token mode) or no user matches.
pub fn resolve_user(state: &AppState, headers: &axum::http::HeaderMap) -> Option<String> {
    let policy = state.policy.as_ref()?;
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))?;
    policy.user_for_bearer(token).map(|u| u.user_id.clone())
}

/// Phase 11b: gate a mutation by policy. When `state.policy` is
/// `None`, this is a no-op — the legacy single-token flow remains.
/// When set, the caller's bearer is resolved to a user; the user's
/// rules are evaluated for `op` against `node`; a `RequireApproval`
/// decision blocks the caller on `state.approvals.request(...)` for
/// the configured TTL.
pub async fn enforce_policy(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    op: &str,
    node: &str,
) -> Result<(), (StatusCode, String)> {
    let Some(policy) = state.policy.as_ref() else {
        return Ok(()); // legacy mode
    };
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or((StatusCode::UNAUTHORIZED, "missing bearer".into()))?;
    let user = policy
        .user_for_bearer(token)
        .ok_or((StatusCode::UNAUTHORIZED, "unknown user".into()))?;
    let (decision, rule) = policy.decide(user, op, node);
    match decision {
        crate::policy::Decision::Allow => Ok(()),
        crate::policy::Decision::Deny => Err((
            StatusCode::FORBIDDEN,
            format!("policy denies {} on {}", op, node),
        )),
        crate::policy::Decision::NeedsApproval => {
            let (approvers, ttl) = match rule.map(|r| &r.action) {
                Some(crate::policy::PolicyAction::RequireApproval { approvers, ttl_secs }) => {
                    (approvers.clone(), std::time::Duration::from_secs(*ttl_secs))
                }
                _ => (vec![], std::time::Duration::from_secs(60)),
            };
            let outcome = state
                .approvals
                .request(
                    user.user_id.clone(),
                    op.into(),
                    node.into(),
                    approvers,
                    ttl,
                )
                .await;
            match outcome {
                crate::policy::ApprovalOutcome::Approved => Ok(()),
                crate::policy::ApprovalOutcome::Denied => Err((
                    StatusCode::FORBIDDEN,
                    "approval denied".into(),
                )),
                crate::policy::ApprovalOutcome::Timeout => Err((
                    StatusCode::FORBIDDEN,
                    "approval timed out".into(),
                )),
            }
        }
    }
}

// -------- UI write handlers -------------------------------------------------
//
// Form-driven counterparts to POST /api/nodes (JSON) and DELETE
// /api/nodes/:id. The UI handlers parse application/x-www-form-urlencoded
// bodies and respond with redirects so the browser ends up at `/` after
// success — the standard POST/Redirect/GET pattern.
//
// On auth failure these handlers redirect to /login rather than returning
// a bare 401. The user is already in a browser; a redirect is the right
// UX. (Programmatic callers hitting these endpoints will see the 303 and
// can follow the Location header if they want.)

#[derive(serde::Deserialize)]
pub struct UiAddNodeForm {
    pub node_id: String,
    pub address: String,
}

pub async fn ui_add_node(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    axum::Form(form): axum::Form<UiAddNodeForm>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if !is_authenticated(&state, &headers) {
        return axum::response::Redirect::to("/login").into_response();
    }
    let address: std::net::SocketAddr = match form.address.parse() {
        Ok(a) => a,
        Err(e) => {
            // Re-render the dashboard with an inline error rather than
            // bouncing the user to a separate error page.
            let snapshot = state.registry.status_snapshot().await;
            return (
                StatusCode::BAD_REQUEST,
                crate::dashboard::templates::page_with_error(
                    &snapshot,
                    true,
                    &format!("Invalid address '{}': {}", form.address, e),
                ),
            )
                .into_response();
        }
    };
    match add_node_impl(&state, &form.node_id, address).await {
        Ok(_) => axum::response::Redirect::to("/").into_response(),
        Err((status, msg)) => {
            let snapshot = state.registry.status_snapshot().await;
            (
                status,
                crate::dashboard::templates::page_with_error(&snapshot, true, &msg),
            )
                .into_response()
        }
    }
}

pub async fn ui_delete_node(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    Path(node_id): Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if !is_authenticated(&state, &headers) {
        return axum::response::Redirect::to("/login").into_response();
    }
    match delete_node_impl(&state, &node_id).await {
        Ok(()) => axum::response::Redirect::to("/").into_response(),
        Err((status, msg)) => {
            let snapshot = state.registry.status_snapshot().await;
            (
                status,
                crate::dashboard::templates::page_with_error(&snapshot, true, &msg),
            )
                .into_response()
        }
    }
}

// -------- Layout UI handlers ------------------------------------------------
//
// Form-driven counterparts to POST /api/layout and DELETE /api/layout/:id.
// Same redirect-on-success, redirect-to-login-on-unauth, inline-error-banner
// pattern as the add/delete-node UI.

#[derive(serde::Deserialize)]
pub struct UiLayoutForm {
    pub node_id: String,
    /// `i64` here so we can serialize a -1 / 0 / 1 grid without needing
    /// per-axis signed-int parsing in the handler.
    pub col: i64,
    pub row: i64,
}

pub async fn ui_set_layout(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    axum::Form(form): axum::Form<UiLayoutForm>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if !is_authenticated(&state, &headers) {
        return axum::response::Redirect::to("/login").into_response();
    }
    let body = LayoutBody {
        node_id: form.node_id,
        col: form.col,
        row: form.row,
    };
    match set_layout_impl(&state, &body).await {
        Ok(()) => axum::response::Redirect::to("/").into_response(),
        Err((status, msg)) => {
            let snapshot = state.registry.status_snapshot().await;
            (
                status,
                crate::dashboard::templates::page_with_error(&snapshot, true, &msg),
            )
                .into_response()
        }
    }
}

// -------- Browser shell pane ------------------------------------------------
//
// Two endpoints:
//   GET /shell/:node_id          → renders the HTML shell page (xterm-free,
//                                   minimal-viable terminal: <pre> output +
//                                   <input> for commands).
//   GET /api/shell/ws/:node_id   → WebSocket upgrade; bridges browser
//                                   keystrokes to the agent's PTY and PTY
//                                   output back. Same-origin cookies carry
//                                   auth so we don't need to thread the
//                                   bearer token through the upgrade.
//
// Frame protocol: text. Browser sends keystrokes (or full lines) as
// text frames; server sends PTY output as text frames. Bytes are passed
// through unchanged — ANSI control sequences from the shell will appear
// as literal characters in the <pre>. This is intentionally primitive
// (no xterm.js dependency); users who want full ncurses-app support
// should stick with the MCP `shell_open`/`shell_write`/`shell_read`
// tools.

pub async fn shell_page(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    Path(node_id): Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if !is_authenticated(&state, &headers) {
        return axum::response::Redirect::to("/login").into_response();
    }
    crate::dashboard::templates::shell_page(&node_id).into_response()
}

pub async fn shell_ws(
    ws: axum::extract::ws::WebSocketUpgrade,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    Path(node_id): Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if !is_authenticated(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, "not authenticated").into_response();
    }
    ws.on_upgrade(move |socket| handle_shell_socket(socket, state, node_id))
}

async fn handle_shell_socket(
    socket: axum::extract::ws::WebSocket,
    state: AppState,
    node_id: String,
) {
    use axum::extract::ws::Message;
    use futures::{SinkExt, StreamExt};

    // Open a PTY on the agent. If the agent rejects, send an error
    // frame to the browser and close.
    let pty_id = match state.registry.shell_open(&node_id, None, 80, 24).await {
        Ok(id) => id,
        Err(e) => {
            let (mut tx, _rx) = socket.split();
            let _ = tx
                .send(Message::Text(format!(
                    "[hub] shell_open on '{}' failed: {}\n",
                    node_id, e
                )))
                .await;
            let _ = tx.send(Message::Close(None)).await;
            return;
        }
    };

    let (mut sender, mut receiver) = socket.split();
    let registry = state.registry.clone();
    let registry_for_writer = registry.clone();
    let node_id_writer = node_id.clone();

    // Output pump: poll the PTY buffer every 200ms and forward any
    // bytes to the browser. 200ms is a compromise between perceived
    // responsiveness and not hammering the registry.
    let node_for_pump = node_id.clone();
    let pump_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            match registry.shell_read(&node_for_pump, pty_id).await {
                Ok(buf) if !buf.is_empty() => {
                    let text = String::from_utf8_lossy(&buf).into_owned();
                    if sender.send(Message::Text(text)).await.is_err() {
                        break;
                    }
                }
                Ok(_) => continue,
                Err(_) => {
                    // PTY gone (agent dropped, shell exited).
                    let _ = sender.send(Message::Close(None)).await;
                    break;
                }
            }
        }
    });

    // Input pump: forward every browser text frame to the PTY.
    while let Some(msg) = receiver.next().await {
        let Ok(msg) = msg else { break };
        match msg {
            Message::Text(t) => {
                if registry_for_writer
                    .shell_write(&node_id_writer, pty_id, t.into_bytes())
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Message::Binary(b) => {
                if registry_for_writer
                    .shell_write(&node_id_writer, pty_id, b)
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {} // Ping/Pong are handled by axum
        }
    }

    // Browser hung up. Cancel the output pump and close the PTY on
    // the agent so it doesn't linger.
    pump_task.abort();
    let _ = registry_for_writer.shell_close(&node_id_writer, pty_id).await;
}

pub async fn ui_unset_layout(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
    Path(node_id): Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if !is_authenticated(&state, &headers) {
        return axum::response::Redirect::to("/login").into_response();
    }
    match delete_layout_impl(&state, &node_id).await {
        Ok(()) => axum::response::Redirect::to("/").into_response(),
        Err((status, msg)) => {
            let snapshot = state.registry.status_snapshot().await;
            (
                status,
                crate::dashboard::templates::page_with_error(&snapshot, true, &msg),
            )
                .into_response()
        }
    }
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
