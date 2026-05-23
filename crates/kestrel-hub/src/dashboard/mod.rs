// crates/kestrel-hub/src/dashboard/mod.rs
use std::sync::Arc;

use axum::{
    Router,
    extract::Path,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use tokio::task::JoinHandle;

use crate::router::NodeRegistry;

pub mod api;
pub mod session;
pub mod sse;
pub mod templates;

/// Map of node_id → live supervisor task handle.
/// Hot-reload mutates this under the `config_write_lock` to keep file + memory in sync.
pub type SupervisorMap = Arc<tokio::sync::RwLock<std::collections::HashMap<String, JoinHandle<()>>>>;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<NodeRegistry>,
    pub config_path: String,
    /// Hub-side master secret. Supervisors derive each node's per-node PSK
    /// from this + the node_id at connect time via HKDF-SHA256. The master
    /// itself never goes over the wire and never reaches agents.
    pub master_secret: Vec<u8>,
    /// Symmetric key used to sign / verify dashboard session cookies.
    /// Derived from `master_secret` via HKDF with a session-specific info
    /// string; rotating the master automatically invalidates every
    /// outstanding session cookie.
    pub session_key: [u8; 32],
    pub supervisors: SupervisorMap,
    /// Serializes config file read-modify-write cycles across concurrent HTTP requests.
    pub config_write_lock: Arc<tokio::sync::Mutex<()>>,
    /// Bearer token required on mutation endpoints (POST/DELETE /api/nodes,
    /// and the form action target of /login). `None` means auth is
    /// disabled — the read-only endpoints stay open either way.
    pub control_token: Option<String>,
}

impl AppState {
    pub fn new(
        registry: Arc<NodeRegistry>,
        config_path: String,
        master_secret: Vec<u8>,
    ) -> Self {
        let session_key = kestrel_proto::derive_session_signing_key(&master_secret);
        AppState {
            registry,
            config_path,
            master_secret,
            session_key,
            supervisors: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            config_write_lock: Arc::new(tokio::sync::Mutex::new(())),
            control_token: None,
        }
    }

    /// Builder-style: require a Bearer token on mutation endpoints.
    pub fn with_control_token(mut self, token: String) -> Self {
        self.control_token = Some(token);
        self
    }
}

// Existing read-only handlers extract Arc<NodeRegistry> via FromRef — keep them working.
impl axum::extract::FromRef<AppState> for Arc<NodeRegistry> {
    fn from_ref(state: &AppState) -> Self {
        state.registry.clone()
    }
}

/// Build the dashboard's axum Router. Serves `/`, `/sse`, `/api/nodes`,
/// `/api/events`, and `/assets/*` (assets compiled into the binary so the hub
/// runs from any directory after `cargo install`).
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/sse", get(sse_handler))
        .route("/login", get(api::login_form).post(api::login_submit))
        .route("/logout", axum::routing::post(api::logout))
        .route("/ui/nodes", axum::routing::post(api::ui_add_node))
        .route("/ui/nodes/:node_id/delete", axum::routing::post(api::ui_delete_node))
        .route("/api/nodes", get(api::nodes_json).post(api::post_node))
        .route("/api/nodes/:node_id", axum::routing::delete(api::delete_node))
        .route("/api/events", get(api::events_handler))
        .route("/assets/:name", get(asset_handler))
        .with_state(state)
}

// Assets compiled into the binary. Edit the files under crates/kestrel-hub/assets/
// and a fresh cargo build picks them up.
const ASSET_DASHBOARD_CSS: &[u8] = include_bytes!("../../assets/dashboard.css");
const ASSET_HTMX_MIN_JS: &[u8] = include_bytes!("../../assets/htmx.min.js");
const ASSET_HTMX_SSE_JS: &[u8] = include_bytes!("../../assets/htmx-sse.js");

async fn asset_handler(Path(name): Path<String>) -> impl IntoResponse {
    let (bytes, mime): (&'static [u8], &'static str) = match name.as_str() {
        "dashboard.css" => (ASSET_DASHBOARD_CSS, "text/css; charset=utf-8"),
        "htmx.min.js" => (ASSET_HTMX_MIN_JS, "application/javascript; charset=utf-8"),
        "htmx-sse.js" => (ASSET_HTMX_SSE_JS, "application/javascript; charset=utf-8"),
        _ => return (StatusCode::NOT_FOUND, "not found").into_response(),
    };
    ([(header::CONTENT_TYPE, mime)], bytes).into_response()
}

async fn index(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
) -> maud::Markup {
    let snapshot = state.registry.status_snapshot().await;
    let authed = api::is_authenticated(&state, &headers);
    templates::page(&snapshot, authed)
}

async fn sse_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
) -> axum::response::sse::Sse<
    impl futures::stream::Stream<
        Item = Result<axum::response::sse::Event, std::convert::Infallible>,
    > + Send
    + 'static,
> {
    let authed = api::is_authenticated(&state, &headers);
    sse::stream(state.registry, authed)
}
