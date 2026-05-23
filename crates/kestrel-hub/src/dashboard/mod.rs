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
use zeroize::Zeroizing;

use crate::router::NodeRegistry;

pub mod api;
pub mod session;
pub mod sse;
pub mod templates;

/// Map of node_id → live supervisor task handle.
/// Hot-reload mutates this under the `config_write_lock` to keep file + memory in sync.
pub type SupervisorMap = Arc<tokio::sync::RwLock<std::collections::HashMap<String, JoinHandle<()>>>>;

/// One cached screenshot for a node. PNG bytes captured at
/// `captured_at`. The dashboard's screenshot endpoint serves these
/// directly when fresh; on cache miss / staleness it triggers a fresh
/// capture via the registry.
#[derive(Clone)]
pub struct CachedScreenshot {
    pub png: std::sync::Arc<Vec<u8>>,
    pub captured_at: std::time::Instant,
}

/// Cache: node_id → most recent CachedScreenshot. TTL is checked by
/// the handler. Stored bytes are Arc'd so multiple concurrent reads
/// don't clone megabyte payloads.
pub type ScreenshotCache =
    Arc<tokio::sync::RwLock<std::collections::HashMap<String, CachedScreenshot>>>;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<NodeRegistry>,
    pub config_path: String,
    /// Hub-side master secret. Supervisors derive each node's per-node PSK
    /// from this + the node_id at connect time via HKDF-SHA256. The master
    /// itself never goes over the wire and never reaches agents.
    ///
    /// Wrapped in `Zeroizing` so the underlying memory is wiped when the
    /// `AppState` (or any clone of it) drops. Defense-in-depth against
    /// process-memory dumps and accidental swap-out.
    pub master_secret: Zeroizing<Vec<u8>>,
    /// Symmetric key used to sign / verify dashboard session cookies.
    /// Derived from `master_secret` via HKDF with a session-specific info
    /// string; rotating the master automatically invalidates every
    /// outstanding session cookie.
    pub session_key: Zeroizing<[u8; 32]>,
    /// Live KVM layout. The KVM event-loop task reads this on every
    /// mouse-edge crossing; the dashboard's POST/DELETE /api/layout
    /// endpoints write to it under `config_write_lock` so file and
    /// memory views stay synchronized. Hot-reload — no hub restart
    /// needed to add or move a node on the grid.
    pub layout: crate::kvm::SharedLayout,
    /// Cached screenshots per node, served via /api/screenshot/:id.
    /// Bounded staleness via the handler's TTL check; no eviction needed
    /// since the working set is bounded by the configured nodes.
    pub screenshots: ScreenshotCache,
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
        master_secret: impl Into<Zeroizing<Vec<u8>>>,
    ) -> Self {
        Self::with_layout(registry, config_path, master_secret, crate::kvm::shared_layout(Vec::new()))
    }

    /// Variant of [`new`] that lets the caller pass in a SharedLayout —
    /// used by `Command::Start` to share the same `Arc<RwLock<...>>`
    /// between the KVM task and the dashboard so layout edits via the
    /// dashboard take effect live. Tests that don't drive the KVM task
    /// use [`new`] which constructs an empty layout internally.
    ///
    /// The `master_secret` is `impl Into<Zeroizing<Vec<u8>>>` so callers
    /// can pass either a raw `Vec<u8>` (auto-wrapped) or a pre-wrapped
    /// `Zeroizing<Vec<u8>>` from `enrollment::load_master_secret`.
    pub fn with_layout(
        registry: Arc<NodeRegistry>,
        config_path: String,
        master_secret: impl Into<Zeroizing<Vec<u8>>>,
        layout: crate::kvm::SharedLayout,
    ) -> Self {
        // The Into bound covers both `Vec<u8>` (via zeroize's blanket From
        // for any T: Zeroize) and `Zeroizing<Vec<u8>>` (identity).
        let master_secret: Zeroizing<Vec<u8>> = master_secret.into();
        let session_key = Zeroizing::new(kestrel_proto::derive_session_signing_key(&master_secret));
        AppState {
            registry,
            config_path,
            master_secret,
            session_key,
            layout,
            screenshots: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
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
        .route("/ui/layout", axum::routing::post(api::ui_set_layout))
        .route("/ui/layout/:node_id/delete", axum::routing::post(api::ui_unset_layout))
        .route("/api/nodes", get(api::nodes_json).post(api::post_node))
        .route("/api/nodes/:node_id", axum::routing::delete(api::delete_node))
        .route("/api/layout", axum::routing::post(api::post_layout))
        .route("/api/layout/:node_id", axum::routing::delete(api::delete_layout))
        .route("/api/events", get(api::events_handler))
        .route("/api/screenshot/:node_id", get(api::screenshot_handler))
        .route("/shell/:node_id", get(api::shell_page))
        .route("/api/shell/ws/:node_id", get(api::shell_ws))
        .route("/assets/:name", get(asset_handler))
        .with_state(state)
}

// Assets compiled into the binary. Edit the files under crates/kestrel-hub/assets/
// and a fresh cargo build picks them up.
const ASSET_DASHBOARD_CSS: &[u8] = include_bytes!("../../assets/dashboard.css");
const ASSET_HTMX_MIN_JS: &[u8] = include_bytes!("../../assets/htmx.min.js");
const ASSET_HTMX_SSE_JS: &[u8] = include_bytes!("../../assets/htmx-sse.js");
const ASSET_SHELL_JS: &[u8] = include_bytes!("../../assets/shell.js");

async fn asset_handler(Path(name): Path<String>) -> impl IntoResponse {
    let (bytes, mime): (&'static [u8], &'static str) = match name.as_str() {
        "dashboard.css" => (ASSET_DASHBOARD_CSS, "text/css; charset=utf-8"),
        "htmx.min.js" => (ASSET_HTMX_MIN_JS, "application/javascript; charset=utf-8"),
        "htmx-sse.js" => (ASSET_HTMX_SSE_JS, "application/javascript; charset=utf-8"),
        "shell.js" => (ASSET_SHELL_JS, "application/javascript; charset=utf-8"),
        _ => return (StatusCode::NOT_FOUND, "not found").into_response(),
    };
    ([(header::CONTENT_TYPE, mime)], bytes).into_response()
}

async fn index(
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: axum::http::HeaderMap,
) -> maud::Markup {
    let snapshot = state.registry.status_snapshot().await;
    let layout = state.layout.read().await.clone();
    let authed = api::is_authenticated(&state, &headers);
    templates::page_with_layout(&snapshot, &layout, authed)
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
