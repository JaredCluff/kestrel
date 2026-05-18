// crates/kestrel-hub/src/dashboard/mod.rs
use std::sync::Arc;

use axum::{Router, routing::get};
use tokio::task::JoinHandle;
use tower_http::services::ServeDir;

use crate::router::NodeRegistry;

pub mod api;
pub mod sse;
pub mod templates;

/// Map of node_id → live supervisor task handle.
/// Hot-reload mutates this under the `config_write_lock` to keep file + memory in sync.
pub type SupervisorMap = Arc<tokio::sync::RwLock<std::collections::HashMap<String, JoinHandle<()>>>>;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<NodeRegistry>,
    pub config_path: String,
    pub psk: Vec<u8>,
    pub supervisors: SupervisorMap,
    /// Serializes config file read-modify-write cycles across concurrent HTTP requests.
    pub config_write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl AppState {
    pub fn new(
        registry: Arc<NodeRegistry>,
        config_path: String,
        psk: Vec<u8>,
    ) -> Self {
        AppState {
            registry,
            config_path,
            psk,
            supervisors: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            config_write_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

// Existing read-only handlers extract Arc<NodeRegistry> via FromRef — keep them working.
impl axum::extract::FromRef<AppState> for Arc<NodeRegistry> {
    fn from_ref(state: &AppState) -> Self {
        state.registry.clone()
    }
}

/// Build the dashboard's axum Router. Serves `/`, `/sse`, `/api/nodes`,
/// `/api/events`, and `/assets/*`.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/sse", get(sse_handler))
        .route("/api/nodes", get(api::nodes_json).post(api::post_node))
        .route("/api/nodes/:node_id", axum::routing::delete(api::delete_node))
        .route("/api/events", get(api::events_handler))
        .nest_service("/assets", ServeDir::new("crates/kestrel-hub/assets"))
        .with_state(state)
}

async fn index(axum::extract::State(state): axum::extract::State<AppState>) -> maud::Markup {
    let snapshot = state.registry.status_snapshot().await;
    templates::page(&snapshot)
}

async fn sse_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::response::sse::Sse<
    impl futures::stream::Stream<
        Item = Result<axum::response::sse::Event, std::convert::Infallible>,
    > + Send
    + 'static,
> {
    sse::stream(state.registry)
}
