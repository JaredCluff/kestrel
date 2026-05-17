// crates/kestrel-hub/src/dashboard/mod.rs
use std::sync::Arc;

use axum::{Router, routing::get};
use tower_http::services::ServeDir;

use crate::router::NodeRegistry;

pub mod templates;

#[derive(Clone)]
struct AppState {
    registry: Arc<NodeRegistry>,
}

/// Build the dashboard's axum Router. Serves `/`, `/sse` (placeholder until Task 7), and `/assets/*`.
pub fn router(registry: Arc<NodeRegistry>) -> Router {
    let state = AppState { registry };

    Router::new()
        .route("/", get(index))
        .route("/sse", get(sse_placeholder))
        .nest_service("/assets", ServeDir::new("crates/kestrel-hub/assets"))
        .with_state(state)
}

async fn index(axum::extract::State(state): axum::extract::State<AppState>) -> maud::Markup {
    let snapshot = state.registry.status_snapshot().await;
    templates::page(&snapshot)
}

// Placeholder — Task 7 replaces this with a real SSE stream from the broadcast channel.
async fn sse_placeholder() -> &'static str {
    ""
}
