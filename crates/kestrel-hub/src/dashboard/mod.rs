// crates/kestrel-hub/src/dashboard/mod.rs
use std::sync::Arc;

use axum::{Router, routing::get};
use tower_http::services::ServeDir;

use crate::router::NodeRegistry;

pub mod api;
pub mod sse;
pub mod templates;

#[derive(Clone)]
struct AppState {
    registry: Arc<NodeRegistry>,
}

impl axum::extract::FromRef<AppState> for Arc<NodeRegistry> {
    fn from_ref(state: &AppState) -> Self {
        state.registry.clone()
    }
}

/// Build the dashboard's axum Router. Serves `/`, `/sse`, `/api/nodes`,
/// `/api/events`, and `/assets/*`.
pub fn router(registry: Arc<NodeRegistry>) -> Router {
    let state = AppState { registry };

    Router::new()
        .route("/", get(index))
        .route("/sse", get(sse_handler))
        .route("/api/nodes", get(api::nodes_json))
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
