// crates/kestrel-hub/src/dashboard/sse.rs
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use crate::dashboard::templates::nodes_rows;
use crate::router::NodeRegistry;

/// Build an SSE stream that emits a `<tbody>` fragment on every node-status change.
/// The first event is sent immediately with the current snapshot so a fresh page paints quickly.
pub fn stream(
    registry: Arc<NodeRegistry>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>> + Send + 'static> {
    let initial = registry.clone();
    let rx_registry = registry.clone();

    let initial_snapshot = async move {
        let snap = initial.status_snapshot().await;
        Ok(Event::default()
            .event("nodes")
            .data(nodes_rows(&snap).into_string()))
    };
    let initial_stream = futures::stream::once(initial_snapshot);

    // On every event (including Lagged), re-render the full snapshot. Each
    // broadcast item produces one render — there is no per-tick coalescing
    // here. We accept the work cost because each render reflects the latest
    // state at the moment it runs, so a burst of N events emits N
    // identical-looking fragments and the client converges quickly. If this
    // ever becomes a perf bottleneck, wrap the stream in `.ready_chunks(N)`
    // or `.throttle(...)` to debounce.
    let rx = registry.subscribe();
    let updates = BroadcastStream::new(rx).then(move |_msg| {
        let registry = rx_registry.clone();
        async move {
            let snap = registry.status_snapshot().await;
            Ok(Event::default()
                .event("nodes")
                .data(nodes_rows(&snap).into_string()))
        }
    });

    let combined = initial_stream.chain(updates);

    Sse::new(combined).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn render_path_produces_tbody_fragment() {
        let registry = Arc::new(NodeRegistry::new());
        registry.mark_reconnecting("a", 1).await;

        let snap = registry.status_snapshot().await;
        let body = nodes_rows(&snap).into_string();

        assert!(
            body.contains("reconnecting"),
            "expected reconnecting state, got: {}",
            body
        );
        assert!(
            body.contains(">a<"),
            "expected node id 'a' in fragment, got: {}",
            body
        );
    }

    #[tokio::test]
    async fn stream_builds_without_panic() {
        // Smoke test that constructing the SSE stream does not panic, and that the
        // returned Sse type compiles into a valid axum response type.
        let registry = Arc::new(NodeRegistry::new());
        let _sse: Sse<_> = stream(registry);
    }
}
