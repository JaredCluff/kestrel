// crates/kestrel-hub/src/dashboard/sse.rs
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use crate::dashboard::templates::nodes_rows_with_controls_and_world;
use crate::router::NodeRegistry;

/// Build an SSE stream that emits a `<tbody>` fragment on every
/// node-status change. The first event is sent immediately with the
/// current snapshot so a fresh page paints quickly.
///
/// `authed` is captured at SSE-connection-open time. Renders include the
/// per-row Remove buttons iff the browser was authenticated when it
/// opened the stream. Auth changes mid-stream (e.g. session expiry)
/// don't update the row-level controls until the page is reloaded —
/// acceptable since deletes are gated server-side anyway and an
/// unauthenticated click would 303 back through /login.
pub fn stream(
    registry: Arc<NodeRegistry>,
    authed: bool,
) -> Sse<impl Stream<Item = Result<Event, Infallible>> + Send + 'static> {
    let initial = registry.clone();
    let rx_registry = registry.clone();

    let initial_snapshot = async move {
        let snap = initial.status_snapshot().await;
        let worlds = collect_worlds(&initial, &snap).await;
        Ok(Event::default()
            .event("nodes")
            .data(
                nodes_rows_with_controls_and_world(&snap, &worlds, authed)
                    .into_string(),
            ))
    };
    let initial_stream = futures::stream::once(initial_snapshot);

    // On every event (including Lagged), re-render the full snapshot.
    // World-state changes also fire events (NodeEvent::WorldChanged
    // via PR #49) so the focused cell stays fresh.
    let rx = registry.subscribe();
    let updates = BroadcastStream::new(rx).then(move |_msg| {
        let registry = rx_registry.clone();
        async move {
            let snap = registry.status_snapshot().await;
            let worlds = collect_worlds(&registry, &snap).await;
            Ok(Event::default()
                .event("nodes")
                .data(
                    nodes_rows_with_controls_and_world(&snap, &worlds, authed)
                        .into_string(),
                ))
        }
    });

    let combined = initial_stream.chain(updates);

    Sse::new(combined).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

/// Build the world-state map for every visible node in one pass.
/// Lifts the same logic the index handler uses into a helper so SSE
/// renders see fresh focused-app info on every broadcast.
async fn collect_worlds(
    registry: &Arc<NodeRegistry>,
    snap: &[crate::events::NodeStatus],
) -> std::collections::HashMap<String, kestrel_proto::WorldState> {
    let mut worlds = std::collections::HashMap::new();
    for s in snap {
        if let Some(w) = registry.world_state_for(&s.node_id).await {
            worlds.insert(s.node_id.clone(), w);
        }
    }
    worlds
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::templates::nodes_rows_with_controls;

    #[tokio::test]
    async fn render_path_produces_tbody_fragment() {
        let registry = Arc::new(NodeRegistry::new());
        registry.mark_reconnecting("a", 1).await;

        let snap = registry.status_snapshot().await;
        let body = nodes_rows_with_controls(&snap, false).into_string();

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
        let _sse: Sse<_> = stream(registry, false);
    }
}
