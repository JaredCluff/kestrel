// crates/kestrel-hub/tests/dashboard_world.rs
//
// Phase 6 PR-6.5 — dashboard surface tests:
//   - GET /api/world/:id returns the cached state as JSON
//   - GET /api/world/:id returns 404 when no observation has arrived
//   - Index page renders the focused-app cell when the world cache
//     has an entry; muted "—" when it doesn't
//
// Read-only endpoint, no auth required — same surface area as
// /api/nodes and /api/events.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kestrel_hub::dashboard::{router, AppState};
use kestrel_hub::router::NodeRegistry;
use kestrel_proto::{FocusedApp, WorldState};
use tower::ServiceExt;

fn test_master() -> Vec<u8> {
    b"kestrel-test-master-32bytes-pad!".to_vec()
}

fn starter_toml(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("kestrel.toml");
    std::fs::write(
        &path,
        r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#,
    )
    .unwrap();
    path
}

fn build_app() -> (axum::Router, AppState) {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path()).to_str().unwrap().to_string();
    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, path, test_master());
    Box::leak(Box::new(dir));
    (router(state.clone()), state)
}

fn ws_with_app(name: &str, ts: u64) -> WorldState {
    WorldState {
        focused_app: Some(FocusedApp {
            name: name.into(),
            pid: 1,
            window_title: None,
        }),
        mouse: None,
        displays: vec![],
        clipboard: None,
        shells: vec![],
        screen_fingerprint: None,
        last_observed_unix: ts,
    }
}

#[tokio::test]
async fn world_api_returns_json_for_known_node() {
    let (app, state) = build_app();
    state
        .registry
        .observe_world_update("alpha", ws_with_app("Safari", 100))
        .await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/world/alpha")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed["focused_app"]["name"], "Safari");
    assert_eq!(parsed["last_observed_unix"], 100);
}

#[tokio::test]
async fn world_api_returns_404_for_unknown_node() {
    let (app, _) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/world/ghost")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn index_renders_focused_cell_when_world_observed() {
    let (app, state) = build_app();
    state.registry.mark_reconnecting("alpha", 1).await;
    state
        .registry
        .observe_world_update("alpha", ws_with_app("Safari", 100))
        .await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 65_536).await.unwrap();
    let html = String::from_utf8_lossy(&bytes);
    // Focused cell must contain the app name.
    assert!(
        html.contains(r#"<td class="focused">"#),
        "expected focused cell in HTML"
    );
    assert!(html.contains("Safari"), "missing Safari app name in:\n{}", html);
}

#[tokio::test]
async fn index_renders_em_dash_when_no_world_observation() {
    // Reconnecting node with no world state yet — the focused cell
    // should render the muted em-dash, not blow up the page.
    let (app, state) = build_app();
    state.registry.mark_reconnecting("alpha", 1).await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 65_536).await.unwrap();
    let html = String::from_utf8_lossy(&bytes);
    // Cell present, but the value is the em-dash placeholder. We use
    // the literal char rather than the HTML-entity form because maud
    // emits raw chars.
    assert!(html.contains(r#"<td class="focused">"#));
    assert!(html.contains("—"), "expected em-dash placeholder");
    // And the page didn't crash.
    assert!(html.contains("alpha"));
}
