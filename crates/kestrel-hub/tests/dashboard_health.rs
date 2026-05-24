// crates/kestrel-hub/tests/dashboard_health.rs
//
// HTTP coverage for the /healthz and /readyz endpoints introduced as
// part of the operational-hardening pass. These probes are unauthed
// (load balancers / k8s shouldn't need the control token) and cheap.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kestrel_hub::dashboard::{router, AppState};
use kestrel_hub::router::NodeRegistry;
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

#[tokio::test]
async fn healthz_returns_200_without_auth() {
    let (app, _state) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 10).await.unwrap();
    assert_eq!(&bytes[..], b"ok\n");
}

#[tokio::test]
async fn readyz_503_when_no_node_has_connected() {
    let (app, _state) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn readyz_200_after_a_node_has_marked_reconnecting() {
    // Any non-fresh registry state counts as "we have something to
    // route to" for readiness purposes — the supervisor's first
    // mark_reconnecting call is enough.
    let (app, state) = build_app();
    state.registry.mark_reconnecting("alpha", 1).await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
