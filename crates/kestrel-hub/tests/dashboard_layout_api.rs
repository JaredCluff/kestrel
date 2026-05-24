// crates/kestrel-hub/tests/dashboard_layout_api.rs
//
// Coverage for the hot-reload KVM layout endpoints:
//   - POST /api/layout    (set/update)
//   - DELETE /api/layout/:node_id
//
// Pins:
//   1. Live edits update the SharedLayout AND the on-disk TOML.
//   2. set-then-set replaces (idempotent move), not append-duplicate.
//   3. Delete returns 404 when the entry isn't present.
//   4. Delete on a structurally-broken `hub.layout = "foo"` returns 500,
//      not a misleading 404.
//   5. Auth is enforced (no bearer / no cookie => 401).
//   6. After a set/delete, the file is byte-modified — the running hub
//      is no longer authoritative-only, the config matches.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kestrel_hub::dashboard::{router, AppState};
use kestrel_hub::kvm;
use kestrel_hub::router::NodeRegistry;
use kestrel_test::{starter_toml, test_master};
use tower::ServiceExt;

const TOKEN: &str = "test-control-token-layout-aaaaaa";

fn build_app() -> (axum::Router, AppState, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path());
    let path_str = path.to_str().unwrap().to_string();
    let registry = Arc::new(NodeRegistry::new());
    let layout = kvm::shared_layout(Vec::new());
    let state = AppState::with_layout(registry, path_str, test_master(), layout)
        .with_control_token(TOKEN.into());
    Box::leak(Box::new(dir));
    (router(state.clone()), state, path)
}

fn bearer(req: axum::http::request::Builder) -> axum::http::request::Builder {
    req.header("authorization", format!("Bearer {}", TOKEN))
}

#[tokio::test]
async fn post_layout_updates_live_state_and_file() {
    let (app, state, config_path) = build_app();

    let body = serde_json::json!({"node_id": "alpha", "col": 1, "row": 0});
    let resp = app
        .oneshot(
            bearer(Request::builder().method("POST").uri("/api/layout"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Live SharedLayout reflects the new entry.
    let layout = state.layout.read().await;
    assert_eq!(layout.len(), 1);
    assert_eq!(layout[0].node_id, "alpha");
    assert_eq!(layout[0].col, 1);
    assert_eq!(layout[0].row, 0);

    // On-disk TOML reflects the new entry too.
    let on_disk = std::fs::read_to_string(&config_path).unwrap();
    assert!(on_disk.contains("alpha"), "config missing alpha: {}", on_disk);
}

#[tokio::test]
async fn post_layout_is_idempotent_move_not_append() {
    let (app, state, _path) = build_app();

    let first = serde_json::json!({"node_id": "alpha", "col": 1, "row": 0});
    let _ = app
        .clone()
        .oneshot(
            bearer(Request::builder().method("POST").uri("/api/layout"))
                .header("content-type", "application/json")
                .body(Body::from(first.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Move alpha to (2, 3). Re-posting with the same node_id must
    // REPLACE, not append a second alpha entry.
    let moved = serde_json::json!({"node_id": "alpha", "col": 2, "row": 3});
    let resp = app
        .oneshot(
            bearer(Request::builder().method("POST").uri("/api/layout"))
                .header("content-type", "application/json")
                .body(Body::from(moved.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let layout = state.layout.read().await;
    assert_eq!(layout.len(), 1, "duplicate entry created on re-set");
    assert_eq!(layout[0].col, 2);
    assert_eq!(layout[0].row, 3);
}

#[tokio::test]
async fn delete_layout_removes_live_state_and_file() {
    let (app, state, config_path) = build_app();
    let _ = app
        .clone()
        .oneshot(
            bearer(Request::builder().method("POST").uri("/api/layout"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"node_id": "victim", "col": 0, "row": 0}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .oneshot(
            bearer(Request::builder().method("DELETE").uri("/api/layout/victim"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let layout = state.layout.read().await;
    assert!(layout.iter().all(|l| l.node_id != "victim"));

    let on_disk = std::fs::read_to_string(&config_path).unwrap();
    assert!(!on_disk.contains("victim"));
}

#[tokio::test]
async fn delete_layout_missing_returns_404() {
    let (app, _, _) = build_app();
    let resp = app
        .oneshot(
            bearer(Request::builder().method("DELETE").uri("/api/layout/nope"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_layout_with_malformed_array_returns_500() {
    // Hand-write a config where hub.layout is a string rather than an
    // array. The structural-error branch in try_remove_layout must
    // surface as 500, not a misleading 404. Pairs with the same
    // discipline on try_remove_node.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kestrel.toml");
    std::fs::write(
        &path,
        r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
layout           = "not-an-array"
"#,
    )
    .unwrap();
    let registry = Arc::new(NodeRegistry::new());
    let layout = kvm::shared_layout(Vec::new());
    let state = AppState::with_layout(
        registry,
        path.to_str().unwrap().to_string(),
        test_master(),
        layout,
    )
    .with_control_token(TOKEN.into());
    let app = router(state);

    let resp = app
        .oneshot(
            bearer(Request::builder().method("DELETE").uri("/api/layout/alpha"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn post_layout_without_auth_returns_401() {
    let (app, _, _) = build_app();
    let body = serde_json::json!({"node_id": "alpha", "col": 0, "row": 0});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/layout")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn delete_layout_without_auth_returns_401() {
    let (app, _, _) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/layout/alpha")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// -------- Browser form-driven layout UI -----------------------------------

#[tokio::test]
async fn ui_set_layout_with_bearer_redirects_home_and_updates_live_state() {
    // The /ui/layout form handler accepts either bearer or cookie
    // auth — using bearer here keeps the test simple.
    let (app, state, _path) = build_app();
    let body = "node_id=alpha&col=2&row=0";
    let resp = app
        .oneshot(
            bearer(Request::builder().method("POST").uri("/ui/layout"))
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        resp.status(),
        StatusCode::SEE_OTHER | StatusCode::FOUND
    ));
    assert_eq!(
        resp.headers().get(axum::http::header::LOCATION).unwrap().to_str().unwrap(),
        "/"
    );
    let layout = state.layout.read().await;
    assert_eq!(layout.len(), 1);
    assert_eq!(layout[0].node_id, "alpha");
    assert_eq!(layout[0].col, 2);
    assert_eq!(layout[0].row, 0);
}

#[tokio::test]
async fn ui_unset_layout_with_bearer_redirects_home_and_removes_entry() {
    let (app, state, _path) = build_app();
    // Seed an entry first.
    let _ = app
        .clone()
        .oneshot(
            bearer(Request::builder().method("POST").uri("/api/layout"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"node_id": "alpha", "col": 1, "row": 0}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let resp = app
        .oneshot(
            bearer(Request::builder()
                .method("POST")
                .uri("/ui/layout/alpha/delete"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        resp.status(),
        StatusCode::SEE_OTHER | StatusCode::FOUND
    ));
    let layout = state.layout.read().await;
    assert!(layout.is_empty(), "layout should be empty after UI unset");
}

#[tokio::test]
async fn ui_set_layout_without_auth_redirects_to_login() {
    let (app, _, _) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ui/layout")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("node_id=x&col=0&row=0"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        resp.status(),
        StatusCode::SEE_OTHER | StatusCode::FOUND
    ));
    assert_eq!(
        resp.headers().get(axum::http::header::LOCATION).unwrap().to_str().unwrap(),
        "/login"
    );
}

#[tokio::test]
async fn index_renders_layout_section_when_entries_exist() {
    // After setting a layout entry, the dashboard's HTML root should
    // include a Layout section showing it. This pins that the index
    // handler threads SharedLayout into the template.
    let (app, _, _) = build_app();
    let _ = app
        .clone()
        .oneshot(
            bearer(Request::builder().method("POST").uri("/api/layout"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"node_id": "renderme", "col": 7, "row": 11}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
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
    let bytes = axum::body::to_bytes(resp.into_body(), 32_768).await.unwrap();
    let html = String::from_utf8_lossy(&bytes);
    assert!(html.contains("Layout"), "missing Layout subhead in:\n{}", html);
    assert!(html.contains("renderme"), "missing layout entry node_id");
    assert!(html.contains("(7, 11)"), "missing layout coords");
}
