// crates/kestrel-hub/tests/phase7_hot_reload.rs
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kestrel_hub::dashboard::{router, AppState};
use kestrel_hub::router::NodeRegistry;
use tower::ServiceExt;

fn test_psk() -> Vec<u8> {
    b"kestrel-test-psk-32bytes-padded!".to_vec()
}

fn starter_toml(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("kestrel.toml");
    let contents = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#;
    std::fs::write(&path, contents).unwrap();
    path
}

#[tokio::test]
async fn post_node_then_delete_node_round_trip_updates_config_and_supervisors() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = starter_toml(dir.path());
    let config_path_str = config_path.to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry.clone(), config_path_str.clone(), test_psk());
    let app = router(state.clone());

    // POST /api/nodes
    let body = serde_json::json!({
        "node_id": "alpha",
        "address": "127.0.0.1:65535"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Supervisor map should now contain "alpha".
    assert!(state.supervisors.read().await.contains_key("alpha"));

    // Config file should now contain the node.
    let written = std::fs::read_to_string(&config_path_str).unwrap();
    assert!(written.contains("alpha"), "config should contain node 'alpha':\n{}", written);
    assert!(written.contains("127.0.0.1:65535"));

    // DELETE /api/nodes/alpha
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/nodes/alpha")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Supervisor map should no longer contain "alpha".
    assert!(!state.supervisors.read().await.contains_key("alpha"));

    // Config file should no longer contain the node.
    let written = std::fs::read_to_string(&config_path_str).unwrap();
    assert!(!written.contains("alpha"), "config should NOT contain 'alpha' after delete:\n{}", written);
}

#[tokio::test]
async fn post_node_rejects_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = starter_toml(dir.path());
    let config_path_str = config_path.to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, config_path_str, test_psk());
    let app = router(state);

    let body = serde_json::json!({"node_id": "x", "address": "127.0.0.1:65501"});
    let first_req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let first_resp = app.clone().oneshot(first_req).await.unwrap();
    assert_eq!(first_resp.status(), StatusCode::CREATED);

    let dup_req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let dup_resp = app.oneshot(dup_req).await.unwrap();
    assert_eq!(dup_resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn delete_node_404_when_missing() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = starter_toml(dir.path());
    let config_path_str = config_path.to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, config_path_str, test_psk());
    let app = router(state);

    let req = Request::builder()
        .method("DELETE")
        .uri("/api/nodes/ghost")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
