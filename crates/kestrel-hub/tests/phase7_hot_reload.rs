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

// ── Pass 6 coverage additions ──────────────────────────────────────────────

#[tokio::test]
async fn post_node_rejects_invalid_address() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = starter_toml(dir.path());
    let config_path_str = config_path.to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, config_path_str, test_psk());
    let app = router(state);

    let body = serde_json::json!({"node_id": "x", "address": "not-an-address"});
    let req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_node_rejects_malformed_json_body() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = starter_toml(dir.path());
    let config_path_str = config_path.to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, config_path_str, test_psk());
    let app = router(state);

    // Body missing the required `address` field.
    let body = serde_json::json!({"node_id": "x"});
    let req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    // axum's Json extractor returns 422 Unprocessable Entity on a body that
    // parses as JSON but doesn't match the target type. (Some axum versions
    // return 400; accept either.)
    let status = resp.status();
    assert!(
        status == StatusCode::UNPROCESSABLE_ENTITY || status == StatusCode::BAD_REQUEST,
        "expected 400 or 422, got {}",
        status
    );
}

#[tokio::test]
async fn delete_node_cleans_live_state_even_when_not_in_config() {
    // Adversarial case: the supervisor was spawned via POST, but someone
    // externally edited the config to remove the node. A subsequent DELETE
    // must still abort the live supervisor (and forget the registry entry)
    // — otherwise a "removed" node leaks a reconnect loop forever.
    let dir = tempfile::tempdir().unwrap();
    let config_path = starter_toml(dir.path());
    let config_path_str = config_path.to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry.clone(), config_path_str.clone(), test_psk());
    let app = router(state.clone());

    // 1. POST adds 'orphan' to config + spawns supervisor.
    let body = serde_json::json!({"node_id": "orphan", "address": "127.0.0.1:65532"});
    let post = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let post_resp = app.clone().oneshot(post).await.unwrap();
    assert_eq!(post_resp.status(), StatusCode::CREATED);
    assert!(state.supervisors.read().await.contains_key("orphan"));

    // 2. Externally edit the config to remove the node (simulates someone
    //    editing kestrel.toml by hand).
    let stripped = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#;
    std::fs::write(&config_path_str, stripped).unwrap();

    // 3. DELETE — the config no longer has the node, but the live supervisor
    //    does. Pass 6 fix: handler still aborts the live state and returns
    //    204, rather than returning 404 and leaking.
    let del = Request::builder()
        .method("DELETE")
        .uri("/api/nodes/orphan")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(del).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(!state.supervisors.read().await.contains_key("orphan"));
    // The DELETE handler must also have called forget_node on the registry
    // — otherwise a dashboard view would keep showing the orphan as
    // online/reconnecting forever. Pin that explicitly so a regression that
    // dropped only the registry-side cleanup wouldn't pass.
    let snap = registry.status_snapshot().await;
    assert!(
        snap.iter().all(|s| s.node_id != "orphan"),
        "registry should have forgotten 'orphan' after DELETE; got: {:?}",
        snap.iter().map(|s| &s.node_id).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn post_node_seeds_registry_before_returning() {
    // Pass 6 fix: POST must seed the registry status synchronously *inside*
    // the handler, before returning. Without that, a follow-up GET could miss
    // the row while the supervisor task is still queued.
    //
    // Pass 11 strengthening: read via `try_status_snapshot()` (a sync,
    // non-yielding read) instead of the async `status_snapshot().await`. If
    // the seed call at `api.rs::post_node` were removed, the spawned
    // supervisor task — which would otherwise seed the row at its first
    // iteration — has NOT had a chance to run yet between `oneshot.await`
    // returning and this sync read on the current_thread runtime. So this
    // assertion would fail, pinning the synchronous-seed behavior.
    let dir = tempfile::tempdir().unwrap();
    let config_path = starter_toml(dir.path());
    let config_path_str = config_path.to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry.clone(), config_path_str, test_psk());
    let app = router(state);

    let body = serde_json::json!({"node_id": "seeded", "address": "127.0.0.1:65533"});
    let post = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(post).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Sync read — no scheduler yields between `oneshot().await` completing
    // and this line, so the supervisor task cannot have run yet.
    let snap = registry
        .try_status_snapshot()
        .expect("status RwLock should be uncontended right after POST returns");
    assert!(
        snap.iter().any(|s| s.node_id == "seeded"),
        "registry should contain 'seeded' synchronously after POST returns (no supervisor yield in between); got: {:?}",
        snap.iter().map(|s| &s.node_id).collect::<Vec<_>>()
    );
}
