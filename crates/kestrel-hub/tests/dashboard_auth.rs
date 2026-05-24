// crates/kestrel-hub/tests/dashboard_auth.rs
//
// Verify that mutation endpoints (POST /api/nodes, DELETE /api/nodes/:id)
// require `Authorization: Bearer <control_token>` when the AppState has a
// control_token set. Read-only endpoints stay open either way.
use axum::body::Body;
use axum::http::{Request, StatusCode};
use kestrel_hub::dashboard::AppState;
use kestrel_test::{build_app, build_app_with_token};
use tower::ServiceExt;

const TOKEN: &str = "test-control-token-123456789abcdef";

fn app_with_token() -> (axum::Router, AppState) {
    build_app_with_token(TOKEN)
}

#[tokio::test]
async fn post_node_without_token_returns_401() {
    let (app, _) = app_with_token();
    let body = serde_json::json!({"node_id": "x", "address": "127.0.0.1:65530"});
    let req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn post_node_with_wrong_token_returns_401() {
    let (app, _) = app_with_token();
    let body = serde_json::json!({"node_id": "x", "address": "127.0.0.1:65531"});
    let req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .header("authorization", "Bearer wrong-token")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn post_node_with_correct_token_succeeds() {
    let (app, _state) = app_with_token();
    let body = serde_json::json!({"node_id": "x", "address": "127.0.0.1:65532"});
    let req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", TOKEN))
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn delete_node_without_token_returns_401() {
    let (app, _) = app_with_token();
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/nodes/ghost")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn read_only_endpoints_stay_open_without_token() {
    let (app, _) = app_with_token();

    // GET /api/nodes — should always work
    let req = Request::builder()
        .method("GET")
        .uri("/api/nodes")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // GET / (HTML dashboard) — should always work
    let req = Request::builder()
        .method("GET")
        .uri("/")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn auth_disabled_state_accepts_unauthenticated_mutations() {
    // Build a state with no control_token — legacy/no-auth mode.
    let (app, _state) = build_app();

    let body = serde_json::json!({"node_id": "x", "address": "127.0.0.1:65533"});
    let req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn delete_node_with_wrong_token_returns_401() {
    let (app, _) = app_with_token();
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/nodes/x")
        .header("authorization", "Bearer wrong-token")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn auth_disabled_state_accepts_unauthenticated_deletes() {
    // Symmetric to the POST-side test above. Without this, a future refactor
    // could tighten DELETE auth but leave POST open (or vice versa) without
    // anything failing.
    let (app, _state) = build_app();

    // Pre-add a node so DELETE has something to remove.
    let body = serde_json::json!({"node_id": "x", "address": "127.0.0.1:65534"});
    let post = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let post_resp = app.clone().oneshot(post).await.unwrap();
    assert_eq!(post_resp.status(), StatusCode::CREATED);

    let del = Request::builder()
        .method("DELETE")
        .uri("/api/nodes/x")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(del).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}
