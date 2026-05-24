// crates/kestrel-hub/tests/dashboard_ui_writes.rs
//
// End-to-end coverage of the form-driven write UI added in PR-2:
//   - POST /ui/nodes (Add node form)
//   - POST /ui/nodes/:id/delete (Remove button)
//
// Pairs with `dashboard_session_auth.rs` (cookie infra) and
// `dashboard_auth.rs` (bearer path). These tests pin that the form
// handlers respect the session cookie, redirect on success, redirect
// to /login when unauthenticated, and render an inline error on a
// bad address rather than crashing.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use kestrel_hub::dashboard::AppState;
use kestrel_test::{build_app_with_token, cookie_for};
use tower::ServiceExt;

const TOKEN: &str = "test-control-token-aaaaaaaaaaaaaa";

fn build_app() -> (axum::Router, AppState) {
    build_app_with_token(TOKEN)
}

#[tokio::test]
async fn ui_add_node_with_cookie_redirects_home_and_seeds_registry() {
    let (app, state) = build_app();
    let cookie = cookie_for(&state);

    let body = "node_id=ui-added&address=127.0.0.1:65530";
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ui/nodes")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        matches!(resp.status(), StatusCode::SEE_OTHER | StatusCode::FOUND),
        "expected redirect to /, got {}",
        resp.status()
    );
    let loc = resp.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    assert_eq!(loc, "/");

    // The seed must have happened — registry contains the new row.
    let snap = state.registry.status_snapshot().await;
    assert!(snap.iter().any(|s| s.node_id == "ui-added"));
}

#[tokio::test]
async fn ui_add_node_without_auth_redirects_to_login() {
    let (app, _) = build_app();
    let body = "node_id=nope&address=127.0.0.1:65530";
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ui/nodes")
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
    let loc = resp.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    assert_eq!(loc, "/login");
}

#[tokio::test]
async fn ui_add_node_bad_address_renders_inline_error_with_400() {
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let body = "node_id=garbled&address=this-is-not-a-socketaddr";
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ui/nodes")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(resp.into_body(), 16_384).await.unwrap();
    let html = String::from_utf8_lossy(&bytes);
    assert!(html.contains(r#"<p class="error">"#), "expected error banner in: {}", html);
    assert!(html.contains("Invalid address"));
}

#[tokio::test]
async fn ui_delete_node_with_cookie_redirects_home_and_forgets_registry() {
    let (app, state) = build_app();
    let cookie = cookie_for(&state);

    // Add first (so there's something to delete).
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ui/nodes")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie.clone())
                .body(Body::from("node_id=victim&address=127.0.0.1:65530"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(state.registry.status_snapshot().await.iter().any(|s| s.node_id == "victim"));

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ui/nodes/victim/delete")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        resp.status(),
        StatusCode::SEE_OTHER | StatusCode::FOUND
    ));
    let loc = resp.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    assert_eq!(loc, "/");

    let snap = state.registry.status_snapshot().await;
    assert!(snap.iter().all(|s| s.node_id != "victim"));
}

#[tokio::test]
async fn ui_delete_node_without_auth_redirects_to_login() {
    let (app, _) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ui/nodes/anything/delete")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        resp.status(),
        StatusCode::SEE_OTHER | StatusCode::FOUND
    ));
    let loc = resp.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    assert_eq!(loc, "/login");
}

#[tokio::test]
async fn ui_delete_node_unknown_returns_404_with_error_page() {
    // Same error UX as add-node: stay on the dashboard with an error
    // banner instead of bouncing to a generic 404 page.
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ui/nodes/never-existed/delete")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let bytes = axum::body::to_bytes(resp.into_body(), 16_384).await.unwrap();
    let html = String::from_utf8_lossy(&bytes);
    assert!(html.contains(r#"<p class="error">"#), "expected error banner in: {}", html);
    assert!(html.contains("never-existed"));
}

#[tokio::test]
async fn index_unauthed_renders_no_write_controls() {
    // Verifies the end-to-end render path (handler → page() → HTML)
    // hides write controls for unauthenticated viewers. Complements
    // the unit test in templates.rs which asserts the same on the
    // pure-function level.
    let (app, _) = build_app();
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
    let bytes = axum::body::to_bytes(resp.into_body(), 16_384).await.unwrap();
    let html = String::from_utf8_lossy(&bytes);
    assert!(html.contains("Sign in"));
    assert!(!html.contains(r#"action="/ui/nodes""#));
}

#[tokio::test]
async fn index_authed_renders_write_controls() {
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 16_384).await.unwrap();
    let html = String::from_utf8_lossy(&bytes);
    assert!(html.contains("Sign out"));
    assert!(html.contains(r#"action="/ui/nodes""#));
}
