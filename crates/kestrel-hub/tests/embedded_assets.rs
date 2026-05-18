// crates/kestrel-hub/tests/embedded_assets.rs
//
// Verify that the dashboard's /assets/* routes serve the embedded asset bytes
// regardless of the test runner's working directory. Pre-this-change, the
// handler was `ServeDir::new("crates/kestrel-hub/assets")` — relative to CWD —
// which broke any `cargo install` deployment.
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use kestrel_hub::dashboard::{AppState, router};
use kestrel_hub::router::NodeRegistry;
use tower::ServiceExt;

fn test_psk() -> Vec<u8> {
    b"kestrel-test-psk-32bytes-padded!".to_vec()
}

fn app() -> axum::Router {
    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, "kestrel.toml".into(), test_psk());
    router(state)
}

async fn fetch_asset(name: &str) -> (StatusCode, Option<String>, Vec<u8>) {
    let req = Request::builder()
        .uri(format!("/assets/{}", name))
        .body(Body::empty())
        .unwrap();
    let resp = app().oneshot(req).await.unwrap();
    let status = resp.status();
    let ctype = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap().to_vec();
    (status, ctype, body)
}

#[tokio::test]
async fn dashboard_css_is_embedded_and_served() {
    let (status, ctype, body) = fetch_asset("dashboard.css").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ctype.as_deref(), Some("text/css; charset=utf-8"));
    assert!(body.starts_with(b"/* Kestrel dashboard"), "css body unexpected: {:?}", &body[..body.len().min(80)]);
}

#[tokio::test]
async fn htmx_min_js_is_embedded_and_served() {
    let (status, ctype, body) = fetch_asset("htmx.min.js").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ctype.as_deref(), Some("application/javascript; charset=utf-8"));
    // htmx.min.js is ~50 KB and starts with the var htmx declaration.
    assert!(body.len() > 10_000, "htmx.min.js too small: {} bytes", body.len());
    assert!(std::str::from_utf8(&body[..30]).unwrap_or("").contains("htmx"));
}

#[tokio::test]
async fn htmx_sse_js_is_embedded_and_served() {
    let (status, ctype, body) = fetch_asset("htmx-sse.js").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ctype.as_deref(), Some("application/javascript; charset=utf-8"));
    assert!(body.len() > 1_000, "htmx-sse.js too small: {} bytes", body.len());
}

#[tokio::test]
async fn unknown_asset_returns_404() {
    let (status, _, _) = fetch_asset("does-not-exist.txt").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
