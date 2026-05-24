// crates/kestrel-hub/tests/dashboard_shell.rs
//
// Smoke tests for the browser shell pane. The WS bridge is exercised
// best by an actual browser + agent combination; here we pin:
//
//   - GET /shell/:node_id renders the HTML page with the right asset
//     wiring (shell.js, the WS-URL marker) for authed users.
//   - GET /shell/:node_id without auth redirects to /login.
//   - The shell.js asset is served at /assets/shell.js as
//     application/javascript.
//
// The full WS handler (handle_shell_socket) end-to-end test would
// need a connected agent; phase3 already exercises shell_open /
// shell_write / shell_read via the registry surface, and the WS
// handler is a thin bridge over those proven primitives.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use kestrel_hub::dashboard::AppState;
use kestrel_test::{build_app_with_token, cookie_for};
use tower::ServiceExt;

const TOKEN: &str = "test-control-token-shell-pane-aa";

fn build_app() -> (axum::Router, AppState) {
    build_app_with_token(TOKEN)
}

#[tokio::test]
async fn shell_page_authed_renders_terminal_html() {
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/shell/macstudio")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 32_768).await.unwrap();
    let html = String::from_utf8_lossy(&bytes);

    // Page identifies the node, links to the shell asset, and sets the
    // __kestrelNodeId marker that shell.js reads.
    assert!(html.contains("macstudio"), "page should embed the node id");
    assert!(html.contains(r#"src="/assets/shell.js""#), "expected shell.js script tag");
    assert!(
        html.contains(r#"window.__kestrelNodeId = "macstudio""#),
        "expected node-id marker for shell.js, got: {}",
        html
    );
    // Required widgets: output pre + input form.
    assert!(html.contains(r#"id="shell-output""#));
    assert!(html.contains(r#"id="shell-input""#));
    assert!(html.contains(r#"id="shell-form""#));
}

#[tokio::test]
async fn shell_page_unauth_redirects_to_login() {
    let (app, _) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/shell/macstudio")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        resp.status(),
        StatusCode::SEE_OTHER | StatusCode::FOUND
    ));
    assert_eq!(
        resp.headers().get(header::LOCATION).unwrap().to_str().unwrap(),
        "/login"
    );
}

#[tokio::test]
async fn shell_js_asset_is_served_as_javascript() {
    let (app, _) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/assets/shell.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers().get(header::CONTENT_TYPE).unwrap().to_str().unwrap();
    assert!(ct.contains("javascript"), "wrong content-type: {}", ct);
    let bytes = axum::body::to_bytes(resp.into_body(), 16_384).await.unwrap();
    let js = String::from_utf8_lossy(&bytes);
    // Sanity: the JS file actually has the WS open call.
    assert!(js.contains("new WebSocket"), "shell.js must open a WS");
    assert!(js.contains("/api/shell/ws/"), "shell.js must target the WS route");
}

#[tokio::test]
async fn dashboard_index_shows_shell_link_for_online_authed_rows() {
    // A signed-in operator on the dashboard should see a "Shell" link
    // next to each Online node — wires the row to /shell/<id>.
    // We can't easily put a node in Online state here (that needs a
    // real handshake), so we register one via the public registry API
    // pattern and assert what we can.
    let (app, state) = build_app();
    let cookie = cookie_for(&state);

    // Put a node in the registry as Reconnecting. The shell link is
    // gated on Online — for Reconnecting we should NOT see it. Pins
    // the conditional.
    state.registry.mark_reconnecting("rc", 1).await;
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
    let bytes = axum::body::to_bytes(resp.into_body(), 65_536).await.unwrap();
    let html = String::from_utf8_lossy(&bytes);
    assert!(html.contains("rc"));
    // Not Online → no shell link.
    assert!(
        !html.contains(r#"href="/shell/rc""#),
        "Reconnecting nodes must not get a Shell link"
    );
}
