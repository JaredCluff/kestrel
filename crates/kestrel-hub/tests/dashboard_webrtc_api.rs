// crates/kestrel-hub/tests/dashboard_webrtc_api.rs
//
// HTTP-level coverage of the WebRTC signalling endpoints introduced
// in Phase 13b. These hit the axum router with `oneshot` so they
// exercise the actual handler bodies (auth check, base64 decode,
// agent-forward dispatch) rather than just unit-testing the helpers.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kestrel_hub::dashboard::{router, session, AppState};
use kestrel_hub::router::NodeRegistry;
use tower::ServiceExt;

const TOKEN: &str = "test-control-token-aaaaaaaaaaaaaa";

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
    let state = AppState::new(registry, path, test_master()).with_control_token(TOKEN.into());
    Box::leak(Box::new(dir));
    (router(state.clone()), state)
}

fn cookie_for(state: &AppState) -> String {
    let (set_cookie, _) =
        session::set_cookie_header(&state.session_key, session::DEFAULT_SESSION_TTL_SECS);
    let value = set_cookie
        .strip_prefix("kestrel_session=")
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    format!("kestrel_session={}", value)
}

async fn json_body(resp: axum::response::Response<Body>) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn create_session_requires_auth() {
    let (app, _state) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/webrtc/session")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"node_id":"alpha"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    // check_auth returns 401 or similar — anything 4xx counts as
    // rejection. Reading the precise status is brittle if it ever
    // moves between 401 and 403.
    assert!(
        resp.status().is_client_error(),
        "expected 4xx, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn create_session_with_cookie_returns_session_id() {
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/webrtc/session")
                .header("content-type", "application/json")
                .header("cookie", cookie)
                .body(Body::from(r#"{"node_id":"alpha"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    let id = body.get("session_id").and_then(|v| v.as_str()).unwrap();
    assert!(id.starts_with("rt-"), "session id shape: {}", id);
}

#[tokio::test]
async fn post_offer_to_unknown_session_returns_404() {
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/webrtc/session/rt-nonexistent/offer")
                .header("content-type", "application/json")
                .header("cookie", cookie)
                .body(Body::from(r#"{"sdp_b64":"dj0wDQo="}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn post_offer_with_no_agent_connected_returns_202() {
    // Session exists but the target node has never connected — the
    // handler should record the offer locally and ACCEPTED so the
    // browser can poll/timeout, not 5xx.
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let id = state.webrtc_sessions.create("ghost-node".into()).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/webrtc/session/{}/offer", id))
                .header("content-type", "application/json")
                .header("cookie", &cookie)
                .body(Body::from(r#"{"sdp_b64":"dj0wDQo="}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn post_ice_to_unknown_session_returns_404() {
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/webrtc/session/rt-nonexistent/ice")
                .header("content-type", "application/json")
                .header("cookie", cookie)
                .body(Body::from(r#"{"candidate_json":"{}"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn post_ice_to_known_session_without_agent_is_204() {
    // The agent isn't connected, but the SessionRegistry can still
    // record the candidate. record_ice succeeds → 204 NO_CONTENT.
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let id = state.webrtc_sessions.create("ghost-node".into()).await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/webrtc/session/{}/ice", id))
                .header("content-type", "application/json")
                .header("cookie", cookie)
                .body(Body::from(
                    r#"{"candidate_json":"{\"candidate\":\"end-of-candidates\"}"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // The candidate was recorded.
    let session = state.webrtc_sessions.get(&id).await.unwrap();
    assert_eq!(session.ice_candidates.len(), 1);
}

#[tokio::test]
async fn get_session_returns_full_record() {
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let id = state.webrtc_sessions.create("alpha".into()).await;
    let _ = state.webrtc_sessions.record_offer(&id, "fake-b64".into()).await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/webrtc/session/{}", id))
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body.get("node_id").unwrap(), "alpha");
    assert_eq!(body.get("status").unwrap(), "offer_received");
    assert_eq!(body.get("offer_b64").unwrap(), "fake-b64");
}

#[tokio::test]
async fn node_detail_page_renders_when_authed() {
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/node/alpha")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("<video"), "page must contain a <video> element");
    assert!(
        html.contains("/assets/node-detail.js"),
        "page must reference the external bootstrap script"
    );
    assert!(
        html.contains(r#"data-node-id="alpha""#),
        "page must carry the node id via a data attribute"
    );
    assert!(html.contains("alpha"), "page should reference the node id");
}

#[tokio::test]
async fn node_detail_page_does_not_inject_node_id_into_script_context() {
    // Regression: node_id is part of the URL path and was previously
    // interpolated into an inline <script> block via Rust's `{:?}`
    // debug formatter. Debug formatting escapes JS string delimiters
    // but NOT HTML — so a node_id like `x</script><script>alert(1)`
    // could break out of the script context entirely.
    //
    // After the fix, node_id flows via a data attribute on the <video>
    // element. maud HTML-escapes attribute values, so `</script>`
    // becomes `&lt;/script&gt;` in the HTML source — no XSS surface.
    let (app, state) = build_app();
    let cookie = cookie_for(&state);
    // Percent-encode the payload by hand so we don't pull in a new
    // dep for one test. axum's Path<String> percent-decodes; the
    // handler sees the literal angle brackets + quotes.
    let evil = "evil%3C%2Fscript%3E%3Cscript%3Ealert('xss')%3C%2Fscript%3E";
    let url = format!("/node/{}", evil);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&url)
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    // The unescaped `</script>` byte sequence must not appear inside
    // any <script> block. Easiest defense: assert it doesn't appear
    // anywhere except potentially as `&lt;/script&gt;`.
    assert!(
        !html.contains("</script><script>alert"),
        "unescaped script-break sequence reached the page: {}",
        html.lines()
            .filter(|l| l.contains("alert"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    // And confirm the data-attribute carrier is present.
    assert!(
        html.contains("data-node-id="),
        "expected data-node-id attribute carrier"
    );
}

#[tokio::test]
async fn node_detail_page_without_auth_shows_sign_in_prompt() {
    let (app, _state) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/node/alpha")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        html.contains("Sign in") || html.contains("sign in"),
        "anonymous viewers should see a sign-in prompt; got: {}",
        html.chars().take(400).collect::<String>()
    );
    // And NOT see a video element (no stream for unauthenticated visitors).
    assert!(
        !html.contains("<video"),
        "anonymous viewers should not see the video element"
    );
}
