// crates/kestrel-hub/tests/dashboard_session_auth.rs
//
// End-to-end coverage of the /login + cookie flow added in PR-1 of the
// session-auth series. Pairs with `dashboard_auth.rs` (which covers the
// CLI's bearer-token path); together they pin both auth surfaces.
//
// All tests drive the axum router directly via `tower::ServiceExt::oneshot`.
// No real TCP socket needed — the cookie / header behavior is independent
// of transport.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use kestrel_hub::dashboard::{router, session, AppState};
use kestrel_hub::router::NodeRegistry;
use kestrel_test::{build_app_with_token, starter_toml, test_master};
use tower::ServiceExt;

const TOKEN: &str = "test-control-token-123456789abcdef";

fn build_app() -> (axum::Router, AppState) {
    build_app_with_token(TOKEN)
}

fn build_app_with_master(master: Vec<u8>) -> (axum::Router, AppState) {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path()).to_str().unwrap().to_string();
    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, path, master).with_control_token(TOKEN.into());
    Box::leak(Box::new(dir));
    (router(state.clone()), state)
}

/// Pull `kestrel_session=<value>` out of a Set-Cookie header.
fn cookie_value_from_set_cookie(set_cookie: &str) -> Option<&str> {
    let after_eq = set_cookie.strip_prefix("kestrel_session=")?;
    after_eq.split(';').next()
}

#[tokio::test]
async fn login_get_renders_form() {
    let (app, _) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/login")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let html = String::from_utf8_lossy(&body);
    assert!(html.contains(r#"action="/login""#));
    assert!(html.contains(r#"name="token""#));
    // The form posts back to /login, so a method=post must be present.
    assert!(html.contains(r#"method="post""#));
}

#[tokio::test]
async fn login_with_correct_token_sets_cookie_and_redirects() {
    let (app, _) = build_app();
    let body = format!("token={}", TOKEN);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    // axum's Redirect::to defaults to 303 See Other for non-GET-equivalent
    // workflows. Either 303 or 302 would be a valid post-PRG redirect.
    assert!(
        matches!(resp.status(), StatusCode::SEE_OTHER | StatusCode::FOUND),
        "expected redirect after successful login, got {}",
        resp.status()
    );
    let set_cookie = resp
        .headers()
        .get(header::SET_COOKIE)
        .expect("Set-Cookie header must be present on successful login")
        .to_str()
        .unwrap();
    assert!(set_cookie.starts_with("kestrel_session="));
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("SameSite=Strict"));
    assert!(set_cookie.contains("Path=/"));
    assert!(set_cookie.contains("Max-Age="));
    // Location header should point home.
    let loc = resp
        .headers()
        .get(header::LOCATION)
        .expect("Location header on redirect")
        .to_str()
        .unwrap();
    assert_eq!(loc, "/");
}

#[tokio::test]
async fn login_with_wrong_token_returns_401_and_no_cookie() {
    let (app, _) = build_app();
    let body = "token=definitely-not-the-real-token";
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    // No cookie should be set on failure — otherwise a brute-forcer could
    // discover the token via Set-Cookie presence rather than body content.
    assert!(resp.headers().get(header::SET_COOKIE).is_none());
}

#[tokio::test]
async fn cookie_authenticates_post_node() {
    let (app, state) = build_app();
    // Issue a valid cookie directly (faster than going through /login).
    let (set_cookie_header, _expiry) =
        session::set_cookie_header(&state.session_key, session::DEFAULT_SESSION_TTL_SECS);
    let cookie_value = cookie_value_from_set_cookie(&set_cookie_header)
        .expect("freshly-issued Set-Cookie must contain our cookie name");

    let body = serde_json::json!({"node_id": "via-cookie", "address": "127.0.0.1:65530"});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes")
                .header("content-type", "application/json")
                .header("cookie", format!("kestrel_session={}", cookie_value))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "valid cookie alone must authenticate POST /api/nodes (no bearer required)"
    );
}

#[tokio::test]
async fn tampered_cookie_is_rejected() {
    let (app, state) = build_app();
    let (set_cookie_header, _expiry) =
        session::set_cookie_header(&state.session_key, session::DEFAULT_SESSION_TTL_SECS);
    let cookie_value = cookie_value_from_set_cookie(&set_cookie_header).unwrap();

    // Bump the expiry but keep the original HMAC. Verifier must reject.
    let (expiry, mac) = cookie_value.split_once('.').unwrap();
    let bumped: u64 = expiry.parse::<u64>().unwrap() + 999_999;
    let tampered = format!("kestrel_session={}.{}", bumped, mac);

    let body = serde_json::json!({"node_id": "bogus", "address": "127.0.0.1:65530"});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes")
                .header("content-type", "application/json")
                .header("cookie", tampered)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn cookie_issued_under_old_master_is_rejected_after_rotation() {
    // Operator rotates the hub's master_secret (rerun `kestrel-hub init`).
    // The session_key changes, so any browser still holding the previous
    // cookie MUST be forced back to /login. Pins the automatic-revocation
    // property the security model promises.
    let (_old_app, old_state) = build_app_with_master(test_master());
    let (cookie_header, _) = session::set_cookie_header(
        &old_state.session_key,
        session::DEFAULT_SESSION_TTL_SECS,
    );
    let cookie_value = cookie_value_from_set_cookie(&cookie_header).unwrap().to_string();

    // Same control_token, different master_secret.
    let (new_app, _new_state) =
        build_app_with_master(b"kestrel-test-NEW-master-32bytes!".to_vec());

    let body = serde_json::json!({"node_id": "rotated", "address": "127.0.0.1:65530"});
    let resp = new_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes")
                .header("content-type", "application/json")
                .header("cookie", format!("kestrel_session={}", cookie_value))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "pre-rotation cookie must NOT authenticate against the new master_secret"
    );
}

#[tokio::test]
async fn bearer_token_still_works_alongside_cookie_path() {
    // Regression: PR-1 added the cookie branch to check_auth, but the CLI's
    // bearer-token path must keep working unchanged. The two paths are
    // independent.
    let (app, _) = build_app();
    let body = serde_json::json!({"node_id": "bearer", "address": "127.0.0.1:65530"});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {}", TOKEN))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn logout_clears_cookie_and_redirects_to_login() {
    let (app, _) = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/logout")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        resp.status(),
        StatusCode::SEE_OTHER | StatusCode::FOUND
    ));
    let set_cookie = resp
        .headers()
        .get(header::SET_COOKIE)
        .expect("logout must set a clearing cookie")
        .to_str()
        .unwrap();
    assert!(set_cookie.contains("kestrel_session=;"));
    assert!(set_cookie.contains("Max-Age=0"));
    let loc = resp.headers().get(header::LOCATION).unwrap().to_str().unwrap();
    assert_eq!(loc, "/login");
}

#[tokio::test]
async fn missing_credentials_returns_401_with_both_methods_hinted() {
    // When no bearer and no cookie are present, the 401 body should hint
    // at BOTH auth paths — that's the only signal a curling user has that
    // the cookie path exists.
    let (app, _) = build_app();
    let body = serde_json::json!({"node_id": "anon", "address": "127.0.0.1:65530"});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let bytes = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let msg = String::from_utf8_lossy(&bytes);
    assert!(msg.to_lowercase().contains("bearer"), "401 body should mention bearer: {}", msg);
    assert!(msg.to_lowercase().contains("cookie"), "401 body should mention cookie: {}", msg);
}
