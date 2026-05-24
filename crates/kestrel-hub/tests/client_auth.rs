// crates/kestrel-hub/tests/client_auth.rs
//
// End-to-end test of the HubClient's auth-failure surface. Pairs with the
// server-side `dashboard_auth.rs` tests, which use `tower::ServiceExt::oneshot`
// to drive the router directly. These tests go over a real TCP socket via
// `axum::serve` so the reqwest client's bearer_auth header serialization is
// exercised — the dashboard_auth tests can't catch a client-side regression.

use std::sync::Arc;

use kestrel_hub::client::HubClient;
use kestrel_hub::dashboard::{AppState, router};
use kestrel_hub::router::NodeRegistry;
use kestrel_test::{starter_toml, test_psk};
use tempfile::TempDir;
use tokio::net::TcpListener;

const TOKEN: &str = "test-control-token-deadbeef";

/// Spin up the dashboard router on a random port and return its base URL +
/// the tempdir (held to keep the kestrel.toml alive for the test).
async fn spawn_hub_with_token() -> (String, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path()).to_str().unwrap().to_string();
    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, path, test_psk()).with_control_token(TOKEN.into());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (format!("http://{}", addr), dir)
}

#[tokio::test]
async fn add_node_without_token_returns_401_error_with_hint() {
    let (base, _dir) = spawn_hub_with_token().await;
    let client = HubClient::new(&base); // no .with_token(...)
    let err = client
        .add_node("alpha", "127.0.0.1:65530")
        .await
        .unwrap_err();
    let msg = err.to_string();
    // HubClient::add_node / remove_node format failures as
    // "hub returned 401 Unauthorized: <body>" — anchor the assertion to the
    // exact 3-character status code so a coincidental match against a port
    // number (or a different reqwest error format on upgrade) can't pass.
    assert!(
        msg.contains("hub returned 401"),
        "expected 'hub returned 401' in error, got: {}",
        msg
    );
}

#[tokio::test]
async fn add_node_with_wrong_token_returns_401() {
    let (base, _dir) = spawn_hub_with_token().await;
    let client = HubClient::new(&base).with_token("wrong-token".into());
    let err = client
        .add_node("alpha", "127.0.0.1:65531")
        .await
        .unwrap_err();
    let msg = err.to_string();
    // HubClient::add_node / remove_node format failures as
    // "hub returned 401 Unauthorized: <body>" — anchor the assertion to the
    // exact 3-character status code so a coincidental match against a port
    // number (or a different reqwest error format on upgrade) can't pass.
    assert!(
        msg.contains("hub returned 401"),
        "expected 'hub returned 401' in error, got: {}",
        msg
    );
}

#[tokio::test]
async fn add_node_with_correct_token_succeeds() {
    let (base, _dir) = spawn_hub_with_token().await;
    let client = HubClient::new(&base).with_token(TOKEN.into());
    let dto = client
        .add_node("alpha", "127.0.0.1:65532")
        .await
        .expect("add_node with correct token should succeed");
    assert_eq!(dto.node_id, "alpha");
}

#[tokio::test]
async fn remove_node_without_token_returns_401() {
    let (base, _dir) = spawn_hub_with_token().await;
    let client = HubClient::new(&base);
    let err = client.remove_node("alpha").await.unwrap_err();
    let msg = err.to_string();
    // HubClient::add_node / remove_node format failures as
    // "hub returned 401 Unauthorized: <body>" — anchor the assertion to the
    // exact 3-character status code so a coincidental match against a port
    // number (or a different reqwest error format on upgrade) can't pass.
    assert!(
        msg.contains("hub returned 401"),
        "expected 'hub returned 401' in error, got: {}",
        msg
    );
}
