// crates/kestrel-hub/tests/cli_add_remove.rs
//
// Subprocess tests for `kestrel-hub add-node` / `remove-node` that pin the
// Pass 8 (CG-F1) fix: when the running hub responds with an HTTP error,
// the CLI MUST surface it and refuse to silently fall back to local file
// mutation — otherwise the running hub's config and the on-disk config
// diverge.
//
// Strategy: spin up the real dashboard router with a control token, then
// invoke the CLI with a WRONG token via `KESTREL_TOKEN`. The hub will
// respond 401, the CLI must exit non-zero, and the on-disk config must
// remain unchanged.
//
// (We don't need a separate "transport failure means file fallback" test
// here — the unenroll/init tests + the no-arg start-without-config tests
// cover the file-only paths from other angles. This test specifically
// pins the "hub responded => surface, don't fall back" branch.)

use std::process::Command;
use std::sync::Arc;
use tokio::net::TcpListener;

use kestrel_hub::dashboard::{router, AppState};
use kestrel_hub::router::NodeRegistry;

const HUB_TOKEN: &str = "test-control-token-deadbeef";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_kestrel-hub")
}

fn test_psk() -> Vec<u8> {
    b"kestrel-test-psk-32bytes-padded!".to_vec()
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

async fn spawn_hub_with_token(config_path: &std::path::Path) -> String {
    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(
        registry,
        config_path.to_str().unwrap().to_string(),
        test_psk(),
    )
    .with_control_token(HUB_TOKEN.into());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{}", addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_node_surfaces_hub_401_and_leaves_config_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = starter_toml(dir.path());
    let hub_url = spawn_hub_with_token(&cfg).await;
    let before = std::fs::read_to_string(&cfg).unwrap();

    let out = Command::new(bin())
        .env("KESTREL_TOKEN", "wrong-token")
        .args([
            "add-node",
            "alpha",
            "127.0.0.1:65530",
            "--hub",
            &hub_url,
            "--config",
            cfg.to_str().unwrap(),
        ])
        .output()
        .expect("failed to spawn kestrel-hub");

    assert!(
        !out.status.success(),
        "add-node should fail when the hub returns 401; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("hub returned 401"),
        "expected 'hub returned 401' in stderr, got: {}",
        stderr
    );

    // The file MUST be byte-for-byte unchanged — the CLI must NOT have
    // fallen back to local file mutation.
    let after = std::fs::read_to_string(&cfg).unwrap();
    assert_eq!(
        before, after,
        "CLI must not write the config when the hub rejected the change"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_node_surfaces_hub_401_and_leaves_config_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = starter_toml(dir.path());
    let hub_url = spawn_hub_with_token(&cfg).await;
    let before = std::fs::read_to_string(&cfg).unwrap();

    let out = Command::new(bin())
        .env("KESTREL_TOKEN", "wrong-token")
        .args([
            "remove-node",
            "alpha",
            "--hub",
            &hub_url,
            "--config",
            cfg.to_str().unwrap(),
        ])
        .output()
        .expect("failed to spawn kestrel-hub");

    assert!(
        !out.status.success(),
        "remove-node should fail when the hub returns 401; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("hub returned 401"),
        "expected 'hub returned 401' in stderr, got: {}",
        stderr
    );

    let after = std::fs::read_to_string(&cfg).unwrap();
    assert_eq!(
        before, after,
        "CLI must not write the config when the hub rejected the change"
    );
}
