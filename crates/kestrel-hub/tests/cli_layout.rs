// crates/kestrel-hub/tests/cli_layout.rs
//
// Subprocess tests for `kestrel-hub layout-set` / `layout-unset` that pin
// the HTTP-first-with-fallback behavior added by the KVM hot-reload work:
//
//   - When the hub is running AND auth succeeds, the change applies live
//     via POST/DELETE /api/layout. (Implicitly tested by the
//     dashboard_layout_api.rs integration tests — those exercise the
//     server side directly.)
//
//   - When the hub responds with an HTTP error (e.g. 401 from a wrong
//     token, 500 from a malformed config, 404 from a missing entry on
//     unset), the CLI must SURFACE the error and refuse to silently fall
//     back to writing the file. Otherwise the running hub's in-memory
//     layout diverges from kestrel.toml.
//
//   - When the hub is unreachable (transport failure), the CLI falls
//     back to file mutation. We don't explicitly cover that path here;
//     it's the documented default with no hub on the wire.
//
// Mirrors the pattern in `cli_add_remove.rs`.

use std::process::Command;
use std::sync::Arc;
use tokio::net::TcpListener;

use kestrel_hub::dashboard::{router, AppState};
use kestrel_hub::router::NodeRegistry;

const HUB_TOKEN: &str = "test-control-token-layout-cli-aa";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_kestrel-hub")
}

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

async fn spawn_hub_with_token(config_path: &std::path::Path) -> String {
    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(
        registry,
        config_path.to_str().unwrap().to_string(),
        test_master(),
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
async fn layout_set_surfaces_hub_401_and_leaves_config_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = starter_toml(dir.path());
    let hub_url = spawn_hub_with_token(&cfg).await;
    let before = std::fs::read_to_string(&cfg).unwrap();

    let out = Command::new(bin())
        .env("KESTREL_TOKEN", "wrong-token")
        .args([
            "layout-set",
            "alpha",
            "1",
            "0",
            "--hub",
            &hub_url,
            "--config",
            cfg.to_str().unwrap(),
        ])
        .output()
        .expect("failed to spawn kestrel-hub");

    assert!(
        !out.status.success(),
        "layout-set should fail when the hub returns 401; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("hub returned 401"),
        "expected 'hub returned 401' in stderr, got: {}",
        stderr
    );

    // On-disk config MUST be byte-for-byte unchanged: the CLI must NOT
    // have fallen back to file mutation when the running hub rejected.
    let after = std::fs::read_to_string(&cfg).unwrap();
    assert_eq!(
        before, after,
        "CLI must not write the config when the hub rejected layout-set"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn layout_unset_surfaces_hub_401_and_leaves_config_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = starter_toml(dir.path());
    let hub_url = spawn_hub_with_token(&cfg).await;
    let before = std::fs::read_to_string(&cfg).unwrap();

    let out = Command::new(bin())
        .env("KESTREL_TOKEN", "wrong-token")
        .args([
            "layout-unset",
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
        "layout-unset should fail when the hub returns 401; stderr: {}",
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
        "CLI must not write the config when the hub rejected layout-unset"
    );
}
