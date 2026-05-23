// crates/kestrel-agent/tests/unenroll.rs
//
// Subprocess test for `kestrel-agent unenroll`. Mirrors the hub-side tests:
// dry-run preserves files, --yes deletes them unless --keep-config.
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_kestrel-agent")
}

fn starter_toml(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("kestrel.toml");
    std::fs::write(
        &path,
        r#"
[agent]
node_id = "test-node"
listen  = "127.0.0.1:7272"
psk     = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
"#,
    )
    .unwrap();
    path
}

#[test]
fn unenroll_dry_run_does_not_delete_config() {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path());

    let out = Command::new(bin())
        .args(["unenroll", "--config", path.to_str().unwrap()])
        .output()
        .expect("failed to spawn kestrel-agent");

    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("would"), "expected dry-run output, got: {}", stdout);
    assert!(path.exists(), "config file should NOT be deleted in dry-run");
}

#[test]
fn unenroll_yes_deletes_config_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path());

    let out = Command::new(bin())
        .args(["unenroll", "--config", path.to_str().unwrap(), "--yes"])
        .output()
        .expect("failed to spawn kestrel-agent");

    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(!path.exists(), "config file should be deleted");
}

#[test]
fn unenroll_yes_with_missing_config_is_a_no_op_not_an_error() {
    // Agent-side mirror of the hub test — missing-file branch must be a
    // clean no-op.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kestrel.toml");
    assert!(!path.exists());

    let out = Command::new(bin())
        .args(["unenroll", "--config", path.to_str().unwrap(), "--yes"])
        .output()
        .expect("failed to spawn kestrel-agent");

    assert!(
        out.status.success(),
        "expected exit 0 on missing-file unenroll; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!path.exists());
}

#[test]
fn unenroll_yes_keep_config_preserves_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path());

    let out = Command::new(bin())
        .args([
            "unenroll",
            "--config",
            path.to_str().unwrap(),
            "--yes",
            "--keep-config",
        ])
        .output()
        .expect("failed to spawn kestrel-agent");

    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(path.exists(), "config file should be kept with --keep-config");
}
