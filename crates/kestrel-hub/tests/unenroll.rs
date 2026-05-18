// crates/kestrel-hub/tests/unenroll.rs
//
// Subprocess test for `kestrel-hub unenroll`. Verifies:
// (1) without --yes, no destructive action happens (dry-run mode)
// (2) with --yes and --keep-config, the TOML stays
// (3) with --yes alone, the TOML is deleted
//
// Keyring deletion is hard to test cross-platform — we only assert the
// config-file path; the keyring path is exercised in the bin's enrollment
// unit tests and at runtime.
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_kestrel-hub")
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

#[test]
fn unenroll_dry_run_does_not_delete_config() {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path());

    let out = Command::new(bin())
        .args(["unenroll", "--config", path.to_str().unwrap()])
        .output()
        .expect("failed to spawn kestrel-hub");

    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("would"), "expected dry-run output, got: {}", stdout);
    assert!(stdout.contains("Re-run with --yes"));
    assert!(path.exists(), "config file should NOT be deleted in dry-run");
}

#[test]
fn unenroll_yes_deletes_config_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path());

    let out = Command::new(bin())
        .args(["unenroll", "--config", path.to_str().unwrap(), "--yes"])
        .output()
        .expect("failed to spawn kestrel-hub");

    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(!path.exists(), "config file should be deleted");
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
        .expect("failed to spawn kestrel-hub");

    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(path.exists(), "config file should be kept with --keep-config");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("kept"));
}
