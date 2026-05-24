// crates/kestrel-hub/tests/unenroll.rs
//
// Subprocess test for `kestrel-hub unenroll`. Verifies:
// (1) without --yes, no destructive action happens (dry-run mode)
// (2) with --yes and --keep-config, the TOML stays
// (3) with --yes alone, the TOML is deleted
//
// IMPORTANT: --yes invokes `enrollment::clear_hub_keyring()`, which deletes
// the `kestrel`/`psk` and `kestrel`/`control_token` entries from the
// developer's REAL system keyring. Tests that exercise --yes are gated
// behind `#[ignore]` so `cargo test` against a developer machine with a
// real Kestrel install doesn't clobber the operator's setup. Opt in with
// `cargo test --include-ignored` when you specifically want to verify the
// --yes paths. The dry-run and missing-config paths are kept un-ignored
// since they touch neither the keyring nor the file.
use std::process::Command;

use kestrel_test::starter_toml;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_kestrel-hub")
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
#[ignore = "touches real OS keyring; run with --include-ignored to verify"]
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
#[ignore = "touches real OS keyring; run with --include-ignored to verify"]
fn unenroll_yes_with_missing_config_is_a_no_op_not_an_error() {
    // The unenroll command's file-delete step must handle "file already
    // gone" as a no-op rather than an error. Reproduces the missing-file
    // branch: dry-run prints "does not exist, skipping" and --yes prints
    // nothing for the file but still exits 0.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kestrel.toml");
    assert!(!path.exists(), "precondition: file must not exist");

    let out = Command::new(bin())
        .args(["unenroll", "--config", path.to_str().unwrap(), "--yes"])
        .output()
        .expect("failed to spawn kestrel-hub");

    assert!(
        out.status.success(),
        "expected exit 0 on missing-file unenroll; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!path.exists(), "file still does not exist after unenroll");
}

#[test]
fn unenroll_dry_run_with_missing_config_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kestrel.toml");

    let out = Command::new(bin())
        .args(["unenroll", "--config", path.to_str().unwrap()])
        .output()
        .expect("failed to spawn kestrel-hub");

    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("does not exist"),
        "dry-run should explicitly report missing file; got: {}",
        stdout
    );
}

#[test]
#[ignore = "touches real OS keyring; run with --include-ignored to verify"]
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
