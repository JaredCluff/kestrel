// crates/kestrel-hub/tests/phase4.rs
use kestrel_hub::transport::connect;
use kestrel_test::{start_agent, test_psk};

#[tokio::test]
async fn test_describe_returns_valid_node() {
    // On CI/headless this returns fallback=true with role="root".
    // On a desktop with AX permission it returns the real focused app tree.
    // Either way the response must be a valid AccessibilityNode (no panic, no error).
    let addr = start_agent("ax-fallback-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    let tree = handle.describe(0).await.unwrap();
    assert!(!tree.role.is_empty(), "role must be non-empty even in fallback mode");
}

#[tokio::test]
#[ignore = "requires Accessibility permission (macOS TCC → System Settings → Privacy & Security → Accessibility); run manually"]
async fn test_describe_real_ax_tree() {
    // Grant Accessibility access to the terminal (or IDE) running this test, then un-ignore.
    let addr = start_agent("ax-real-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    let tree = handle.describe(0).await.unwrap();
    assert!(
        !tree.fallback,
        "expected real AX tree but got fallback — check Accessibility permission"
    );
    assert!(!tree.role.is_empty(), "real tree root must have a non-empty role");
}
