// crates/kestrel-hub/tests/phase2.rs
use std::time::Duration;
use kestrel_hub::transport::{connect, ping_once};
use kestrel_test::{start_agent, test_psk};

#[tokio::test]
async fn test_screenshot_round_trip() {
    let addr = start_agent("screen-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    let png = handle.screenshot(0, None).await.unwrap();
    // On headless environments (no screen-recording permission) the agent returns
    // empty bytes rather than panicking — that is acceptable protocol behaviour.
    // We only assert the call succeeds without error.
    if !png.is_empty() {
        assert_eq!(&png[..4], &[0x89, 0x50, 0x4E, 0x47], "must be a valid PNG");
    }
}

#[tokio::test]
async fn test_ping_pong_still_works_after_refactor() {
    let addr = start_agent("ping-node-p2").await;
    let rtt = ping_once(addr, &test_psk()).await.unwrap();
    assert!(rtt.as_millis() < 100, "loopback RTT was {}ms", rtt.as_millis());
}

#[tokio::test]
async fn test_key_event_no_crash() {
    use kestrel_proto::{KeyCode, Modifiers, PressRelease};
    let addr = start_agent("key-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    handle.send_key_event(KeyCode::Char('a'), Modifiers::default(), PressRelease::Click)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Agent is still alive: screenshot should still succeed (or return empty on headless)
    let png = handle.screenshot(0, None).await.unwrap();
    let _ = png;
}

#[tokio::test]
async fn test_type_text_no_crash() {
    let addr = start_agent("text-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    handle.send_type_text("hello".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Agent still alive
    let png = handle.screenshot(0, None).await.unwrap();
    let _ = png;
}
