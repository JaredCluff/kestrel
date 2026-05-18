// crates/kestrel-hub/tests/phase3.rs
use std::net::SocketAddr;
use std::time::Duration;
use kestrel_agent::config::AgentConfig;
use kestrel_hub::transport::connect;

fn test_psk() -> Vec<u8> {
    b"kestrel-test-psk-32bytes-padded!".to_vec()
}

async fn start_agent(node_id: &str) -> SocketAddr {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let cfg = AgentConfig::new("127.0.0.1:0".parse().unwrap(), node_id.into(), test_psk());
    tokio::spawn(async move {
        kestrel_agent::transport::serve(&cfg, Some(ready_tx)).await.unwrap();
    });
    ready_rx.await.expect("agent did not send bound address")
}

#[tokio::test]
async fn test_shell_run_echo() {
    let addr = start_agent("shell-echo-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    let output = handle.run_shell("echo kestrel-phase3-ok").await.unwrap();
    assert!(
        output.contains("kestrel-phase3-ok"),
        "expected 'kestrel-phase3-ok' in shell output, got: {:?}",
        output
    );
}

#[tokio::test]
async fn test_shell_interactive_open_write_read_close() {
    let addr = start_agent("shell-interactive-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();

    let pty_id = handle.spawn_shell(None, 80, 24).await.unwrap();

    handle.write_shell(pty_id, b"echo interactive-test\n".to_vec()).await.unwrap();

    // Give the shell time to process the command
    tokio::time::sleep(Duration::from_millis(300)).await;

    let raw = handle.read_shell_buffer(pty_id).await.unwrap();
    let output = String::from_utf8_lossy(&raw);
    assert!(
        output.contains("interactive-test"),
        "expected 'interactive-test' in buffered output, got: {:?}",
        output
    );

    handle.close_shell(pty_id).await.unwrap();
}

#[tokio::test]
async fn test_shell_run_multiline() {
    let addr = start_agent("shell-multiline-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    let output = handle.run_shell("echo line1 && echo line2").await.unwrap();
    assert!(output.contains("line1"), "missing line1 in: {:?}", output);
    assert!(output.contains("line2"), "missing line2 in: {:?}", output);
}

#[tokio::test]
#[ignore = "requires display server / clipboard daemon; run manually"]
async fn test_clipboard_text_roundtrip() {
    let addr = start_agent("clipboard-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    use kestrel_proto::ClipboardContent;
    handle.clipboard_write(ClipboardContent::Text("kestrel-clipboard-xyz".into())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let got = handle.clipboard_read().await.unwrap();
    assert_eq!(got, ClipboardContent::Text("kestrel-clipboard-xyz".into()));
}
