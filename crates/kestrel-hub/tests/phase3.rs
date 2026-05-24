// crates/kestrel-hub/tests/phase3.rs
use std::time::Duration;
use kestrel_hub::transport::connect;
use kestrel_test::{start_agent, test_psk};

// NOTE: PTY slave ttys have ECHO enabled by default, so anything written to
// the master is echoed back through the master *before* the shell touches
// it. To verify the shell actually *executed* the command, the assertion
// target must be something that only the shell can produce — typically a
// substring that's the *result* of an expansion (arithmetic, command
// substitution) and that doesn't appear in the input bytes. Otherwise the
// test passes even if the shell never ran the command at all.

#[tokio::test]
async fn test_shell_run_echo() {
    let addr = start_agent("shell-echo-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    // 7*11 = 77. The literal "TAG_77" only appears in the shell's
    // arithmetic-expanded output — the input bytes contain "TAG_$((7*11))"
    // but no "77", so terminal echo alone cannot satisfy the assertion.
    let output = handle.run_shell("printf 'TAG_%d\\n' $((7*11))").await.unwrap();
    assert!(
        output.contains("TAG_77"),
        "expected arithmetic-expanded 'TAG_77' in shell output, got: {:?}",
        output
    );
}

#[tokio::test]
async fn test_shell_interactive_open_write_read_close() {
    let addr = start_agent("shell-interactive-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();

    let pty_id = handle.spawn_shell(None, 80, 24).await.unwrap();

    // 33+11 = 44. "INT_44_END" only appears after the shell evaluates the
    // arithmetic expression — terminal echo of input alone cannot match.
    handle
        .write_shell(pty_id, b"printf 'INT_%d_END\\n' $((33+11))\n".to_vec())
        .await
        .unwrap();

    // Give the shell time to process the command
    tokio::time::sleep(Duration::from_millis(300)).await;

    let raw = handle.read_shell_buffer(pty_id).await.unwrap();
    let output = String::from_utf8_lossy(&raw);
    assert!(
        output.contains("INT_44_END"),
        "expected arithmetic-expanded 'INT_44_END' in buffered output, got: {:?}",
        output
    );

    handle.close_shell(pty_id).await.unwrap();
}

#[tokio::test]
async fn test_shell_run_multiline() {
    let addr = start_agent("shell-multiline-node").await;
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    // 10+5 = 15 and 20+5 = 25. Neither "A15_" nor "B25_" appears in the
    // input bytes (which only contain the unexpanded "$((10+5))"/"$((20+5))"
    // forms), so the assertion fails if the shell didn't actually run.
    let output = handle
        .run_shell("printf 'A%d_\\n' $((10+5)) && printf 'B%d_\\n' $((20+5))")
        .await
        .unwrap();
    assert!(output.contains("A15_"), "missing A15_ in: {:?}", output);
    assert!(output.contains("B25_"), "missing B25_ in: {:?}", output);
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
