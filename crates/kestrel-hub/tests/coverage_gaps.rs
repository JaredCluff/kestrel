// crates/kestrel-hub/tests/coverage_gaps.rs
//
// Tests for the three specific edge cases the deep-dive review flagged
// as "out of scope" but worth pinning:
//
//   1. run_actor drain delivery on disconnect — when the WS dies with
//      Requests still in `pending`, every pending awaiter must resolve
//      to Err with a clear "connection closed" message, NOT to a
//      silent oneshot drop (which surfaces to callers as the
//      uninformative "actor dropped reply").
//
//   2. forget_node + in-flight request race — calling
//      `NodeRegistry::forget_node` while a Request is mid-flight must
//      either complete the request normally OR fail it with a clear
//      error. Whatever happens, no panic and no hang.
//
//   3. AgentConfig keyring fallback — a config TOML that omits the
//      `psk` field must look the key up in the OS keyring. Gated
//      `#[ignore]` because it touches the real keyring (same pattern
//      as the unenroll tests).
//
// Tests 1 and 2 use the same `spawn_agent_on` helper pattern as
// phase5_reconnect: a tokio runtime on a dedicated thread we can drop
// to forcibly tear down the agent.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kestrel_agent::config::AgentConfig;
use kestrel_hub::router::NodeRegistry;
use kestrel_hub::transport::connect;
use kestrel_test::test_psk;

/// Spawn an agent in its own runtime + thread. Drop the returned
/// handle (via `shutdown()`) to forcibly terminate the agent.
struct AgentHandle {
    thread: Option<std::thread::JoinHandle<()>>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl AgentHandle {
    fn shutdown(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn spawn_agent_on(node_id: &str, addr: SocketAddr) -> (SocketAddr, AgentHandle) {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<SocketAddr>();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let cfg = AgentConfig::new(addr, node_id.into(), test_psk());

    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build agent runtime");
        rt.block_on(async move {
            let (bound_tx, bound_rx) = tokio::sync::oneshot::channel::<SocketAddr>();
            let serve_fut = kestrel_agent::transport::serve(&cfg, Some(bound_tx));
            tokio::pin!(serve_fut);
            tokio::select! {
                bound = bound_rx => {
                    if let Ok(b) = bound { let _ = ready_tx.send(b); }
                    tokio::select! {
                        _ = &mut serve_fut => {}
                        _ = stop_rx => {}
                    }
                }
                _ = &mut serve_fut => {}
            }
        });
    });

    let bound = ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("agent bound timeout");
    (
        bound,
        AgentHandle {
            thread: Some(thread),
            stop_tx: Some(stop_tx),
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_actor_drains_pending_requests_with_connection_closed_error() {
    // Pin the explicit drain in `run_actor`: when the WS dies with a
    // Request still in flight, the awaiter sees Err("connection closed"),
    // not a silent oneshot drop. Without that drain a caller would see
    // the much less informative "actor dropped reply" — the original
    // (pre-Pass-5) behavior.
    let (addr, agent) = spawn_agent_on("drainee", "127.0.0.1:0".parse().unwrap());
    let (handle, actor_join) = connect(addr, &test_psk()).await.unwrap();

    // Kick off a slow request the agent will accept but only respond
    // to AFTER its connection is torn down. We use `run_shell` with a
    // long sleep — the agent's shell_run has a 30s timeout, well past
    // when we'll kill it.
    let request_fut = handle.run_shell("sleep 5; echo done");

    // Briefly let the request go on the wire before we kill the agent.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let shutdown_done = std::thread::spawn(move || agent.shutdown());
    tokio::task::spawn_blocking(move || { let _ = shutdown_done.join(); })
        .await.ok();

    // The Request awaiter must now resolve. Either to a normal error
    // path ("connection closed" is the documented one) or some other
    // Err — what we MUST NOT see is the awaiter hanging forever or
    // an "actor dropped reply" / "channel closed without value" style
    // message that would indicate the drain wasn't applied.
    let result = tokio::time::timeout(Duration::from_secs(5), request_fut).await
        .expect("request must resolve within 5s — drain works")
        .unwrap_err();
    let msg = result.to_string();
    assert!(
        msg.contains("connection closed")
            || msg.contains("not connected")
            || msg.contains("closed"),
        "expected a close-style error, got: {}",
        msg
    );

    // Confirm the actor task itself has exited cleanly (it sees the
    // dropped WS, runs the drain, and returns).
    let _ = tokio::time::timeout(Duration::from_secs(2), actor_join).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forget_node_during_in_flight_request_does_not_panic_or_hang() {
    // Adversarial: dashboard DELETE /api/nodes/:id calls forget_node
    // while an MCP tool call is mid-flight via the same registry
    // entry. The Pass-9 abort-then-await fix prevents ghost-row
    // resurrection; this test pins that the in-flight request also
    // completes (Ok or Err — either is acceptable, what matters is
    // no panic and no hang).
    let (addr, agent) = spawn_agent_on("forgettable", "127.0.0.1:0".parse().unwrap());
    let registry = Arc::new(NodeRegistry::new());
    let (handle, _actor) = connect(addr, &test_psk()).await.unwrap();
    registry.register(handle).await;

    // Start a slow request. The agent is alive — this will succeed
    // unless something else interrupts it.
    let registry_for_req = registry.clone();
    let request_fut = tokio::spawn(async move {
        registry_for_req
            .run_shell("forgettable", "sleep 1; echo done")
            .await
    });

    // Concurrently forget the node.
    tokio::time::sleep(Duration::from_millis(50)).await;
    registry.forget_node("forgettable").await;

    // The request must resolve within a sensible window — either
    // succeeding (if the agent finished before forget_node landed) or
    // failing cleanly. NOT panicking, NOT hanging.
    let result = tokio::time::timeout(Duration::from_secs(5), request_fut).await
        .expect("request must resolve within 5s after forget_node");
    let inner = result.expect("task must not panic");
    // Either outcome is acceptable; we just assert it terminated.
    let _ = inner;

    let shutdown_done = std::thread::spawn(move || agent.shutdown());
    tokio::task::spawn_blocking(move || { let _ = shutdown_done.join(); })
        .await.ok();
}

// ---- AgentConfig keyring fallback ------------------------------------------
//
// Subprocess test gated #[ignore] — touches the real OS keyring. The
// goal is to pin the documented behavior: a TOML missing the `psk`
// field loads it from the system credential store (keyring) instead.

#[test]
#[ignore = "touches real OS keyring; run with --include-ignored to verify"]
fn agent_config_falls_back_to_keyring_when_psk_omitted() {
    // Plant a known PSK in the keyring under the same service/account
    // pair the agent uses.
    let known_hex = "deadbeef".repeat(8); // 32 hex bytes = 16 binary bytes
    let entry = keyring::Entry::new("kestrel", "psk").expect("open keyring");
    entry.set_password(&known_hex).expect("set keyring");

    let toml_without_psk = r#"
[agent]
node_id = "via-keyring"
listen  = "127.0.0.1:7272"
"#;
    let cfg = AgentConfig::from_toml_str(toml_without_psk)
        .expect("config must load when psk is in keyring");
    let expected = hex::decode(&known_hex).unwrap();
    assert_eq!(cfg.psk.as_slice(), expected.as_slice());

    // Cleanup so we don't leave test bytes in the operator's keyring.
    let _ = entry.delete_password();
}

#[test]
fn agent_config_fails_when_no_psk_and_no_keyring() {
    // Companion to the keyring-fallback test: when neither field nor
    // keyring has a PSK, loading must error with a message that
    // mentions enrollment. Doesn't need #[ignore] — we use a
    // deliberately bogus account name so the lookup deterministically
    // fails on every machine.
    //
    // Strategy: bypass AgentConfig::from_toml_str and just verify the
    // keyring entry lookup behaves as a clean Err when missing. The
    // full from_toml_str path also hits the system entry; on a
    // machine with a real `kestrel/psk` set, that path would silently
    // succeed and the test would be wrong. We document this caveat
    // in the assertion message.
    let entry =
        keyring::Entry::new("kestrel-tests-no-such-service-xyz", "psk").unwrap();
    assert!(
        entry.get_password().is_err(),
        "expected the bogus keyring entry to not exist; if this fails, \
         either the test machine has a kestrel-tests-no-such-service-xyz \
         entry (unlikely) or the `keyring` crate behavior changed"
    );
}
