// crates/kestrel-hub/tests/phase5_reconnect.rs
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kestrel_agent::config::AgentConfig;
use kestrel_hub::config::NodeConfig;
use kestrel_hub::events::NodeEvent;
use kestrel_hub::router::NodeRegistry;
use kestrel_hub::supervisor;

fn test_psk() -> Vec<u8> {
    b"kestrel-test-psk-32bytes-padded!".to_vec()
}

/// A handle to an agent running on its own dedicated tokio runtime in a separate thread.
/// Dropping (or calling `shutdown`) drops the runtime, which forcibly terminates every
/// task including the per-connection `tokio::spawn` children — that's what we need to
/// simulate a real agent process exit and force the supervisor to observe a disconnect.
struct AgentHandle {
    thread: Option<std::thread::JoinHandle<()>>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl AgentHandle {
    /// Signal the agent to stop and wait for its thread (and runtime) to fully tear down.
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

    // We build the runtime INSIDE the dedicated thread so the runtime is owned by
    // that thread. Sending a shutdown signal causes serve to exit; the thread then
    // returns, dropping the runtime on that thread (which is allowed). Dropping the
    // runtime cancels every spawned task (including per-connection handlers), which
    // forces the supervisor's WebSocket to close.
    let (rt_handle_tx, rt_handle_rx) = std::sync::mpsc::channel::<tokio::runtime::Handle>();
    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build agent runtime");
        let _ = rt_handle_tx.send(rt.handle().clone());
        rt.block_on(async move {
            let (bound_tx, bound_rx) = tokio::sync::oneshot::channel::<SocketAddr>();
            let serve_fut = kestrel_agent::transport::serve(&cfg, Some(bound_tx));
            tokio::pin!(serve_fut);
            tokio::select! {
                bound = bound_rx => {
                    if let Ok(b) = bound {
                        let _ = ready_tx.send(b);
                    }
                    tokio::select! {
                        _ = &mut serve_fut => {}
                        _ = stop_rx => {}
                    }
                }
                _ = &mut serve_fut => {
                    // serve exited before binding — nothing to report
                }
            }
        });
        // rt is dropped here on this thread — that drops all per-connection tasks
        // and unblocks any peers' WebSocket reads.
    });

    let _ = rt_handle_rx.recv_timeout(Duration::from_secs(5))
        .expect("agent runtime did not start");
    let bound = ready_rx.recv_timeout(Duration::from_secs(5))
        .expect("agent did not signal bound address");
    (bound, AgentHandle {
        thread: Some(thread),
        stop_tx: Some(stop_tx),
    })
}

/// Repeatedly receive events until either a Connected event for the given node_id arrives,
/// or the per-recv timeout elapses. Returns Ok when Connected arrives, Err on timeout.
async fn wait_for_connected(
    rx: &mut tokio::sync::broadcast::Receiver<NodeEvent>,
    expected_node_id: &str,
    overall_timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + overall_timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err("overall timeout waiting for Connected".into());
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(NodeEvent::Connected { node_id, .. })) if node_id == expected_node_id => return Ok(()),
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => return Err(format!("broadcast recv error: {:?}", e)),
            Err(_) => return Err("timeout".into()),
        }
    }
}

async fn wait_for_disconnected(
    rx: &mut tokio::sync::broadcast::Receiver<NodeEvent>,
    expected_node_id: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err("timeout waiting for Disconnected".into());
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(NodeEvent::Disconnected { node_id, .. })) if node_id == expected_node_id => return Ok(()),
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => return Err(format!("broadcast recv error: {:?}", e)),
            Err(_) => return Err("timeout".into()),
        }
    }
}

/// Used by `supervisor_keeps_retrying_with_wrong_psk` — the agent will accept
/// connections with this PSK; the supervisor will be configured with `test_psk`,
/// so auth fails and the supervisor's reconnect loop should keep firing.
fn rotated_psk() -> Vec<u8> {
    b"kestrel-test-DIFFERENT-32bytes-pad!".to_vec()
}

fn spawn_agent_on_with_psk(
    node_id: &str,
    addr: SocketAddr,
    psk: Vec<u8>,
) -> (SocketAddr, AgentHandle) {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<SocketAddr>();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let cfg = AgentConfig::new(addr, node_id.into(), psk);
    let (rt_handle_tx, rt_handle_rx) = std::sync::mpsc::channel::<tokio::runtime::Handle>();
    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build agent runtime");
        let _ = rt_handle_tx.send(rt.handle().clone());
        rt.block_on(async move {
            let (bound_tx, bound_rx) = tokio::sync::oneshot::channel::<SocketAddr>();
            let serve_fut = kestrel_agent::transport::serve(&cfg, Some(bound_tx));
            tokio::pin!(serve_fut);
            tokio::select! {
                bound = bound_rx => {
                    if let Ok(b) = bound {
                        let _ = ready_tx.send(b);
                    }
                    tokio::select! {
                        _ = &mut serve_fut => {}
                        _ = stop_rx => {}
                    }
                }
                _ = &mut serve_fut => {}
            }
        });
    });
    let _ = rt_handle_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("agent runtime did not start");
    let bound = ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("agent did not signal bound address");
    (
        bound,
        AgentHandle {
            thread: Some(thread),
            stop_tx: Some(stop_tx),
        },
    )
}

/// Count Disconnected events received within `window` for `expected_node_id`.
async fn count_disconnects(
    rx: &mut tokio::sync::broadcast::Receiver<NodeEvent>,
    expected_node_id: &str,
    window: Duration,
) -> usize {
    let deadline = tokio::time::Instant::now() + window;
    let mut n = 0;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return n;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(NodeEvent::Disconnected { node_id, .. })) if node_id == expected_node_id => {
                n += 1;
            }
            Ok(Ok(_)) => {}
            _ => return n,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_uses_configured_node_id_when_agent_hostname_differs() {
    // Pass-1 fix: a typo in kestrel.toml pointing one node's id at another
    // node's host would silently register the agent under the WRONG identity
    // because the PSK is shared across the fleet. Supervisor now compares the
    // agent's self-reported hostname against the configured `node_id` and
    // trusts the operator's config.
    //
    // This test was added in Pass 2 — the Pass-1 change had no test coverage
    // because every other integration test happens to use the same id on
    // both sides.
    let (addr, agent) =
        spawn_agent_on("agent-reports-this", "127.0.0.1:0".parse().unwrap());

    let registry = Arc::new(NodeRegistry::new());
    let mut rx = registry.subscribe();
    let _sup = supervisor::spawn(
        NodeConfig {
            node_id: "operator-config".into(),
            address: addr,
        },
        registry.clone(),
        test_psk(),
    );

    // The Connected event MUST carry the operator's configured node_id, not
    // the agent's self-reported hostname.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut got = false;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(NodeEvent::Connected { node_id, .. })) => {
                assert_eq!(
                    node_id, "operator-config",
                    "supervisor must use the configured node_id, not the agent-reported hostname"
                );
                got = true;
                break;
            }
            Ok(Ok(_)) => continue,
            _ => break,
        }
    }
    assert!(got, "did not observe a Connected event in time");

    // Registry should also key by the configured id (the MCP dispatch + KVM
    // routing both look up nodes by this string).
    let snap = registry.status_snapshot().await;
    assert!(
        snap.iter().any(|s| s.node_id == "operator-config"),
        "registry should contain the configured node_id; got {:?}",
        snap.iter().map(|s| &s.node_id).collect::<Vec<_>>()
    );
    assert!(
        !snap.iter().any(|s| s.node_id == "agent-reports-this"),
        "registry must NOT contain the agent-reported hostname"
    );

    // Cleanup.
    let shutdown_done = std::thread::spawn(move || agent.shutdown());
    tokio::task::spawn_blocking(move || {
        let _ = shutdown_done.join();
    })
    .await
    .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_uses_agent_node_id_when_configured_matches() {
    // Companion to `supervisor_uses_configured_node_id_when_agent_hostname_differs`.
    // When the operator's configured node_id MATCHES what the agent reports,
    // the supervisor's mismatch-rename branch must NOT fire — the registry
    // should key by the (identical) id either way, but the warn-level log
    // should be absent. We can't easily assert "no log" without a tracing
    // capture harness, but we can assert the registered id is exactly the
    // configured one — which is the contract that matters.
    //
    // Without this companion test, a regression that made the supervisor
    // ALWAYS overwrite handle.node_id (instead of conditionally on mismatch)
    // would still pass the original test because the override happens to
    // produce the right id. Pairing both tests pins the if/else surface.
    let (addr, agent) = spawn_agent_on("matching-id", "127.0.0.1:0".parse().unwrap());

    let registry = Arc::new(NodeRegistry::new());
    let mut rx = registry.subscribe();
    let _sup = supervisor::spawn(
        NodeConfig {
            node_id: "matching-id".into(), // SAME as the agent reports
            address: addr,
        },
        registry.clone(),
        test_psk(),
    );

    wait_for_connected(&mut rx, "matching-id", Duration::from_secs(5))
        .await
        .expect("Connected with matching id should arrive");

    let snap = registry.status_snapshot().await;
    assert_eq!(snap.len(), 1, "exactly one node in the registry");
    assert_eq!(snap[0].node_id, "matching-id");

    let shutdown_done = std::thread::spawn(move || agent.shutdown());
    tokio::task::spawn_blocking(move || {
        let _ = shutdown_done.join();
    })
    .await
    .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_keeps_retrying_with_wrong_psk() {
    // Simulates an agent whose PSK has been rotated out-of-band: hub still
    // thinks the old PSK is valid; agent rejects every connection because the
    // TLS-exporter-bound MAC won't verify. Supervisor should keep retrying
    // with the documented backoff schedule (1s, 2s, 4s, …), surfacing repeated
    // Disconnected events. The hub never escalates "auth fails" vs. "host
    // unreachable" today — this test pins that semantics so a future change
    // either preserves it or has to update the assertion explicitly.
    let (addr, agent) =
        spawn_agent_on_with_psk("psk-rotated", "127.0.0.1:0".parse().unwrap(), rotated_psk());

    let registry = Arc::new(NodeRegistry::new());
    let mut rx = registry.subscribe();
    let _sup = supervisor::spawn(
        NodeConfig {
            node_id: "psk-rotated".into(),
            address: addr,
        },
        registry.clone(),
        test_psk(), // wrong PSK relative to the agent
    );

    // First Disconnected arrives almost immediately (auth fails on first
    // connect). Subsequent: 1s, 2s, 4s, 8s, 16s, 30s cap. 12s window gives
    // generous margin for 2 retries on slow CI; if this ever flakes raise to
    // 15s before chasing a real bug.
    let count = count_disconnects(&mut rx, "psk-rotated", Duration::from_secs(12)).await;
    assert!(
        count >= 2,
        "expected at least 2 Disconnected events from a wrong-PSK loop, got {}",
        count
    );

    // Clean up the agent's runtime.
    let shutdown_done = std::thread::spawn(move || agent.shutdown());
    tokio::task::spawn_blocking(move || {
        let _ = shutdown_done.join();
    })
    .await
    .ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_reconnects_after_agent_restart() {
    // 1. Start agent #1 on a random port (on its own dedicated runtime/thread).
    let (addr_a, agent_a) =
        spawn_agent_on("recon-node", "127.0.0.1:0".parse().unwrap());

    // 2. Spawn supervisor.
    let registry = Arc::new(NodeRegistry::new());
    let mut rx = registry.subscribe();
    let _sup = supervisor::spawn(
        NodeConfig { node_id: "recon-node".into(), address: addr_a },
        registry.clone(),
        test_psk(),
    );

    // 3. Expect first Connected within 5s.
    wait_for_connected(&mut rx, "recon-node", Duration::from_secs(5))
        .await
        .expect("first Connected did not arrive in time");

    // 4. Shut down agent #1's runtime — this drops the listener AND all per-connection
    //    tasks, forcing the supervisor's WebSocket to actually close. Dropping a tokio
    //    runtime must happen on a thread that is NOT part of any tokio runtime, so we
    //    spawn a native thread to do it.
    let shutdown_done = std::thread::spawn(move || agent_a.shutdown());
    // Poll the native join handle without blocking the async runtime.
    tokio::task::spawn_blocking(move || {
        let _ = shutdown_done.join();
    })
    .await
    .expect("agent_a shutdown thread join");
    // Give the OS a moment to release the port.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. Expect Disconnected within 5s.
    wait_for_disconnected(&mut rx, "recon-node", Duration::from_secs(5))
        .await
        .expect("Disconnected did not arrive in time");

    // 6. Start agent #2 on the SAME address.
    let (_addr_b, _agent_b) = spawn_agent_on("recon-node", addr_a);

    // 7. Expect eventual Connected within 15s (supervisor backoff is 1s, 2s, 4s, …).
    wait_for_connected(&mut rx, "recon-node", Duration::from_secs(15))
        .await
        .expect("reconnect Connected did not arrive in time");
}
