// crates/kestrel-hub/tests/phase5_reconnect.rs
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kestrel_agent::config::AgentConfig;
use kestrel_hub::config::NodeConfig;
use kestrel_hub::events::NodeEvent;
use kestrel_hub::router::NodeRegistry;
use kestrel_hub::supervisor;

/// Hub-side master secret for tests. The supervisor receives this; each
/// agent in a test receives only its own HKDF-derived per-node PSK.
fn test_master_secret() -> Vec<u8> {
    b"kestrel-test-master-32bytes-pad!".to_vec()
}

/// Per-node PSK derivation matching what the production supervisor does on
/// connect. Tests give this to the agent and the master_secret to the
/// supervisor — same as a real deployment.
fn test_psk_for(node_id: &str) -> Vec<u8> {
    kestrel_proto::derive_per_node_psk(&test_master_secret(), node_id).to_vec()
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
    let cfg = AgentConfig::new(addr, node_id.into(), test_psk_for(node_id));

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
    // Pass-1 originally protected against this scenario by overriding the
    // agent's self-reported hostname with the operator's configured id —
    // necessary because every agent shared a single fleet PSK, so the typo
    // would otherwise silently misregister an agent.
    //
    // With per-node PSKs (HKDF-derived from a hub master), this scenario
    // can no longer silently succeed: the agent's PSK is bound to its own
    // node_id, the supervisor derives the PSK from the *configured* id, the
    // two derivations differ, and the HMAC handshake fails. The test now
    // pins that STRONGER property — mismatched ids never even authenticate.
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
        test_master_secret(),
    );

    // Within the timeout, a Connected event MUST NOT arrive — auth fails,
    // the supervisor only emits Disconnected/Reconnecting.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut saw_connected = false;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(NodeEvent::Connected { .. })) => {
                saw_connected = true;
                break;
            }
            Ok(Ok(_)) => continue, // Disconnected/Reconnecting are expected
            _ => break,
        }
    }
    assert!(
        !saw_connected,
        "auth must FAIL when configured node_id != the id the agent's PSK was derived under"
    );

    // The registry must also NOT contain the agent under the agent's
    // self-reported hostname (the supervisor never registered anything).
    let snap = registry.status_snapshot().await;
    assert!(
        !snap.iter().any(|s| s.node_id == "agent-reports-this"),
        "agent-reports-this must not leak into the registry under any name"
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
    // Happy path: when the operator's configured node_id matches what the
    // agent's PSK was derived under, the supervisor's HKDF derivation
    // yields the same key the agent stored, the HMAC handshake verifies,
    // and we get a Connected event whose node_id is the configured one.
    // Companion to the mismatched-ids test above which verifies auth
    // fails when the two diverge.
    let (addr, agent) = spawn_agent_on("matching-id", "127.0.0.1:0".parse().unwrap());

    let registry = Arc::new(NodeRegistry::new());
    let mut rx = registry.subscribe();
    let _sup = supervisor::spawn(
        NodeConfig {
            node_id: "matching-id".into(), // SAME as the agent reports
            address: addr,
        },
        registry.clone(),
        test_master_secret(),
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
        // Supervisor will derive(master, "psk-rotated") for auth; the agent
        // has a completely unrelated PSK (rotated_psk()), so verification
        // fails on every retry. This pins the supervisor's behavior when
        // an agent's PSK has been rotated out of sync with the hub.
        test_master_secret(),
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
        test_master_secret(),
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
