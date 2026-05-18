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
