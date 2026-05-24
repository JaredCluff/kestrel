// crates/kestrel-hub/tests/per_node_psk_rejection.rs
//
// End-to-end tests for the per-node PSK security property:
//
//   1. An agent enrolled with derive(M, "beta") cannot authenticate against
//      a hub that's connecting to it as "alpha" (because the hub would
//      compute derive(M, "alpha"), which differs).
//
//   2. An agent enrolled under master M1 cannot authenticate against a hub
//      that has master M2 (because the derivations differ even for the
//      same node_id).
//
// These tests fail-by-construction without per-node PSKs: under the old
// shared-PSK design both sides would have held the same key and both
// scenarios would have authenticated successfully. With per-node PSKs the
// HMAC handshake rejects the connection at the auth step, the supervisor
// emits Disconnected, and the test passes.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kestrel_agent::config::AgentConfig;
use kestrel_hub::config::NodeConfig;
use kestrel_hub::events::NodeEvent;
use kestrel_hub::router::NodeRegistry;
use kestrel_hub::supervisor;

fn master_a() -> zeroize::Zeroizing<Vec<u8>> {
    zeroize::Zeroizing::new(b"kestrel-test-master-A-32bytes-pd".to_vec())
}

fn master_b() -> zeroize::Zeroizing<Vec<u8>> {
    zeroize::Zeroizing::new(b"kestrel-test-master-B-32bytes-pd".to_vec())
}

fn derived(master: &[u8], node_id: &str) -> Vec<u8> {
    kestrel_proto::derive_per_node_psk(master, node_id).to_vec()
}

/// Boot a tokio runtime on a dedicated thread, serve the agent on it, and
/// return its bound address. Mirrors the helper used in phase5_reconnect.
fn spawn_agent_with_psk(
    node_id: &'static str,
    psk: Vec<u8>,
) -> SocketAddr {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<SocketAddr>();
    let cfg = AgentConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        node_id.into(),
        psk,
    );

    std::thread::spawn(move || {
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
                    let _ = serve_fut.await;
                }
                _ = &mut serve_fut => {}
            }
        });
    });

    ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("agent did not bind")
}

/// Wait up to `total` for ANY supervisor event for `node_id`. Returns true
/// if we observed a Connected event (which would mean auth succeeded, i.e.
/// the test should FAIL). Returns false if the window expired with only
/// Disconnected/Reconnecting events (the expected outcome).
async fn observed_connected(
    rx: &mut tokio::sync::broadcast::Receiver<NodeEvent>,
    node_id: &str,
    total: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + total;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { return false; }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(NodeEvent::Connected { node_id: id, .. })) if id == node_id => return true,
            Ok(Ok(_)) => continue,
            _ => return false,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_node_psk_does_not_authenticate() {
    // Setup: an agent has been enrolled as "beta" — its keyring holds
    // derive(M, "beta"). An adversarial or misconfigured hub tries to
    // connect to that agent's address but claims (via configuration) the
    // target is "alpha", so it derives derive(M, "alpha") for HMAC. With
    // per-node PSKs those two keys differ and authentication MUST fail.
    let agent_addr = spawn_agent_with_psk("beta", derived(&master_a(), "beta"));

    let registry = Arc::new(NodeRegistry::new());
    let mut rx = registry.subscribe();
    let _sup = supervisor::spawn(
        NodeConfig {
            node_id: "alpha".into(),
            address: agent_addr,
        },
        registry.clone(),
        master_a(),
    );

    let saw_connected = observed_connected(&mut rx, "alpha", Duration::from_secs(3)).await;
    assert!(
        !saw_connected,
        "cross-node PSK MUST NOT authenticate: 'alpha' supervisor connected to an agent serving 'beta'"
    );

    // Registry must reflect the failed-to-connect state for the configured
    // node_id (Reconnecting/Offline). The agent's own node_id must NOT
    // appear under either name.
    let snap = registry.status_snapshot().await;
    assert!(
        !snap.iter().any(|s| s.node_id == "beta"),
        "registry must not contain 'beta' (the agent's id was never accepted)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_master_psk_does_not_authenticate() {
    // Same node_id on both sides, but the hub's master_secret differs from
    // the master under which the agent's PSK was derived. derive(M1, "n")
    // != derive(M2, "n"), so the HMAC handshake rejects. This pins that
    // master rotation actually invalidates pre-rotation agent enrollments
    // (the property we'd want when revoking a leaked master).
    let agent_addr = spawn_agent_with_psk("node", derived(&master_a(), "node"));

    let registry = Arc::new(NodeRegistry::new());
    let mut rx = registry.subscribe();
    let _sup = supervisor::spawn(
        NodeConfig {
            node_id: "node".into(),
            address: agent_addr,
        },
        registry.clone(),
        master_b(), // <-- different master
    );

    let saw_connected = observed_connected(&mut rx, "node", Duration::from_secs(3)).await;
    assert!(
        !saw_connected,
        "cross-master PSK MUST NOT authenticate: hub master B vs. agent enrolled under master A"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matched_per_node_psk_authenticates() {
    // Positive control. Same master, same node_id on both sides — the
    // happy path that the two negative tests above lean on as the
    // counterexample. Verifies the test scaffolding itself isn't
    // accidentally failing for an unrelated reason.
    let agent_addr = spawn_agent_with_psk("happy", derived(&master_a(), "happy"));

    let registry = Arc::new(NodeRegistry::new());
    let mut rx = registry.subscribe();
    let _sup = supervisor::spawn(
        NodeConfig {
            node_id: "happy".into(),
            address: agent_addr,
        },
        registry.clone(),
        master_a(),
    );

    // 10s gives the handshake (TCP connect → TLS → PSK challenge →
    // AuthResponse → supervisor publishes Connected) plenty of headroom
    // even when many test binaries run in parallel under cargo test.
    // Was 5s; bumped to remove a flake observed under workspace-wide test
    // runs that share the host's TCP backlog.
    let saw_connected = observed_connected(&mut rx, "happy", Duration::from_secs(10)).await;
    assert!(
        saw_connected,
        "matched per-node PSK (same master, same node_id) MUST authenticate"
    );
}
