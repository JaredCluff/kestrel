use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use zeroize::Zeroizing;

use crate::config::NodeConfig;
use crate::router::NodeRegistry;
use crate::transport;

/// Returns the next reconnect backoff duration for the given attempt number.
/// 1s → 2s → 4s → 8s → 16s → 30s (cap).
fn backoff_for(attempt: u32) -> Duration {
    let secs = 1u64 << attempt.min(5);
    Duration::from_secs(secs.min(30))
}

/// Spawn a long-lived supervisor task that keeps a single node connected.
///
/// Loop: connect → register → wait for actor exit → mark disconnected → sleep(backoff) → retry.
/// The supervisor never exits on its own; aborting the returned `JoinHandle` is the only way to stop it.
///
/// `master_secret` is the hub's HKDF input; the actual PSK used on each
/// connect is `derive_per_node_psk(master_secret, node_cfg.node_id)` so
/// rotating one node never affects another.
pub fn spawn(
    node_cfg: NodeConfig,
    registry: Arc<NodeRegistry>,
    master_secret: Zeroizing<Vec<u8>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Derive once per supervisor lifetime — node_id and master are both
        // fixed for the lifetime of this task. Reconnects use the same PSK.
        // The derived PSK is also Zeroizing so it gets wiped when the
        // supervisor task ends (or the hub process exits).
        let psk = Zeroizing::new(
            kestrel_proto::derive_per_node_psk(&master_secret, &node_cfg.node_id),
        );
        let mut attempt: u32 = 0;
        loop {
            if attempt > 0 {
                registry.mark_reconnecting(&node_cfg.node_id, attempt).await;
                tokio::time::sleep(backoff_for(attempt - 1)).await;
            } else {
                // First-ever attempt — seed status as Reconnecting so the dashboard
                // sees the node before the first connection succeeds.
                registry.mark_reconnecting(&node_cfg.node_id, 0).await;
            }

            match transport::connect_with_world_sink(node_cfg.address, &*psk).await {
                Ok((mut handle, actor_join, mut world_rx, mut caps_rx, mut webrtc_rx)) => {
                    // Verify the agent's claimed hostname matches the
                    // configured node_id. With per-node PSKs (PR #29)
                    // mismatched ids fail authentication entirely, so
                    // this branch is unreachable in practice; the
                    // override is kept as a belt-and-braces for any
                    // future scenario where the agent's claimed id
                    // could differ from the hub's configured one.
                    if handle.node_id != node_cfg.node_id {
                        tracing::warn!(
                            "supervisor: {} reports hostname '{}'; registering under configured node_id",
                            node_cfg.address,
                            handle.node_id,
                        );
                        handle.node_id = node_cfg.node_id.clone();
                    }
                    // Phase 8: record capabilities under the
                    // canonical node_id BEFORE register() so any
                    // immediate fleet_find call sees them.
                    if let Some(caps) = handle.capabilities.clone() {
                        registry
                            .record_capabilities(&node_cfg.node_id, caps)
                            .await;
                    }
                    registry.register(handle).await;

                    // Spawn a side task that forwards the agent's
                    // WorldUpdate stream into the registry's world
                    // cache. Exits when either the WS dies (world_rx
                    // closes) or the supervisor task itself is aborted.
                    let world_registry = registry.clone();
                    let world_node_id = node_cfg.node_id.clone();
                    let world_pump = tokio::spawn(async move {
                        while let Some(state) = world_rx.recv().await {
                            world_registry
                                .observe_world_update(&world_node_id, state)
                                .await;
                        }
                    });

                    // Phase 8 follow-up: pump live Capabilities frames
                    // the agent re-emits periodically into the registry.
                    // record_capabilities is idempotent for unchanged
                    // values, so re-sends are cheap.
                    let caps_registry = registry.clone();
                    let caps_node_id = node_cfg.node_id.clone();
                    let caps_pump = tokio::spawn(async move {
                        while let Some(caps) = caps_rx.recv().await {
                            caps_registry
                                .record_capabilities(&caps_node_id, caps)
                                .await;
                        }
                    });

                    // Phase 13b: pump agent-originated WebRTC events
                    // (SDP answers + ICE candidates) into the optional
                    // SessionRegistry attached to the NodeRegistry. No-op
                    // when no SessionRegistry is wired (test paths).
                    let webrtc_registry = registry.clone();
                    let webrtc_pump = tokio::spawn(async move {
                        while let Some(event) = webrtc_rx.recv().await {
                            webrtc_registry.record_webrtc_event(event).await;
                        }
                    });

                    // Wait for the actor to exit (connection closed / error).
                    let _ = actor_join.await;
                    // The world receiver drops with run_actor; the pump
                    // sees Ok(None) and exits naturally. abort() is a
                    // belt-and-braces for the unusual case where the
                    // pump hasn't woken up yet.
                    world_pump.abort();
                    caps_pump.abort();
                    webrtc_pump.abort();
                    attempt = 1;
                    registry
                        .mark_disconnected(&node_cfg.node_id, attempt, backoff_for(0))
                        .await;
                }
                Err(e) => {
                    attempt += 1;
                    let next = backoff_for(attempt - 1);
                    registry
                        .mark_disconnected(&node_cfg.node_id, attempt, next)
                        .await;
                    tracing::warn!(
                        "supervisor: connect to {} ({}) failed (attempt {}): {}",
                        node_cfg.node_id,
                        node_cfg.address,
                        attempt,
                        e
                    );
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_capped_at_30s() {
        assert_eq!(backoff_for(0), Duration::from_secs(1));
        assert_eq!(backoff_for(1), Duration::from_secs(2));
        assert_eq!(backoff_for(2), Duration::from_secs(4));
        assert_eq!(backoff_for(3), Duration::from_secs(8));
        assert_eq!(backoff_for(4), Duration::from_secs(16));
        assert_eq!(backoff_for(5), Duration::from_secs(30));
        assert_eq!(backoff_for(10), Duration::from_secs(30));
        assert_eq!(backoff_for(100), Duration::from_secs(30));
    }
}
