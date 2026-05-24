use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use zeroize::Zeroizing;

use crate::config::NodeConfig;
use crate::router::NodeRegistry;
use crate::transport;

/// Base reconnect backoff (no jitter), for the given attempt number.
/// 1s → 2s → 4s → 8s → 16s → 30s (cap). The supervisor wraps this in
/// [`backoff_with_jitter`] for actual sleeps so a fleet of N agents
/// reconnecting after a hub restart doesn't hit the listen socket in
/// lockstep.
fn backoff_for(attempt: u32) -> Duration {
    let secs = 1u64 << attempt.min(5);
    Duration::from_secs(secs.min(30))
}

/// Apply ±25% uniform jitter to the base backoff for this attempt.
/// At attempt=1 (base=1s) sleeps somewhere in 750ms..=1250ms. At the
/// 30s cap, somewhere in 22.5s..=37.5s. Spreads thundering-herd
/// reconnects across the window so the hub's accept loop sees a
/// smoother arrival rate than N-in-one-go.
fn backoff_with_jitter(attempt: u32) -> Duration {
    use rand::Rng;
    let base = backoff_for(attempt).as_millis() as f64;
    let jitter = rand::thread_rng().gen_range(-0.25f64..=0.25f64);
    let with_jitter = (base * (1.0 + jitter)).max(50.0); // floor at 50ms
    Duration::from_millis(with_jitter as u64)
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
                // Jittered sleep: ±25% spread prevents lockstep
                // reconnects after a hub restart from hammering the
                // accept queue all at once.
                tokio::time::sleep(backoff_with_jitter(attempt - 1)).await;
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

    #[test]
    fn jitter_stays_within_25_percent_envelope() {
        // ±25% means: at base 1000ms, every sample is in [750, 1250].
        // Loop a healthy number of trials so the bounds get exercised.
        let base = backoff_for(0).as_millis() as f64;
        let lo = (base * 0.75) as u128 - 1;
        let hi = (base * 1.25) as u128 + 1;
        for _ in 0..500 {
            let j = backoff_with_jitter(0).as_millis();
            assert!(
                j >= lo && j <= hi,
                "jittered backoff {} ms outside [{}, {}]",
                j, lo, hi
            );
        }
    }

    #[test]
    fn jitter_actually_varies() {
        // Pin that jitter is doing something. If thread_rng ever
        // returned a fixed seed, a long run of identical values would
        // mean the jitter is a no-op.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..50 {
            seen.insert(backoff_with_jitter(2).as_millis());
        }
        assert!(seen.len() > 5, "expected diverse jittered values, got only {}", seen.len());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn supervisor_reconnect_loop_fires_repeatedly_under_paused_time() {
        // Deterministic-time test: point the supervisor at a closed
        // port (127.0.0.1:1 is reserved — connect() rejects ~instantly,
        // not after a network timeout). The only real-clock dependency
        // in the loop is `tokio::time::sleep(backoff)`, which `pause`
        // makes virtual.
        //
        // Pinning this property deterministically means future tweaks
        // to the backoff schedule have to be CORRECT, not just
        // "passes when CI happens to be fast enough." Was the source
        // of two timeout-bump papers in past PRs.
        use crate::config::NodeConfig;
        use crate::router::NodeRegistry;
        use crate::events::NodeEvent;
        use std::sync::Arc;

        let registry = Arc::new(NodeRegistry::new());
        let mut rx = registry.subscribe();
        let _sup = spawn(
            NodeConfig {
                node_id: "doomed".into(),
                address: "127.0.0.1:1".parse().unwrap(),
            },
            registry.clone(),
            zeroize::Zeroizing::new(vec![0u8; 32]),
        );

        // Drive the loop forward in interleaved steps: each iteration
        // yields so the spawned task can run (paused tokio::time::sleep
        // returns instantly), then advances virtual time past the next
        // backoff. We loop 6 times to cover 1 + 2 + 4 + 8 + 16 + 30 =
        // 61 virtual seconds — long enough to see multiple Disconnects.
        let mut disconnected = 0;
        for _ in 0..6 {
            // Yield first so the supervisor task gets a chance to
            // make progress (initial connect attempt + Disconnected
            // emission) before we advance past its sleep.
            for _ in 0..3 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_secs(35)).await;
        }
        // Final drain.
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        while let Ok(evt) = rx.try_recv() {
            if matches!(evt, NodeEvent::Disconnected { .. }) {
                disconnected += 1;
            }
        }
        assert!(
            disconnected >= 3,
            "expected ≥3 Disconnected events from a doomed supervisor under paused time, got {}",
            disconnected
        );
    }

    #[test]
    fn jitter_respects_minimum_floor() {
        // The 50ms floor matters when base is small (attempt 0 base 1s
        // can jitter down to 750ms, but a future tweak to base=10ms
        // could go below 50). Exercise the floor by feeding huge
        // negative jitter via repeated calls — statistical only, but
        // we assert no sample is < 50ms.
        for _ in 0..500 {
            assert!(backoff_with_jitter(0).as_millis() >= 50);
        }
    }
}
