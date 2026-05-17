use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

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
pub fn spawn(
    node_cfg: NodeConfig,
    registry: Arc<NodeRegistry>,
    psk: Vec<u8>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
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

            match transport::connect(node_cfg.address, &psk).await {
                Ok((handle, actor_join)) => {
                    registry.register(handle).await;
                    attempt = 0;
                    // Wait for the actor to exit (connection closed / error).
                    let _ = actor_join.await;
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
