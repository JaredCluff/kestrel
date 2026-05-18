// crates/kestrel-hub/src/status.rs
use std::time::Duration;

use crate::config::NodeConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeProbeResult {
    Reachable { rtt: Duration },
    Unreachable { reason: String },
}

#[derive(Debug, Clone)]
pub struct NodeProbe {
    pub node_id: String,
    pub address: std::net::SocketAddr,
    pub result: NodeProbeResult,
}

/// Probe one node by performing a single TLS handshake + Ping/Pong roundtrip,
/// then dropping the connection.
pub async fn probe_node(node: &NodeConfig, psk: &[u8], timeout: Duration) -> NodeProbe {
    let attempt = tokio::time::timeout(
        timeout,
        crate::transport::ping_once(node.address, psk),
    )
    .await;
    let result = match attempt {
        Ok(Ok(rtt)) => NodeProbeResult::Reachable { rtt },
        Ok(Err(e)) => NodeProbeResult::Unreachable { reason: e.to_string() },
        Err(_) => NodeProbeResult::Unreachable { reason: format!("timeout after {:?}", timeout) },
    };
    NodeProbe {
        node_id: node.node_id.clone(),
        address: node.address,
        result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn probe_unreachable_address_reports_unreachable() {
        let node = NodeConfig {
            node_id: "ghost".into(),
            address: "127.0.0.1:1".parse().unwrap(),  // port 1 is reserved and won't accept
        };
        let probe = probe_node(&node, b"any-psk-32-bytes-padded-padded!!", Duration::from_secs(2)).await;
        assert_eq!(probe.node_id, "ghost");
        assert!(matches!(probe.result, NodeProbeResult::Unreachable { .. }), "got: {:?}", probe.result);
    }
}
