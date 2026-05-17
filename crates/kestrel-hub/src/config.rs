use std::net::SocketAddr;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct HubConfig {
    pub listen_mcp: String,
    pub listen_dashboard: SocketAddr,
    pub nodes: Vec<NodeConfig>,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: String,
    pub address: SocketAddr,
}

impl HubConfig {
    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct Raw {
            hub: RawHub,
        }
        #[derive(Deserialize)]
        struct RawHub {
            listen_mcp: String,
            listen_dashboard: String,
            #[serde(default)]
            nodes: Vec<RawNode>,
        }
        #[derive(Deserialize)]
        struct RawNode {
            node_id: String,
            address: String,
        }

        let raw: Raw = toml::from_str(s)?;
        Ok(HubConfig {
            listen_mcp: raw.hub.listen_mcp,
            listen_dashboard: raw.hub.listen_dashboard.parse()?,
            nodes: raw
                .hub
                .nodes
                .into_iter()
                .map(|n| -> anyhow::Result<NodeConfig> {
                    Ok(NodeConfig {
                        node_id: n.node_id,
                        address: n.address.parse()?,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
        })
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Self::from_str(&std::fs::read_to_string(path)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hub_config() {
        let s = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"

[[hub.nodes]]
node_id = "linux-dev"
address = "192.168.1.20:7272"

[[hub.nodes]]
node_id = "mac-studio"
address = "192.168.1.10:7272"
"#;
        let cfg = HubConfig::from_str(s).unwrap();
        assert_eq!(cfg.nodes.len(), 2);
        assert_eq!(cfg.nodes[0].node_id, "linux-dev");
        assert_eq!(cfg.nodes[1].address.port(), 7272);
    }

    #[test]
    fn invalid_node_address_returns_err() {
        let s = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"

[[hub.nodes]]
node_id = "bad-node"
address = "not-a-valid-address"
"#;
        assert!(HubConfig::from_str(s).is_err());
    }
}
