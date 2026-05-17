use std::net::SocketAddr;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct HubConfig {
    pub listen_mcp: String,
    pub listen_dashboard: SocketAddr,
    pub nodes: Vec<NodeConfig>,
    pub layout: Vec<NodeLayout>,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: String,
    pub address: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct NodeLayout {
    pub node_id: String,
    pub col: i32,
    pub row: i32,
}

impl HubConfig {
    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct Raw { hub: RawHub }
        #[derive(Deserialize)]
        struct RawHub {
            listen_mcp: String,
            listen_dashboard: String,
            #[serde(default)]
            nodes: Vec<RawNode>,
            #[serde(default)]
            layout: Vec<RawLayout>,
        }
        #[derive(Deserialize)]
        struct RawNode { node_id: String, address: String }
        #[derive(Deserialize)]
        struct RawLayout { node_id: String, position: RawPosition }
        #[derive(Deserialize)]
        struct RawPosition { col: i32, row: i32 }

        let raw: Raw = toml::from_str(s)?;
        Ok(HubConfig {
            listen_mcp: raw.hub.listen_mcp,
            listen_dashboard: raw.hub.listen_dashboard.parse()?,
            nodes: raw.hub.nodes.into_iter()
                .map(|n| -> anyhow::Result<NodeConfig> {
                    Ok(NodeConfig { node_id: n.node_id, address: n.address.parse()? })
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
            layout: raw.hub.layout.into_iter().map(|l| NodeLayout {
                node_id: l.node_id,
                col: l.position.col,
                row: l.position.row,
            }).collect(),
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

[[hub.layout]]
node_id = "mac-studio"
position = { col = 0, row = 0 }

[[hub.layout]]
node_id = "linux-dev"
position = { col = 1, row = 0 }
"#;
        let cfg = HubConfig::from_str(s).unwrap();
        assert_eq!(cfg.nodes.len(), 2);
        assert_eq!(cfg.nodes[0].node_id, "linux-dev");
        assert_eq!(cfg.nodes[1].address.port(), 7272);
        assert_eq!(cfg.layout.len(), 2);
        assert_eq!(cfg.layout[0].col, 0);
        assert_eq!(cfg.layout[1].col, 1);
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
