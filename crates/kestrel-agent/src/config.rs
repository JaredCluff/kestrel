use std::net::SocketAddr;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub listen: SocketAddr,
    pub node_id: String,
    pub psk: Vec<u8>,
}

impl AgentConfig {
    pub fn new(listen: SocketAddr, node_id: String, psk: Vec<u8>) -> Self {
        AgentConfig { listen, node_id, psk }
    }

    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct Raw { agent: RawAgent }
        #[derive(Deserialize)]
        struct RawAgent { listen: String, node_id: String, psk: String }

        let raw: Raw = toml::from_str(s)?;
        Ok(AgentConfig {
            listen: raw.agent.listen.parse()?,
            node_id: raw.agent.node_id,
            psk: hex::decode(&raw.agent.psk)?,
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
    fn parse_config_from_toml() {
        let s = r#"
[agent]
listen  = "0.0.0.0:7272"
node_id = "test-node"
psk     = "deadbeefdeadbeefdeadbeefdeadbeef"
"#;
        let cfg = AgentConfig::from_str(s).unwrap();
        assert_eq!(cfg.node_id, "test-node");
        assert_eq!(cfg.listen.port(), 7272);
        assert_eq!(cfg.psk, hex::decode("deadbeefdeadbeefdeadbeefdeadbeef").unwrap());
    }
}
