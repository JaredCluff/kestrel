use std::net::SocketAddr;
use serde::Deserialize;
use zeroize::Zeroizing;

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub listen: SocketAddr,
    pub node_id: String,
    /// The agent's per-node PSK. Wrapped in `Zeroizing` so the underlying
    /// memory is wiped when the config (or any clone) drops. Defense-in-
    /// depth against process-memory dumps and accidental swap-out.
    pub psk: Zeroizing<Vec<u8>>,
}

impl AgentConfig {
    pub fn new(listen: SocketAddr, node_id: String, psk: impl Into<Zeroizing<Vec<u8>>>) -> Self {
        AgentConfig { listen, node_id, psk: psk.into() }
    }

    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct Raw { agent: RawAgent }
        #[derive(Deserialize)]
        struct RawAgent {
            listen: String,
            node_id: String,
            #[serde(default)]
            psk: Option<String>,
        }

        let raw: Raw = toml::from_str(s)?;
        let psk = match raw.agent.psk {
            Some(hex_str) => Zeroizing::new(hex::decode(&hex_str)?),
            None => load_psk_from_keyring()?,
        };
        Ok(AgentConfig {
            listen: raw.agent.listen.parse()?,
            node_id: raw.agent.node_id,
            psk,
        })
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Self::from_str(&std::fs::read_to_string(path)?)
    }
}

/// Read the PSK from the system credential store (where `kestrel-agent
/// enroll` puts it). Returns the bytes wrapped in `Zeroizing` so they're
/// wiped from memory when the binding drops.
fn load_psk_from_keyring() -> anyhow::Result<Zeroizing<Vec<u8>>> {
    let entry = keyring::Entry::new("kestrel", "psk")
        .map_err(|e| anyhow::anyhow!("open keyring entry: {}", e))?;
    let hex_str = entry.get_password().map_err(|e| {
        anyhow::anyhow!(
            "no `agent.psk` in config and no PSK in keyring: {} (run `kestrel-agent enroll` first)",
            e
        )
    })?;
    Ok(Zeroizing::new(
        hex::decode(&hex_str).map_err(|e| anyhow::anyhow!("keyring PSK is not valid hex: {}", e))?
    ))
}

/// Write a starter agent kestrel.toml at `path`. Refuses to overwrite.
pub fn scaffold_agent_config(path: &str, node_id: &str, listen: &str) -> anyhow::Result<()> {
    if std::path::Path::new(path).exists() {
        anyhow::bail!("{} already exists", path);
    }
    let contents = format!(
        r#"# Kestrel agent configuration. The PSK lives in your system credential store
# (put there by `kestrel-agent enroll`) and is loaded automatically at startup.
# To pin a specific PSK here instead, uncomment the `psk = "..."` line below.

[agent]
node_id = "{node_id}"
listen  = "{listen}"
# psk   = "<64 hex chars>"
"#,
        node_id = node_id,
        listen = listen,
    );
    std::fs::write(path, contents)
        .map_err(|e| anyhow::anyhow!("write {}: {}", path, e))
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
        // Zeroizing<Vec<u8>> derefs to &[u8] for comparison.
        assert_eq!(
            cfg.psk.as_slice(),
            hex::decode("deadbeefdeadbeefdeadbeefdeadbeef").unwrap().as_slice()
        );
    }

    #[test]
    fn scaffold_agent_config_writes_valid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kestrel.toml");
        let path_str = path.to_str().unwrap();
        super::scaffold_agent_config(path_str, "macstudio", "0.0.0.0:7272").unwrap();
        let contents = std::fs::read_to_string(path_str).unwrap();
        assert!(contents.contains("node_id"));
        assert!(contents.contains("macstudio"));
        assert!(contents.contains("0.0.0.0:7272"));
    }

    #[test]
    fn scaffold_agent_config_refuses_to_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kestrel.toml");
        let path_str = path.to_str().unwrap();
        std::fs::write(path_str, "existing").unwrap();
        let err = super::scaffold_agent_config(path_str, "x", "0.0.0.0:7272").unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }
}
