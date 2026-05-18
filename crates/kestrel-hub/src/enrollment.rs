use rand::RngCore;

pub fn generate_psk() -> Vec<u8> {
    let mut key = vec![0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    key
}

pub fn enrollment_command(hub_ip: &str, psk: &[u8]) -> String {
    format!(
        "kestrel-agent enroll --hub {} --key {}",
        hub_ip,
        hex::encode(psk)
    )
}

pub fn store_psk(psk: &[u8]) -> anyhow::Result<()> {
    let entry = keyring::Entry::new("kestrel", "psk")?;
    entry.set_password(&hex::encode(psk))?;
    Ok(())
}

pub fn load_psk() -> anyhow::Result<Vec<u8>> {
    let entry = keyring::Entry::new("kestrel", "psk")?;
    Ok(hex::decode(entry.get_password()?)?)
}

/// Write a starter hub kestrel.toml at `path`. Refuses to overwrite if a file
/// already exists at that path.
pub fn scaffold_hub_config(path: &str, dashboard_addr: &str) -> anyhow::Result<()> {
    if std::path::Path::new(path).exists() {
        anyhow::bail!("{} already exists", path);
    }
    let contents = format!(
        r#"# Kestrel hub configuration. Edit by hand or use `kestrel-hub` subcommands
# (add-node, remove-node, layout) to mutate it programmatically.

[hub]
listen_mcp       = "stdio"
listen_dashboard = "{dashboard_addr}"

# Nodes the hub connects to. Add with `kestrel-hub add-node <id> <addr>`.

# Optional KVM layout for cursor-edge routing. Add with `kestrel-hub layout set <id> <col> <row>`.
"#,
        dashboard_addr = dashboard_addr,
    );
    std::fs::write(path, contents)
        .map_err(|e| anyhow::anyhow!("write {}: {}", path, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_psk_is_32_bytes() {
        let key = generate_psk();
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn enrollment_command_contains_required_parts() {
        let key = vec![0u8; 32];
        let cmd = enrollment_command("192.168.1.10", &key);
        assert!(cmd.contains("kestrel-agent enroll"));
        assert!(cmd.contains("192.168.1.10"));
        assert!(cmd.contains("--key"));
    }

    #[test]
    fn enrollment_command_hex_encoding() {
        let key = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
        let cmd = enrollment_command("10.0.0.1", &key);
        assert!(cmd.contains("deadbeef"));
    }
}

#[cfg(test)]
mod scaffold_tests {
    use super::*;

    #[test]
    fn scaffold_hub_config_writes_valid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kestrel.toml");
        let path_str = path.to_str().unwrap();
        scaffold_hub_config(path_str, "0.0.0.0:7273").unwrap();
        let contents = std::fs::read_to_string(path_str).unwrap();
        assert!(contents.contains("listen_mcp"));
        assert!(contents.contains("0.0.0.0:7273"));
        // Round-trip through HubConfig::from_str to ensure validity.
        crate::config::HubConfig::from_str(&contents).unwrap();
    }

    #[test]
    fn scaffold_hub_config_refuses_to_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kestrel.toml");
        let path_str = path.to_str().unwrap();
        std::fs::write(path_str, "existing").unwrap();
        let err = scaffold_hub_config(path_str, "0.0.0.0:7273").unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }
}
