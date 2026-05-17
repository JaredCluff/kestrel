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
