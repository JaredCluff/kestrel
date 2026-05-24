// crates/kestrel-hub/src/sandbox_bootstrap.rs
//
// Hub-side helper for auto-installing the kestrel-agent into a freshly
// provisioned Tart VM. The Tart backend boots the VM; this module
// waits for it to acquire an IP, copies the agent binary in via scp,
// and ssh-launches it with the PSK passed via the KESTREL_PSK_HEX
// env-var (SetEnv) so the secret never touches disk on the guest.
//
// Opt-in: TartBackend reads a [sandbox.bootstrap] section from the
// hub config and skips this whole flow when absent. Operators who
// already have a custom enrollment pipeline can ignore the module.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Operator-provided bootstrap configuration. All paths are on the
/// HUB host. The agent_binary is copied byte-for-byte into the guest;
/// it must be a build whose target matches the VM's arch (Tart on
/// Apple Silicon → arm64 darwin guests; Tart on Linux Tart-port →
/// matching arch).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SandboxBootstrapConfig {
    /// Local path to the kestrel-agent binary we will scp into the VM.
    pub agent_binary: PathBuf,
    /// SSH user. Tart's stock macOS images use "admin"; a Linux-port
    /// usually uses "ubuntu" or "root".
    pub ssh_user: String,
    /// Path to the private key SSH should authenticate with. The
    /// matching public key must live in the VM image's
    /// `~/<ssh_user>/.ssh/authorized_keys`.
    pub ssh_key: PathBuf,
    /// Where the hub is listening for agent enrollments, as the agent
    /// will see it (i.e. the IP/hostname routable from inside the VM).
    pub hub_addr_for_agent: String,
    /// Hard ceiling on how long to wait for `tart ip` to start
    /// returning a non-empty address. 60s is the default.
    #[serde(default = "default_ip_wait")]
    pub ip_wait_secs: u64,
}

fn default_ip_wait() -> u64 {
    60
}

impl SandboxBootstrapConfig {
    /// Surface-level validation: refuse to start a bootstrap if any of
    /// the configured paths are missing on disk. This catches the
    /// common "operator typo'd the path" failure mode synchronously
    /// before we waste minutes booting a VM.
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.agent_binary.exists() {
            anyhow::bail!(
                "sandbox.bootstrap.agent_binary not found: {}",
                self.agent_binary.display()
            );
        }
        if !self.ssh_key.exists() {
            anyhow::bail!(
                "sandbox.bootstrap.ssh_key not found: {}",
                self.ssh_key.display()
            );
        }
        if self.ssh_user.trim().is_empty() {
            anyhow::bail!("sandbox.bootstrap.ssh_user must be non-empty");
        }
        if self.hub_addr_for_agent.trim().is_empty() {
            anyhow::bail!("sandbox.bootstrap.hub_addr_for_agent must be non-empty");
        }
        Ok(())
    }
}

/// Parse the IPv4 address from `tart ip` output. Tart prints just the
/// address on stdout when ready, or returns non-zero (with possibly
/// some noise) when the VM hasn't acquired one yet. We accept the
/// first whitespace-trimmed token that looks like a dotted-quad.
pub fn parse_tart_ip(stdout: &str) -> Option<String> {
    let first = stdout.split_whitespace().next()?;
    // Quick sanity: four numeric segments separated by dots.
    let parts: Vec<&str> = first.split('.').collect();
    if parts.len() != 4 {
        return None;
    }
    if !parts.iter().all(|p| p.parse::<u8>().is_ok()) {
        return None;
    }
    Some(first.to_string())
}

/// Poll `tart ip <vm_name>` until it returns an IP or we hit
/// `timeout`. 2s between probes. Returns the IP on success.
pub async fn wait_for_ip(vm_name: &str, timeout: Duration) -> anyhow::Result<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let out = tokio::process::Command::new("tart")
            .args(["ip", vm_name])
            .output()
            .await
            .map_err(|e| anyhow::anyhow!("tart ip {} failed to spawn: {}", vm_name, e))?;
        if out.status.success() {
            let stdout = String::from_utf8_lossy(&out.stdout);
            if let Some(ip) = parse_tart_ip(&stdout) {
                return Ok(ip);
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "tart ip {} did not return an IP within {:?}",
                vm_name,
                timeout
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Run `scp` to copy `local` into the VM at `remote_path`. Uses
/// `-o StrictHostKeyChecking=no` because fresh VMs always present
/// unknown host keys.
pub async fn scp_to_vm(
    local: &Path,
    remote_path: &str,
    user: &str,
    ip: &str,
    key: &Path,
) -> anyhow::Result<()> {
    let dest = format!("{}@{}:{}", user, ip, remote_path);
    let out = tokio::process::Command::new("scp")
        .args([
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-i",
        ])
        .arg(key)
        .arg(local)
        .arg(&dest)
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("scp failed to spawn: {}", e))?;
    if !out.status.success() {
        anyhow::bail!(
            "scp {} -> {} failed (exit {:?}): {}",
            local.display(),
            dest,
            out.status.code(),
            String::from_utf8_lossy(&out.stderr),
        );
    }
    Ok(())
}

/// Run `cmd` over ssh. `env` is forwarded via `-o SetEnv=K=V` so
/// secrets like the PSK don't need to be written to a remote file.
/// The remote sshd must permit AcceptEnv/SetEnv; OpenSSH does by
/// default for keys whitelisted via `AcceptEnv`. For PSK transport
/// SetEnv (client-side push) avoids the AcceptEnv whitelist entirely.
pub async fn ssh_run(
    user: &str,
    ip: &str,
    key: &Path,
    cmd: &str,
    env: &[(&str, &str)],
) -> anyhow::Result<std::process::Output> {
    let mut command = tokio::process::Command::new("ssh");
    command
        .args(["-o", "StrictHostKeyChecking=no"])
        .args(["-o", "UserKnownHostsFile=/dev/null"])
        .args(["-o", "BatchMode=yes"])
        .args(["-i"])
        .arg(key);
    for (k, v) in env {
        command.args(["-o", &format!("SetEnv={}={}", k, v)]);
    }
    command.arg(format!("{}@{}", user, ip)).arg(cmd);
    command
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("ssh failed to spawn: {}", e))
}

/// End-to-end bootstrap: wait for the VM to get an IP, copy the agent
/// binary in, write a minimal agent config, and start the agent. The
/// PSK is forwarded via SSH SetEnv so it lives only in env memory on
/// the guest, never on its disk.
///
/// `psk_hex` must be the per-node PSK the hub expects for `sandbox_id`
/// (HKDF-derived from master_secret + sandbox_id — same as any other
/// node).
pub async fn bootstrap(
    config: &SandboxBootstrapConfig,
    vm_name: &str,
    sandbox_id: &str,
    psk_hex: &str,
) -> anyhow::Result<String> {
    config.validate()?;
    let ip = wait_for_ip(vm_name, Duration::from_secs(config.ip_wait_secs)).await?;

    let remote_bin = "/usr/local/bin/kestrel-agent";
    scp_to_vm(&config.agent_binary, remote_bin, &config.ssh_user, &ip, &config.ssh_key).await?;

    // Make sure the binary is executable; scp preserves mode on most
    // platforms but not all, so be explicit. A non-zero exit here
    // usually means /usr/local/bin isn't writable by the configured
    // ssh_user — surface that as a real error rather than silently
    // continuing into a `start` that will fail with a confusing
    // permission-denied much later.
    let chmod = ssh_run(
        &config.ssh_user,
        &ip,
        &config.ssh_key,
        &format!("chmod +x {}", remote_bin),
        &[],
    )
    .await?;
    if !chmod.status.success() {
        anyhow::bail!(
            "chmod +x {} on {} failed (exit {:?}): {} \
             (check that the SSH user owns or can write {})",
            remote_bin,
            ip,
            chmod.status.code(),
            String::from_utf8_lossy(&chmod.stderr),
            remote_bin,
        );
    }

    // Write a minimal agent kestrel.toml. The PSK is intentionally
    // OMITTED from this file — the agent reads it from KESTREL_PSK_HEX
    // at start time (see kestrel-agent::config::load_psk_from_env_or_keyring).
    let agent_toml = format!(
        r#"[agent]
node_id = "{sandbox_id}"
listen  = "0.0.0.0:7272"
"#,
        sandbox_id = sandbox_id
    );
    let write_cmd = format!(
        "mkdir -p /etc/kestrel && cat > /etc/kestrel/kestrel.toml <<'KESTREL_EOF'\n{}KESTREL_EOF",
        agent_toml
    );
    let out = ssh_run(&config.ssh_user, &ip, &config.ssh_key, &write_cmd, &[]).await?;
    if !out.status.success() {
        anyhow::bail!(
            "write /etc/kestrel/kestrel.toml on {} failed: {}",
            ip,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // Start the agent detached. nohup + & + redirected stdio means
    // the ssh connection can close without killing the child. We don't
    // bother with systemd here — sandboxes are ephemeral by design;
    // their teardown is the hub destroying the VM, not graceful
    // shutdown of the agent.
    let start_cmd = format!(
        "nohup {} start --config /etc/kestrel/kestrel.toml >/tmp/kestrel-agent.log 2>&1 &",
        remote_bin
    );
    let out = ssh_run(
        &config.ssh_user,
        &ip,
        &config.ssh_key,
        &start_cmd,
        &[("KESTREL_PSK_HEX", psk_hex)],
    )
    .await?;
    if !out.status.success() {
        anyhow::bail!(
            "starting kestrel-agent on {} failed: {}",
            ip,
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(ip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parse_tart_ip_extracts_dotted_quad() {
        assert_eq!(
            parse_tart_ip("192.168.64.7\n"),
            Some("192.168.64.7".to_string())
        );
    }

    #[test]
    fn parse_tart_ip_handles_leading_whitespace() {
        assert_eq!(
            parse_tart_ip("  10.0.0.2  "),
            Some("10.0.0.2".to_string())
        );
    }

    #[test]
    fn parse_tart_ip_rejects_garbage() {
        assert!(parse_tart_ip("").is_none());
        assert!(parse_tart_ip("not-an-ip").is_none());
        assert!(parse_tart_ip("999.0.0.1").is_none()); // 999 > u8
        assert!(parse_tart_ip("1.2.3").is_none()); // wrong arity
    }

    #[test]
    fn validate_rejects_missing_agent_binary() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("id_rsa");
        std::fs::File::create(&key_path).unwrap().write_all(b"").unwrap();
        let cfg = SandboxBootstrapConfig {
            agent_binary: PathBuf::from("/nonexistent/path/kestrel-agent"),
            ssh_user: "admin".into(),
            ssh_key: key_path,
            hub_addr_for_agent: "10.0.0.1:7271".into(),
            ip_wait_secs: 60,
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("agent_binary not found"), "got: {}", err);
    }

    #[test]
    fn validate_rejects_missing_ssh_key() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("kestrel-agent");
        std::fs::File::create(&bin).unwrap().write_all(b"").unwrap();
        let cfg = SandboxBootstrapConfig {
            agent_binary: bin,
            ssh_user: "admin".into(),
            ssh_key: PathBuf::from("/nonexistent/id_rsa"),
            hub_addr_for_agent: "10.0.0.1:7271".into(),
            ip_wait_secs: 60,
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("ssh_key not found"), "got: {}", err);
    }

    #[test]
    fn validate_rejects_empty_strings() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("kestrel-agent");
        let key = dir.path().join("id_rsa");
        std::fs::File::create(&bin).unwrap().write_all(b"").unwrap();
        std::fs::File::create(&key).unwrap().write_all(b"").unwrap();

        let mut cfg = SandboxBootstrapConfig {
            agent_binary: bin,
            ssh_user: "  ".into(),
            ssh_key: key,
            hub_addr_for_agent: "10.0.0.1:7271".into(),
            ip_wait_secs: 60,
        };
        assert!(cfg.validate().unwrap_err().to_string().contains("ssh_user"));

        cfg.ssh_user = "admin".into();
        cfg.hub_addr_for_agent = "".into();
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("hub_addr_for_agent"));
    }

    #[test]
    fn default_ip_wait_is_sane() {
        let s = r#"
agent_binary = "/tmp/kestrel-agent"
ssh_user = "admin"
ssh_key = "/tmp/id_rsa"
hub_addr_for_agent = "10.0.0.1:7271"
"#;
        let cfg: SandboxBootstrapConfig = toml::from_str(s).unwrap();
        assert_eq!(cfg.ip_wait_secs, 60);
    }
}
