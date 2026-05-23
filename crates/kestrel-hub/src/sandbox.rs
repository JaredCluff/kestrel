// crates/kestrel-hub/src/sandbox.rs
//
// Phase 10: ephemeral sandbox provisioning. New MCP tool
// `sandbox_spawn(image, ttl_secs)` provisions a fresh VM, installs the
// agent, registers it with the hub, returns a node_id; the operator
// (or auto-teardown) calls `sandbox_destroy(node_id)` to tear it down.
//
// Backends are platform-specific:
//   - macOS: Tart (https://tart.run) — fast VM lifecycle for ARM Macs.
//   - Linux: Lima or QEMU — defer to a follow-up implementation.
//   - Windows: Hyper-V quick-create — defer.
//   - Cloud: a pluggable provider trait — defer.
//
// CAVEAT: this module ships the abstractions + Tart-on-macOS skeleton.
// Each backend's actual subprocess calls are TODO comments — they
// require live testing on the target platform which we can't do
// from the dev machine driving this PR. The shape is reviewable; the
// bodies need follow-up work + integration tests on real
// virtualization hardware.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

/// Identifier for a spawned sandbox. Equal to the agent's node_id
/// the hub assigns at registration time. Shape "sb-" + 12 hex chars.
pub type SandboxId = String;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Sandbox {
    pub id: SandboxId,
    pub backend: String,
    pub image: String,
    pub created_unix: u64,
    pub expires_unix: u64,
    pub status: SandboxStatus,
    /// Optional human-readable hint about where the VM lives
    /// (Tart VM name, Lima instance name, cloud VM id). Useful for
    /// operator forensics when the auto-teardown fails.
    pub backend_handle: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxStatus {
    Provisioning,
    Ready,
    Destroyed,
    Failed,
}

/// In-memory registry of sandboxes. Auto-teardown happens via a
/// reaper task that wakes up periodically and destroys any expired
/// sandboxes. The registry does NOT persist across hub restarts;
/// operators rerunning the hub will need to manually clean up any
/// lingering VMs via the backend's CLI (`tart delete`, `limactl
/// delete`).
#[derive(Clone)]
pub struct SandboxRegistry {
    inner: Arc<RwLock<HashMap<SandboxId, Sandbox>>>,
}

impl Default for SandboxRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SandboxRegistry {
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Spawn a sandbox using the appropriate backend for the host.
    /// Returns the SandboxId on success (sandbox is in Provisioning;
    /// the caller polls via `get` to know when it transitions to
    /// Ready). On backend failure returns Err; nothing left allocated.
    pub async fn spawn(&self, image: &str, ttl_secs: u64) -> anyhow::Result<SandboxId> {
        let id = fresh_sandbox_id();
        let now = now_unix();
        let backend = pick_backend();
        let entry = Sandbox {
            id: id.clone(),
            backend: backend.name().into(),
            image: image.into(),
            created_unix: now,
            expires_unix: now + ttl_secs,
            status: SandboxStatus::Provisioning,
            backend_handle: None,
        };
        {
            let mut map = self.inner.write().await;
            map.insert(id.clone(), entry);
        }
        // Spawn the provisioning task. Updates the entry's status
        // and backend_handle when done. v1 here is a stub that
        // immediately fails — real implementations replace this with
        // the backend's actual subprocess calls.
        let inner = self.inner.clone();
        let id_for_task = id.clone();
        let image_for_task = image.to_string();
        tokio::spawn(async move {
            let result = backend.provision(&id_for_task, &image_for_task).await;
            let mut map = inner.write().await;
            if let Some(entry) = map.get_mut(&id_for_task) {
                match result {
                    Ok(handle) => {
                        entry.backend_handle = Some(handle);
                        entry.status = SandboxStatus::Ready;
                    }
                    Err(_e) => {
                        entry.status = SandboxStatus::Failed;
                    }
                }
            }
        });
        Ok(id)
    }

    pub async fn get(&self, id: &str) -> Option<Sandbox> {
        self.inner.read().await.get(id).cloned()
    }

    pub async fn list(&self) -> Vec<Sandbox> {
        let mut v: Vec<Sandbox> = self.inner.read().await.values().cloned().collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v
    }

    /// Destroy a sandbox immediately. Best-effort: backend errors are
    /// logged but the registry still marks the entry Destroyed so
    /// auto-teardown and operator listings don't keep retrying.
    pub async fn destroy(&self, id: &str) -> bool {
        let sandbox = {
            let map = self.inner.read().await;
            map.get(id).cloned()
        };
        let Some(s) = sandbox else { return false };
        if let Err(e) = pick_backend().teardown(&s).await {
            tracing::warn!("sandbox teardown failed for {}: {}", id, e);
        }
        let mut map = self.inner.write().await;
        if let Some(entry) = map.get_mut(id) {
            entry.status = SandboxStatus::Destroyed;
        }
        true
    }

    /// Reaper loop. Spawn once at hub startup. Wakes every 60s, tears
    /// down anything past its expires_unix. Idempotent — already-
    /// destroyed sandboxes are skipped.
    pub fn spawn_reaper(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                let now = now_unix();
                let expired_ids: Vec<String> = {
                    let map = self.inner.read().await;
                    map.values()
                        .filter(|s| {
                            s.status != SandboxStatus::Destroyed && s.expires_unix <= now
                        })
                        .map(|s| s.id.clone())
                        .collect()
                };
                for id in expired_ids {
                    let _ = self.destroy(&id).await;
                }
            }
        })
    }
}

fn fresh_sandbox_id() -> SandboxId {
    use rand::RngCore;
    let mut bytes = [0u8; 6];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("sb-{}", hex::encode(bytes))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ── Backend trait + dispatch ─────────────────────────────────────────────

/// One sandbox backend. Picked at runtime via `pick_backend()` based
/// on the host OS.
#[async_trait::async_trait]
trait Backend: Send + Sync {
    fn name(&self) -> &'static str;
    /// Provision a fresh VM. Returns a backend-specific handle string
    /// (e.g. Tart VM name) on success.
    async fn provision(&self, sandbox_id: &str, image: &str) -> anyhow::Result<String>;
    async fn teardown(&self, sandbox: &Sandbox) -> anyhow::Result<()>;
}

fn pick_backend() -> Box<dyn Backend> {
    #[cfg(target_os = "macos")]
    {
        Box::new(TartBackend)
    }
    #[cfg(target_os = "linux")]
    {
        Box::new(LimaBackend)
    }
    #[cfg(target_os = "windows")]
    {
        Box::new(HyperVBackend)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        Box::new(UnsupportedBackend)
    }
}

// ── Stub implementations (real provisioning is follow-up work) ──────────

#[cfg(target_os = "macos")]
struct TartBackend;
#[cfg(target_os = "macos")]
#[async_trait::async_trait]
impl Backend for TartBackend {
    fn name(&self) -> &'static str { "tart" }
    async fn provision(&self, sandbox_id: &str, image: &str) -> anyhow::Result<String> {
        let vm_name = format!("kestrel-{}", sandbox_id);
        // `tart clone <image> <vm-name>` clones a registered Tart
        // image into a new VM. The image must be available locally
        // (operator runs `tart pull` ahead of time). Tart is required
        // on PATH; we don't try to auto-install it.
        let out = tokio::process::Command::new("tart")
            .args(["clone", image, &vm_name])
            .output()
            .await
            .map_err(|e| anyhow::anyhow!("tart clone failed to spawn: {} (is tart on PATH?)", e))?;
        if !out.status.success() {
            anyhow::bail!(
                "tart clone {} {} failed (exit {:?}): {}",
                image, vm_name, out.status.code(),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        // `tart run` blocks; we spawn it as a detached background
        // task whose JoinHandle we drop intentionally — the VM
        // continues running independently. Teardown shuts it down
        // explicitly via `tart stop`.
        let bg_name = vm_name.clone();
        tokio::spawn(async move {
            let _ = tokio::process::Command::new("tart")
                .args(["run", "--no-graphics", &bg_name])
                .output()
                .await;
        });
        Ok(vm_name)
    }
    async fn teardown(&self, sandbox: &Sandbox) -> anyhow::Result<()> {
        let Some(vm_name) = sandbox.backend_handle.as_deref() else {
            return Ok(()); // never provisioned successfully
        };
        // Best-effort: stop, then delete. Both can fail (VM already
        // gone, etc.); we log and continue rather than propagating.
        let _ = tokio::process::Command::new("tart")
            .args(["stop", vm_name])
            .output()
            .await;
        let _ = tokio::process::Command::new("tart")
            .args(["delete", vm_name])
            .output()
            .await;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
struct LimaBackend;
#[cfg(target_os = "linux")]
#[async_trait::async_trait]
impl Backend for LimaBackend {
    fn name(&self) -> &'static str { "lima" }
    async fn provision(&self, sandbox_id: &str, image: &str) -> anyhow::Result<String> {
        let inst = format!("kestrel-{}", sandbox_id);
        // `limactl start --name=<inst> <template>` boots a fresh
        // Lima instance from a built-in or user-provided template
        // (e.g. "ubuntu", "default"). `--tty=false` is required to
        // avoid blocking on the interactive setup prompt.
        let out = tokio::process::Command::new("limactl")
            .args(["start", "--tty=false", &format!("--name={}", inst), image])
            .output()
            .await
            .map_err(|e| anyhow::anyhow!("limactl start failed to spawn: {} (is limactl on PATH?)", e))?;
        if !out.status.success() {
            anyhow::bail!(
                "limactl start {} {} failed (exit {:?}): {}",
                inst, image, out.status.code(),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(inst)
    }
    async fn teardown(&self, sandbox: &Sandbox) -> anyhow::Result<()> {
        let Some(inst) = sandbox.backend_handle.as_deref() else { return Ok(()); };
        let _ = tokio::process::Command::new("limactl")
            .args(["stop", inst])
            .output()
            .await;
        let _ = tokio::process::Command::new("limactl")
            .args(["delete", inst])
            .output()
            .await;
        Ok(())
    }
}

#[cfg(target_os = "windows")]
struct HyperVBackend;
#[cfg(target_os = "windows")]
#[async_trait::async_trait]
impl Backend for HyperVBackend {
    fn name(&self) -> &'static str { "hyperv" }
    async fn provision(&self, sandbox_id: &str, image: &str) -> anyhow::Result<String> {
        // PowerShell New-VM driven via the Hyper-V module. The
        // `image` is interpreted as a VHDX path. Operators ship
        // pre-baked images; auto-fetch is a follow-up.
        let vm_name = format!("kestrel-{}", sandbox_id);
        let script = format!(
            "New-VM -Name '{}' -VHDPath '{}' -Generation 2 -MemoryStartupBytes 2GB; Start-VM -Name '{}'",
            vm_name, image, vm_name
        );
        let out = tokio::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", &script])
            .output()
            .await
            .map_err(|e| anyhow::anyhow!("powershell failed to spawn: {}", e))?;
        if !out.status.success() {
            anyhow::bail!(
                "Hyper-V New-VM/Start-VM failed (exit {:?}): {}",
                out.status.code(),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(vm_name)
    }
    async fn teardown(&self, sandbox: &Sandbox) -> anyhow::Result<()> {
        let Some(vm_name) = sandbox.backend_handle.as_deref() else { return Ok(()); };
        let script = format!(
            "Stop-VM -Name '{}' -Force; Remove-VM -Name '{}' -Force",
            vm_name, vm_name
        );
        let _ = tokio::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", &script])
            .output()
            .await;
        Ok(())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
struct UnsupportedBackend;
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
#[async_trait::async_trait]
impl Backend for UnsupportedBackend {
    fn name(&self) -> &'static str { "unsupported" }
    async fn provision(&self, _sandbox_id: &str, _image: &str) -> anyhow::Result<String> {
        anyhow::bail!("sandbox provisioning not supported on this OS")
    }
    async fn teardown(&self, _sandbox: &Sandbox) -> anyhow::Result<()> { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spawn_inserts_entry_in_provisioning_state() {
        let reg = SandboxRegistry::new();
        let id = reg.spawn("ubuntu-24.04", 3600).await.unwrap();
        let entry = reg.get(&id).await.unwrap();
        // Just-spawned entries are Provisioning; the spawn task may
        // race to Failed by the time we read (since the v1 backends
        // are all stubs that error). Either is acceptable.
        assert!(matches!(
            entry.status,
            SandboxStatus::Provisioning | SandboxStatus::Failed
        ));
        assert_eq!(entry.image, "ubuntu-24.04");
        // TTL math.
        assert_eq!(entry.expires_unix, entry.created_unix + 3600);
    }

    #[tokio::test]
    async fn fresh_sandbox_id_is_distinct_and_prefixed() {
        let a = fresh_sandbox_id();
        let b = fresh_sandbox_id();
        assert!(a.starts_with("sb-"));
        assert_eq!(a.len(), 3 + 12);
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn destroy_unknown_returns_false() {
        let reg = SandboxRegistry::new();
        assert!(!reg.destroy("sb-nope").await);
    }
}
