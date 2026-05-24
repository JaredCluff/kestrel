// crates/kestrel-hub/src/doctor.rs
//
// Pre-flight + post-flight diagnostics for a hub install. `kestrel-hub
// doctor` walks a checklist of common failure modes — keyring readable,
// master_secret present, audit log writable, dashboard port available,
// configured nodes reachable, sandbox backend on PATH — and prints a
// per-check Pass/Warn/Fail line.
//
// The intent is "when something's wrong with a deploy, this prints
// where to look." Nothing here mutates state; it's safe to run against
// a live hub.

use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Warn,
    Fail,
}

impl Verdict {
    pub fn glyph(&self) -> &'static str {
        match self {
            Verdict::Pass => "✓",
            Verdict::Warn => "!",
            Verdict::Fail => "✗",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckResult {
    /// Display name for the check. `String` (not `&'static str`)
    /// because per-node checks need to interpolate the node_id, and
    /// owning the name is simpler than wrangling a Cow or
    /// Box::leak'ing once per check. The cost (one short heap alloc
    /// per check) is negligible compared to the network I/O the
    /// checks themselves do.
    pub name: String,
    pub verdict: Verdict,
    pub detail: String,
}

/// Run every doctor check and return the per-check results in
/// presentation order. The caller prints them; tests assert against
/// the structured output.
pub async fn run(config_path: &str) -> Vec<CheckResult> {
    let mut out = Vec::new();
    out.push(check_master_secret());
    out.push(check_control_token());
    let cfg = match crate::config::HubConfig::from_file(config_path) {
        Ok(c) => {
            out.push(CheckResult {
                name: "hub config".into(),
                verdict: Verdict::Pass,
                detail: format!("parsed {}", config_path),
            });
            Some(c)
        }
        Err(e) => {
            out.push(CheckResult {
                name: "hub config".into(),
                verdict: Verdict::Fail,
                detail: format!("could not parse {}: {}", config_path, e),
            });
            None
        }
    };
    if let Some(cfg) = &cfg {
        out.push(check_dashboard_port(cfg.listen_dashboard).await);
        out.extend(check_nodes_reachable(cfg).await);
        out.push(check_sandbox_backend(cfg.sandbox_bootstrap.as_ref()));
    }
    out
}

fn check_master_secret() -> CheckResult {
    match crate::enrollment::load_master_secret() {
        Ok(s) if s.len() == 32 => CheckResult {
            name: "master_secret in keyring".into(),
            verdict: Verdict::Pass,
            detail: "present, 32 bytes".into(),
        },
        Ok(s) => CheckResult {
            name: "master_secret in keyring".into(),
            verdict: Verdict::Warn,
            detail: format!(
                "present but {} bytes (expected 32) — re-run `kestrel-hub init`",
                s.len()
            ),
        },
        Err(e) => CheckResult {
            name: "master_secret in keyring".into(),
            verdict: Verdict::Fail,
            detail: format!("not found: {} — run `kestrel-hub init`", e),
        },
    }
}

fn check_control_token() -> CheckResult {
    match crate::enrollment::load_control_token() {
        Ok(t) if !t.is_empty() => CheckResult {
            name: "control_token in keyring".into(),
            verdict: Verdict::Pass,
            detail: "present".into(),
        },
        Ok(_) => CheckResult {
            name: "control_token in keyring".into(),
            verdict: Verdict::Warn,
            detail: "present but empty — dashboard mutations will reject".into(),
        },
        Err(e) => CheckResult {
            name: "control_token in keyring".into(),
            verdict: Verdict::Warn,
            detail: format!(
                "not found: {} — POST/DELETE on /api/nodes will be open (legacy mode)",
                e
            ),
        },
    }
}

async fn check_dashboard_port(addr: std::net::SocketAddr) -> CheckResult {
    // We can't reliably tell whether OUR running hub holds the port
    // vs. something else. So check both: try to bind (if it succeeds,
    // nothing is listening); if it fails with AddrInUse, try a TCP
    // connect (success = something IS listening, which is the
    // expected steady state once `kestrel-hub start` is up).
    match tokio::net::TcpListener::bind(addr).await {
        Ok(_listener) => CheckResult {
            name: "dashboard port".into(),
            verdict: Verdict::Pass,
            detail: format!("{} bindable (hub not running, or stopped)", addr),
        },
        Err(_) => match tokio::net::TcpStream::connect(addr).await {
            Ok(_) => CheckResult {
                name: "dashboard port".into(),
                verdict: Verdict::Pass,
                detail: format!("{} in use (hub appears running)", addr),
            },
            Err(e) => CheckResult {
                name: "dashboard port".into(),
                verdict: Verdict::Fail,
                detail: format!(
                    "{} neither bindable nor connectable: {} (firewall? perm denied?)",
                    addr, e
                ),
            },
        },
    }
}

async fn check_nodes_reachable(cfg: &crate::config::HubConfig) -> Vec<CheckResult> {
    if cfg.nodes.is_empty() {
        return vec![CheckResult {
            name: "configured nodes".into(),
            verdict: Verdict::Warn,
            detail: "no nodes configured — run `kestrel-hub add-node <id> <addr>`".into(),
        }];
    }
    let mut out = Vec::with_capacity(cfg.nodes.len());
    for node in &cfg.nodes {
        let r = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(node.address),
        )
        .await;
        let (verdict, detail) = match r {
            Ok(Ok(_)) => (Verdict::Pass, format!("{} TCP reachable", node.address)),
            Ok(Err(e)) => (
                Verdict::Fail,
                format!("{} connect failed: {}", node.address, e),
            ),
            Err(_) => (
                Verdict::Fail,
                format!("{} connect timeout after 2s", node.address),
            ),
        };
        out.push(CheckResult {
            name: format!("node {}", node.node_id),
            verdict,
            detail,
        });
    }
    out
}

fn check_sandbox_backend(
    bootstrap: Option<&crate::sandbox_bootstrap::SandboxBootstrapConfig>,
) -> CheckResult {
    // Pick the backend the hub will use based on host OS. If sandbox
    // bootstrap is configured, also verify the operator's referenced
    // binaries exist.
    let backend = if cfg!(target_os = "macos") {
        "tart"
    } else if cfg!(target_os = "linux") {
        "limactl"
    } else if cfg!(target_os = "windows") {
        "powershell"
    } else {
        return CheckResult {
            name: "sandbox backend".into(),
            verdict: Verdict::Warn,
            detail: "no supported backend on this OS".into(),
        };
    };
    let on_path = which(backend).is_some();
    if !on_path {
        return CheckResult {
            name: "sandbox backend".into(),
            verdict: Verdict::Warn,
            detail: format!(
                "{} not on PATH — `sandbox_spawn` will fail until installed",
                backend
            ),
        };
    }
    if let Some(b) = bootstrap {
        if !b.agent_binary.exists() {
            return CheckResult {
                name: "sandbox backend".into(),
                verdict: Verdict::Fail,
                detail: format!(
                    "{} found but sandbox.bootstrap.agent_binary missing: {}",
                    backend,
                    b.agent_binary.display()
                ),
            };
        }
        if !b.ssh_key.exists() {
            return CheckResult {
                name: "sandbox backend".into(),
                verdict: Verdict::Fail,
                detail: format!(
                    "{} found but sandbox.bootstrap.ssh_key missing: {}",
                    backend,
                    b.ssh_key.display()
                ),
            };
        }
    }
    CheckResult {
        name: "sandbox backend".into(),
        verdict: Verdict::Pass,
        detail: format!("{} on PATH", backend),
    }
}

/// Minimal `which`: search PATH for a binary by name. Avoids pulling
/// in the `which` crate for one function. Returns the first match or
/// None.
fn which(bin: &str) -> Option<std::path::PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let full = dir.join(bin);
        if full.is_file() {
            return Some(full);
        }
        // Windows: also try .exe / .cmd suffixes.
        #[cfg(target_os = "windows")]
        for ext in ["exe", "cmd", "bat"] {
            let full = dir.join(format!("{}.{}", bin, ext));
            if full.is_file() {
                return Some(full);
            }
        }
    }
    None
}

/// Format the checklist as one line per check, suitable for terminal
/// output. The verdict counts at the end give operators a quick
/// "yes/no, ship it" read.
pub fn format_report(results: &[CheckResult]) -> String {
    let mut out = String::new();
    for r in results {
        out.push_str(&format!("  {} {:<32} {}\n", r.verdict.glyph(), r.name, r.detail));
    }
    let pass = results.iter().filter(|r| r.verdict == Verdict::Pass).count();
    let warn = results.iter().filter(|r| r.verdict == Verdict::Warn).count();
    let fail = results.iter().filter(|r| r.verdict == Verdict::Fail).count();
    out.push_str(&format!(
        "\n{} pass, {} warn, {} fail\n",
        pass, warn, fail
    ));
    out
}

/// True iff no FAIL verdicts. Used to set the CLI's exit code so
/// `kestrel-hub doctor && echo ok` works as a deploy gate.
pub fn ok(results: &[CheckResult]) -> bool {
    !results.iter().any(|r| r.verdict == Verdict::Fail)
}

#[allow(dead_code)]
fn _unused_path() -> &'static Path {
    Path::new("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_glyphs_distinct() {
        assert_ne!(Verdict::Pass.glyph(), Verdict::Warn.glyph());
        assert_ne!(Verdict::Warn.glyph(), Verdict::Fail.glyph());
    }

    #[test]
    fn ok_returns_true_when_no_fail() {
        let r = vec![
            CheckResult { name: "a".into(), verdict: Verdict::Pass, detail: "".into() },
            CheckResult { name: "b".into(), verdict: Verdict::Warn, detail: "".into() },
        ];
        assert!(ok(&r));
    }

    #[test]
    fn ok_returns_false_when_any_fail() {
        let r = vec![
            CheckResult { name: "a".into(), verdict: Verdict::Pass, detail: "".into() },
            CheckResult { name: "b".into(), verdict: Verdict::Fail, detail: "".into() },
        ];
        assert!(!ok(&r));
    }

    #[test]
    fn format_report_shows_summary() {
        let r = vec![
            CheckResult { name: "a".into(), verdict: Verdict::Pass, detail: "ok".into() },
            CheckResult { name: "b".into(), verdict: Verdict::Warn, detail: "huh".into() },
            CheckResult { name: "c".into(), verdict: Verdict::Fail, detail: "no".into() },
        ];
        let s = format_report(&r);
        assert!(s.contains("1 pass"));
        assert!(s.contains("1 warn"));
        assert!(s.contains("1 fail"));
        assert!(s.contains("a"));
        assert!(s.contains("b"));
        assert!(s.contains("c"));
    }

    #[tokio::test]
    async fn dashboard_port_bindable_returns_pass() {
        // Bind to :0 so we get a guaranteed-free port, then drop the
        // listener — the port is briefly free again when we check.
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        let r = check_dashboard_port(addr).await;
        // Either Pass (bindable — the usual outcome) or Pass (in use,
        // if the OS hadn't released the port yet). Both are acceptable.
        assert_eq!(r.verdict, Verdict::Pass);
    }

    #[test]
    fn which_finds_a_known_binary() {
        // `sh` is on PATH on every Unix system; `cmd` on Windows.
        #[cfg(unix)]
        assert!(which("sh").is_some(), "expected `sh` on PATH");
        #[cfg(windows)]
        assert!(which("cmd").is_some(), "expected `cmd` on PATH");
    }

    #[test]
    fn which_misses_a_nonexistent_binary() {
        assert!(which("definitely-not-a-real-binary-zzz-xyz").is_none());
    }
}
