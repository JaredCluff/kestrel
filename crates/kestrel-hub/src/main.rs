// crates/kestrel-hub/src/main.rs
use clap::{Parser, Subcommand};
use kestrel_hub::{
    config::HubConfig,
    dashboard,
    enrollment,
    mcp::KestrelMcp,
    router::NodeRegistry,
    supervisor,
    transport,
};
use rmcp::{ServiceExt, transport::stdio};
use std::sync::Arc;

/// Resolve the hub control token for the CLI. Priority: `$KESTREL_TOKEN` env var,
/// then local keyring (set by `kestrel-hub init` on this machine), then None.
/// `None` is fine for hubs running in legacy/no-auth mode.
fn resolve_control_token() -> Option<String> {
    if let Ok(t) = std::env::var("KESTREL_TOKEN") {
        if !t.is_empty() {
            return Some(t);
        }
    }
    enrollment::load_control_token().ok()
}

/// Heuristic: did the error from `HubClient::add_node` / `remove_node` come
/// from the hub responding with a non-success status, or from a transport
/// failure (connection refused, timeout, TLS error)?
///
/// `HubClient` formats hub-responded errors as `"hub returned <code> ..."`
/// (see `client.rs::add_node`/`remove_node`). Anything else is a transport
/// failure. The CLI uses this to decide whether to fall back to local file
/// mutation: a transport failure means "hub probably not running, write the
/// file myself", but a hub-responded error means "the running hub knows
/// about us and is telling us no" — surface it instead of silently writing
/// to a file the hub will refuse to honor.
fn is_hub_responded_error(e: &anyhow::Error) -> bool {
    e.to_string().contains("hub returned ")
}

#[derive(Parser)]
#[command(name = "kestrel-hub", about = "Kestrel fleet hub")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init {
        /// Hub host as it will appear in the printed `kestrel-agent enroll
        /// --hub <bind>` line. Defaults to `127.0.0.1` for loopback; pass the
        /// LAN IP of this machine (e.g. `--bind 192.168.1.10`) when
        /// enrolling agents on other hosts. Not a listen address — the hub
        /// itself only listens on `--dashboard`.
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
        /// Dashboard listen address. Default `127.0.0.1:7273` is loopback-only;
        /// pass `--dashboard 0.0.0.0:7273` to expose to the LAN (read-only
        /// endpoints have no auth; mutation endpoints require the control
        /// token).
        #[arg(long, default_value = "127.0.0.1:7273")]
        dashboard: String,
    },
    Connect {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
    },
    /// Start the hub: connect to all configured nodes, serve MCP via stdio, and run KVM
    Start {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
        /// Append every MCP tool call to this JSONL file. Use `--audit-log -`
        /// or omit to disable. Each line records: timestamp, tool name,
        /// node_id, an args summary (secrets-free — typed text and
        /// clipboard payloads are length-only), status (ok/error),
        /// duration_ms, and the error message on failure.
        ///
        /// MUST NOT be a path that goes to stdout — the MCP server speaks
        /// JSON-RPC on stdio and an audit line on stdout would corrupt
        /// the protocol. The `-` sentinel explicitly disables audit
        /// rather than writing to stdout.
        #[arg(long)]
        audit_log: Option<String>,
        /// PEM-encoded certificate for the dashboard. Pair with
        /// `--dashboard-key`. When both are supplied, the dashboard
        /// serves HTTPS; otherwise it serves plain HTTP (LAN-trusted
        /// default).
        #[arg(long)]
        dashboard_cert: Option<String>,
        /// PEM-encoded private key for the dashboard. Pair with
        /// `--dashboard-cert`.
        #[arg(long)]
        dashboard_key: Option<String>,
    },
    /// Append a node to kestrel.toml. If the hub is running, applies live;
    /// otherwise the change takes effect at next `start`.
    AddNode {
        node_id: String,
        address: String,
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
        /// Hub control URL (HTTP). If reachable, the change applies live.
        #[arg(long, default_value = "http://127.0.0.1:7273")]
        hub: String,
    },
    /// Print configured nodes from kestrel.toml.
    ListNodes {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
    },
    /// Remove a node from kestrel.toml. If the hub is running, applies live.
    RemoveNode {
        node_id: String,
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
        /// Hub control URL (HTTP). If reachable, the change applies live.
        #[arg(long, default_value = "http://127.0.0.1:7273")]
        hub: String,
    },
    /// Set or update a KVM layout entry for a node. Hot-reload: applies live
    /// against a running hub via the dashboard API; falls back to file
    /// mutation if the hub isn't reachable.
    LayoutSet {
        node_id: String,
        col: i64,
        row: i64,
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
        /// Hub control URL (HTTP). If reachable, the change applies live.
        #[arg(long, default_value = "http://127.0.0.1:7273")]
        hub: String,
    },
    /// Remove a KVM layout entry. Hot-reload: applies live against a
    /// running hub; falls back to file mutation if the hub isn't reachable.
    LayoutUnset {
        node_id: String,
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
        /// Hub control URL (HTTP). If reachable, the change applies live.
        #[arg(long, default_value = "http://127.0.0.1:7273")]
        hub: String,
    },
    /// Print the agent enrollment line for a configured node.
    ///
    /// Uses the hub's stored master_secret to HKDF-derive the node's PSK
    /// and prints `kestrel-agent enroll --hub <bind> --node-id <id> --key <hex>`
    /// for the operator to run on the target machine. Re-runs are
    /// deterministic — calling this twice for the same node yields the
    /// same line (the master_secret does not rotate).
    Key {
        node_id: String,
        /// Hub host as it should appear in the printed `--hub` argument.
        /// Defaults to 127.0.0.1; pass the LAN IP of this hub for agents
        /// enrolling from another machine.
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
    },
    /// Probe each configured node once and print reachability.
    Status {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
        /// Per-node timeout in seconds.
        #[arg(long, default_value_t = 5)]
        timeout: u64,
    },
    /// Open the live TUI dashboard against a running hub.
    Tui {
        /// Base URL of the running hub's dashboard HTTP server.
        #[arg(long, default_value = "http://127.0.0.1:7273")]
        hub: String,
    },
    /// Clear the PSK + control token from the keyring and (optionally) delete kestrel.toml.
    /// Destructive — requires `--yes` to actually take effect.
    Unenroll {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
        /// Skip the config file deletion; only clear keyring entries.
        #[arg(long)]
        keep_config: bool,
        /// Required to actually perform the unenroll. Without it, prints the
        /// planned actions and exits without changing anything.
        #[arg(long)]
        yes: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Init { bind, config, dashboard } => {
            let master_secret = enrollment::generate_master_secret();
            enrollment::store_master_secret(&master_secret)?;
            let token = enrollment::generate_control_token();
            enrollment::store_control_token(&token)?;
            match enrollment::scaffold_hub_config(&config, &dashboard) {
                Ok(()) => println!("Wrote starter config: {}", config),
                Err(e) => {
                    // Already-exists is non-fatal — preserve the user's config.
                    tracing::warn!("config not scaffolded: {}", e);
                }
            }
            println!("Hub master secret generated and stored in system credential store.");
            println!("Per-node PSKs are HKDF-derived from this on every connect — the master never leaves the hub.");
            println!();
            println!("Hub control token (for remote `kestrel-hub add-node --hub <url>` calls):");
            println!("  export KESTREL_TOKEN={}", token);
            println!();
            println!("Next: `kestrel-hub add-node <id> <addr>` registers a node and prints the");
            println!("agent enrollment line (with the per-node PSK to give that agent).");
            println!("You can also re-derive a node's PSK later with `kestrel-hub key <id> --bind {}`.", bind);
        }
        Command::Connect { config } => {
            let cfg = HubConfig::from_file(&config)?;
            let master_secret = enrollment::load_master_secret()?;
            // Hold every (NodeHandle, actor JoinHandle) across the ctrl_c
            // wait. Previously these were per-loop-iteration locals — they
            // dropped at end-of-iteration, closing the cmd_tx and tearing
            // down each connection BEFORE we even started blocking. The
            // "connected: …" lines were accurate at the moment they printed
            // but stale by the time the user could see them.
            let mut handles: Vec<(transport::NodeHandle, tokio::task::JoinHandle<()>)> =
                Vec::with_capacity(cfg.nodes.len());
            for node in &cfg.nodes {
                // Derive this node's PSK from the master_secret on the fly.
                // No long-lived per-node key material lives on the hub disk
                // or in memory beyond this scope.
                let psk = kestrel_proto::derive_per_node_psk(&master_secret, &node.node_id);
                let (conn, actor) = transport::connect(node.address, &psk).await?;
                println!("connected: {} ({})", conn.node_id, conn.os_info.name);
                handles.push((conn, actor));
            }
            tokio::signal::ctrl_c().await?;
            println!("shutting down");
            // Drop handles first (closes the cmd_tx end), then abort the
            // actor tasks. Dropping cmd_tx alone is enough for clean exit
            // but the explicit abort makes shutdown deterministic.
            for (_conn, actor) in handles {
                actor.abort();
            }
        }
        Command::Start { config, audit_log, dashboard_cert, dashboard_key } => {
            let cfg = HubConfig::from_file(&config)?;
            // Resolve the audit logger up front so we either know it's
            // working before binding sockets, or we fall back to disabled
            // with a clear warning. We never let an audit-log open failure
            // prevent the hub from starting.
            let audit = match audit_log.as_deref() {
                Some("-") | None => kestrel_hub::audit::AuditLogger::disabled(),
                Some(path) => match kestrel_hub::audit::AuditLogger::file(path).await {
                    Ok(logger) => {
                        println!("Audit log: appending to {}", path);
                        logger
                    }
                    Err(e) => {
                        tracing::warn!("audit log open failed ({}); proceeding without audit", e);
                        kestrel_hub::audit::AuditLogger::disabled()
                    }
                },
            };
            let master_secret = enrollment::load_master_secret()?;
            let registry = Arc::new(NodeRegistry::new());

            // Build the shared application state; supervisor handles go in `state.supervisors`.
            // The hub stores ONLY the master_secret. Each supervisor derives its node's
            // per-node PSK at connect time. The dashboard's add-node endpoint does the
            // same derivation when hot-spawning a supervisor.
            // Single SharedLayout shared between the KVM task and the dashboard
            // so layout edits via `POST /api/layout` apply live.
            let shared_layout = kestrel_hub::kvm::shared_layout(cfg.layout.clone());
            let state = dashboard::AppState::with_layout(
                registry.clone(),
                config.clone(),
                master_secret.clone(),
                shared_layout.clone(),
            );
            let state = match enrollment::load_control_token() {
                Ok(token) => state.with_control_token(token),
                Err(e) => {
                    tracing::warn!(
                        "no control token in keyring ({}); mutation endpoints unauthenticated. \
                         Run `kestrel-hub init` again to regenerate.",
                        e
                    );
                    state
                }
            };

            // Spawn one supervisor per configured node. Each supervisor handles its own
            // (re)connection lifecycle and emits status events to the registry's broadcast.
            // master_secret was loaded as Zeroizing<Vec<u8>>; AppState consumed one copy
            // and we hand each supervisor its own zeroize-wrapped clone.
            for node in &cfg.nodes {
                let handle = supervisor::spawn(
                    node.clone(),
                    registry.clone(),
                    state.master_secret.clone(),
                );
                state.supervisors.write().await.insert(node.node_id.clone(), handle);
                println!("supervising: {} ({})", node.node_id, node.address);
            }

            kestrel_hub::kvm::start(shared_layout.clone(), registry.clone());

            // Start the dashboard. Failure to bind is fatal because the user
            // explicitly configured this address — surface the error rather
            // than silently continuing without a dashboard.
            //
            // TLS is opt-in: both --dashboard-cert and --dashboard-key must
            // be provided together. If only one is set, that's a config
            // error and we refuse to start (rather than silently fall back
            // to plain HTTP and surprise the operator).
            let dash_state = state.clone();
            let dash_addr = cfg.listen_dashboard;
            let dashboard_handle = match (dashboard_cert.as_deref(), dashboard_key.as_deref()) {
                (Some(cert_path), Some(key_path)) => {
                    // rustls 0.23 (transitively from axum-server 0.7) requires a
                    // CryptoProvider be installed at process startup. We install
                    // the `ring` provider here, lazily — it's idempotent in the
                    // sense that the second call returns the existing provider
                    // back unchanged. Ignoring the Result is intentional.
                    let _ = rustls_23::crypto::ring::default_provider().install_default();
                    let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(
                        cert_path, key_path,
                    )
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "dashboard TLS load (cert={}, key={}) failed: {}",
                            cert_path, key_path, e
                        )
                    })?;
                    println!("Dashboard at https://{}", dash_addr);
                    tokio::spawn(async move {
                        if let Err(e) = axum_server::bind_rustls(dash_addr, tls)
                            .serve(dashboard::router(dash_state).into_make_service())
                            .await
                        {
                            tracing::error!("dashboard TLS server error: {}", e);
                        }
                    })
                }
                (None, None) => {
                    let dash_listener = tokio::net::TcpListener::bind(dash_addr)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("dashboard bind to {} failed: {}", dash_addr, e)
                        })?;
                    let bound = dash_listener.local_addr()?;
                    println!("Dashboard at http://{}", bound);
                    tokio::spawn(async move {
                        if let Err(e) =
                            axum::serve(dash_listener, dashboard::router(dash_state)).await
                        {
                            tracing::error!("dashboard server error: {}", e);
                        }
                    })
                }
                (Some(_), None) | (None, Some(_)) => {
                    anyhow::bail!(
                        "--dashboard-cert and --dashboard-key must both be provided to enable TLS"
                    );
                }
            };

            println!("Kestrel hub started. Serving MCP via stdio.");
            let mcp = KestrelMcp::with_audit(registry, audit);
            let service = mcp.serve(stdio()).await.inspect_err(|e| {
                tracing::error!("MCP serve error: {e:?}");
            })?;
            service.waiting().await?;

            // Best-effort cleanup — abort supervisors and dashboard when MCP exits.
            for (_, h) in state.supervisors.write().await.drain() {
                h.abort();
            }
            dashboard_handle.abort();
        }
        Command::AddNode { node_id, address, config, hub } => {
            // Validate the address once up front so we get a clean error before any I/O.
            let parsed_addr: std::net::SocketAddr = address.parse()
                .map_err(|e| anyhow::anyhow!("invalid address '{}': {}", address, e))?;

            // Try HTTP first — the running hub will write the file itself.
            let mut client = kestrel_hub::client::HubClient::with_quick_timeout(&hub);
            if let Some(t) = resolve_control_token() {
                client = client.with_token(t);
            }
            match client.add_node(&node_id, &address).await {
                Ok(_status) => {
                    println!("added '{}' at {} (live via {}).", node_id, parsed_addr, hub);
                }
                Err(e) if is_hub_responded_error(&e) => {
                    // The hub responded but rejected the request (duplicate,
                    // unauthorized, structural config error, etc.). Do NOT
                    // fall back to file mutation — that would create a
                    // running-hub-vs-on-disk-config inconsistency. Surface
                    // the actual error.
                    return Err(e.context(format!("hub at {} rejected add-node", hub)));
                }
                Err(e) => {
                    // Transport failure — hub probably not running. Fall
                    // back to local file mutation so the change persists
                    // for the next `kestrel-hub start`.
                    tracing::debug!("hub not reachable ({}), writing file directly", e);
                    let mut doc = kestrel_hub::config::load_doc(&config)?;
                    kestrel_hub::config::add_node(&mut doc, &node_id, parsed_addr)?;
                    kestrel_hub::config::save_doc(&config, &doc)?;
                    println!("added '{}' at {}. start `kestrel-hub start` (or restart it) to connect.", node_id, parsed_addr);
                }
            }
            // If we're on the same machine as the hub keyring, derive and print
            // the agent enrollment line. If not (e.g. CLI talking to a remote
            // hub), print a hint to run `kestrel-hub key` on the hub host.
            match enrollment::load_master_secret() {
                Ok(master_secret) => {
                    println!();
                    println!("Run on '{}' to enroll its agent (replace <hub-ip> with this hub's LAN address):", node_id);
                    println!("  {}", enrollment::enrollment_command("<hub-ip>", &node_id, &master_secret));
                }
                Err(_) => {
                    println!();
                    println!("To get the agent enrollment line, run `kestrel-hub key {}` on the hub host.", node_id);
                }
            }
        }
        Command::ListNodes { config } => {
            let cfg = HubConfig::from_file(&config)?;
            if cfg.nodes.is_empty() {
                println!("(no nodes configured)");
            } else {
                for n in &cfg.nodes {
                    println!("{:<24} {}", n.node_id, n.address);
                }
            }
        }
        Command::RemoveNode { node_id, config, hub } => {
            let mut client = kestrel_hub::client::HubClient::with_quick_timeout(&hub);
            if let Some(t) = resolve_control_token() {
                client = client.with_token(t);
            }
            match client.remove_node(&node_id).await {
                Ok(()) => {
                    println!("removed '{}' (live via {}).", node_id, hub);
                }
                Err(e) if is_hub_responded_error(&e) => {
                    // Same logic as add-node: the hub responded with an
                    // error (404 not-found, 500 structural config error,
                    // 401 missing token). Surface it; do NOT silently
                    // overwrite the file behind the running hub.
                    return Err(e.context(format!("hub at {} rejected remove-node", hub)));
                }
                Err(e) => {
                    // Transport failure — fall back to file mutation.
                    tracing::debug!("hub not reachable ({}), writing file directly", e);
                    let mut doc = kestrel_hub::config::load_doc(&config)?;
                    kestrel_hub::config::remove_node(&mut doc, &node_id)?;
                    kestrel_hub::config::save_doc(&config, &doc)?;
                    println!("removed '{}' from {}. (hub not running)", node_id, config);
                }
            }
        }
        Command::LayoutSet { node_id, col, row, config, hub } => {
            // HTTP-first-with-fallback: if a hub is running on `hub`, apply the
            // edit live so the running KVM task picks it up without a restart;
            // otherwise mutate the file directly for next-start.
            let mut client = kestrel_hub::client::HubClient::with_quick_timeout(&hub);
            if let Some(t) = resolve_control_token() {
                client = client.with_token(t);
            }
            match client.set_layout(&node_id, col, row).await {
                Ok(()) => {
                    println!("layout: '{}' -> ({}, {}) (live via {}).", node_id, col, row, hub);
                }
                Err(e) if is_hub_responded_error(&e) => {
                    return Err(e.context(format!("hub at {} rejected layout-set", hub)));
                }
                Err(e) => {
                    tracing::debug!("hub not reachable ({}), writing file directly", e);
                    let mut doc = kestrel_hub::config::load_doc(&config)?;
                    kestrel_hub::config::set_layout(&mut doc, &node_id, col, row)?;
                    kestrel_hub::config::save_doc(&config, &doc)?;
                    println!("layout: '{}' -> ({}, {}).", node_id, col, row);
                }
            }
        }
        Command::LayoutUnset { node_id, config, hub } => {
            let mut client = kestrel_hub::client::HubClient::with_quick_timeout(&hub);
            if let Some(t) = resolve_control_token() {
                client = client.with_token(t);
            }
            match client.remove_layout(&node_id).await {
                Ok(()) => {
                    println!("layout cleared for '{}' (live via {}).", node_id, hub);
                }
                Err(e) if is_hub_responded_error(&e) => {
                    return Err(e.context(format!("hub at {} rejected layout-unset", hub)));
                }
                Err(e) => {
                    tracing::debug!("hub not reachable ({}), writing file directly", e);
                    let mut doc = kestrel_hub::config::load_doc(&config)?;
                    kestrel_hub::config::remove_layout(&mut doc, &node_id)?;
                    kestrel_hub::config::save_doc(&config, &doc)?;
                    println!("layout cleared for '{}'.", node_id);
                }
            }
        }
        Command::Key { node_id, bind } => {
            // Re-derive (deterministically) the per-node PSK from the hub's
            // master_secret and print the enrollment line. Useful when the
            // operator needs to re-onboard an agent without rotating the
            // whole fleet.
            let master_secret = enrollment::load_master_secret()?;
            println!("Run on '{}'\u{2019}s machine to enroll its agent:", node_id);
            println!("  {}", enrollment::enrollment_command(&bind, &node_id, &master_secret));
        }
        Command::Status { config, timeout } => {
            let cfg = HubConfig::from_file(&config)?;
            let master_secret = enrollment::load_master_secret()?;
            if cfg.nodes.is_empty() {
                println!("(no nodes configured)");
            } else {
                for node in &cfg.nodes {
                    // Derive per-node PSK on the fly so probe_node can use it.
                    let psk = kestrel_proto::derive_per_node_psk(&master_secret, &node.node_id);
                    let probe = kestrel_hub::status::probe_node(
                        node,
                        &psk,
                        std::time::Duration::from_secs(timeout),
                    )
                    .await;
                    match probe.result {
                        kestrel_hub::status::NodeProbeResult::Reachable { rtt } => {
                            println!("{:<24} {:<24} online   {}ms", probe.node_id, probe.address, rtt.as_millis());
                        }
                        kestrel_hub::status::NodeProbeResult::Unreachable { reason } => {
                            println!("{:<24} {:<24} offline  ({})", probe.node_id, probe.address, reason);
                        }
                    }
                }
            }
        }
        Command::Tui { hub } => {
            kestrel_hub::tui::run(kestrel_hub::tui::TuiArgs { hub_url: hub }).await?;
        }
        Command::Unenroll { config, keep_config, yes } => {
            let will_delete_config = !keep_config && std::path::Path::new(&config).exists();
            if !yes {
                println!("`kestrel-hub unenroll` would:");
                println!("  - clear keyring entry (kestrel, master_secret)");
                println!("  - clear keyring entry (kestrel, psk) (legacy, if present)");
                println!("  - clear keyring entry (kestrel, control_token)");
                if will_delete_config {
                    println!("  - delete {}", config);
                } else if keep_config {
                    println!("  - keep {} (--keep-config)", config);
                } else {
                    println!("  - {} does not exist, skipping", config);
                }
                println!();
                println!("Re-run with --yes to perform these actions.");
                return Ok(());
            }
            // Track whether any destructive step failed so we can exit
            // non-zero — important for scripted use:
            //     kestrel-hub unenroll --yes && wipe-host
            // should NOT proceed to wipe-host if the keyring or the config
            // delete failed midway.
            let mut any_failed = false;
            for step in enrollment::clear_hub_keyring() {
                match step {
                    enrollment::UnenrollStep::Cleared(w) => println!("{:<16} cleared", w),
                    enrollment::UnenrollStep::NotFound(w) => println!("{:<16} (not found)", w),
                    enrollment::UnenrollStep::Failed(w, e) => {
                        println!("{:<16} FAILED: {}", w, e);
                        any_failed = true;
                    }
                }
            }
            if will_delete_config {
                match std::fs::remove_file(&config) {
                    Ok(()) => println!("{:<16} deleted", config),
                    Err(e) => {
                        println!("{:<16} FAILED: {}", config, e);
                        any_failed = true;
                    }
                }
            } else if keep_config {
                println!("{:<16} kept (--keep-config)", config);
            }
            if any_failed {
                anyhow::bail!("one or more unenroll steps failed; system is in a partial state");
            }
        }
    }
    Ok(())
}
