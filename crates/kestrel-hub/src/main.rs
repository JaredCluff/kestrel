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

#[derive(Parser)]
#[command(name = "kestrel-hub", about = "Kestrel fleet hub")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init {
        /// IP the agent enrollment command will point at. Default `127.0.0.1`
        /// is for loopback / single-machine setups; pass `--bind 0.0.0.0` (or
        /// the LAN IP of this hub host) to let other machines enroll.
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
    /// Set or update a KVM layout entry for a node.
    LayoutSet {
        node_id: String,
        col: i64,
        row: i64,
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
    },
    /// Remove a KVM layout entry.
    LayoutUnset {
        node_id: String,
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
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
            let psk = enrollment::generate_psk();
            enrollment::store_psk(&psk)?;
            let token = enrollment::generate_control_token();
            enrollment::store_control_token(&token)?;
            match enrollment::scaffold_hub_config(&config, &dashboard) {
                Ok(()) => println!("Wrote starter config: {}", config),
                Err(e) => {
                    // Already-exists is non-fatal — preserve the user's config.
                    tracing::warn!("config not scaffolded: {}", e);
                }
            }
            println!("Key generated and stored in system credential store.");
            println!("Run this on each node machine:");
            println!("  {}", enrollment::enrollment_command(&bind, &psk));
            println!();
            println!("Hub control token (stored in keyring; for remote `kestrel-hub add-node --hub <url>` calls):");
            println!("  export KESTREL_TOKEN={}", token);
            println!();
            println!("Then on this hub: kestrel-hub add-node <id> <addr> && kestrel-hub start");
        }
        Command::Connect { config } => {
            let cfg = HubConfig::from_file(&config)?;
            let psk = enrollment::load_psk()?;
            for node in &cfg.nodes {
                let (conn, _actor) = transport::connect(node.address, &psk).await?;
                println!("connected: {} ({})", conn.node_id, conn.os_info.name);
            }
            tokio::signal::ctrl_c().await?;
            println!("shutting down");
        }
        Command::Start { config } => {
            let cfg = HubConfig::from_file(&config)?;
            let psk = enrollment::load_psk()?;
            let registry = Arc::new(NodeRegistry::new());

            // Build the shared application state; supervisor handles go in `state.supervisors`.
            // Load the control token from the keyring if it's there; absent token means
            // legacy/no-auth mode (init upgrades this on next run).
            let state = dashboard::AppState::new(registry.clone(), config.clone(), psk.clone());
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
            for node in &cfg.nodes {
                let handle = supervisor::spawn(node.clone(), registry.clone(), psk.clone());
                state.supervisors.write().await.insert(node.node_id.clone(), handle);
                println!("supervising: {} ({})", node.node_id, node.address);
            }

            kestrel_hub::kvm::start(cfg.layout.clone(), registry.clone());

            // Start the dashboard HTTP server. Failure to bind is fatal because the user
            // explicitly configured this address — surface the error rather than silently
            // continuing without a dashboard.
            let dash_listener = tokio::net::TcpListener::bind(cfg.listen_dashboard)
                .await
                .map_err(|e| anyhow::anyhow!("dashboard bind to {} failed: {}", cfg.listen_dashboard, e))?;
            let dash_addr = dash_listener.local_addr()?;
            let dash_state = state.clone();
            let dashboard_handle = tokio::spawn(async move {
                if let Err(e) = axum::serve(dash_listener, dashboard::router(dash_state)).await {
                    tracing::error!("dashboard server error: {}", e);
                }
            });
            println!("Dashboard at http://{}", dash_addr);

            println!("Kestrel hub started. Serving MCP via stdio.");
            let mcp = KestrelMcp::new(registry);
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
                Err(e) => {
                    // Hub unreachable — fall back to local file mutation.
                    tracing::debug!("hub not reachable ({}), writing file directly", e);
                    let mut doc = kestrel_hub::config::load_doc(&config)?;
                    kestrel_hub::config::add_node(&mut doc, &node_id, parsed_addr)?;
                    kestrel_hub::config::save_doc(&config, &doc)?;
                    println!("added '{}' at {}. start `kestrel-hub start` (or restart it) to connect.", node_id, parsed_addr);
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
                Err(e) => {
                    tracing::debug!("hub not reachable ({}), writing file directly", e);
                    let mut doc = kestrel_hub::config::load_doc(&config)?;
                    kestrel_hub::config::remove_node(&mut doc, &node_id)?;
                    kestrel_hub::config::save_doc(&config, &doc)?;
                    println!("removed '{}' from {}. (hub not running)", node_id, config);
                }
            }
        }
        Command::LayoutSet { node_id, col, row, config } => {
            let mut doc = kestrel_hub::config::load_doc(&config)?;
            kestrel_hub::config::set_layout(&mut doc, &node_id, col, row)?;
            kestrel_hub::config::save_doc(&config, &doc)?;
            println!("layout: '{}' -> ({}, {}).", node_id, col, row);
        }
        Command::LayoutUnset { node_id, config } => {
            let mut doc = kestrel_hub::config::load_doc(&config)?;
            kestrel_hub::config::remove_layout(&mut doc, &node_id)?;
            kestrel_hub::config::save_doc(&config, &doc)?;
            println!("layout cleared for '{}'.", node_id);
        }
        Command::Status { config, timeout } => {
            let cfg = HubConfig::from_file(&config)?;
            let psk = enrollment::load_psk()?;
            if cfg.nodes.is_empty() {
                println!("(no nodes configured)");
            } else {
                for node in &cfg.nodes {
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
                println!("  - clear keyring entry (kestrel, psk)");
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
            for step in enrollment::clear_hub_keyring() {
                match step {
                    enrollment::UnenrollStep::Cleared(w) => println!("{:<16} cleared", w),
                    enrollment::UnenrollStep::NotFound(w) => println!("{:<16} (not found)", w),
                    enrollment::UnenrollStep::Failed(w, e) => println!("{:<16} FAILED: {}", w, e),
                }
            }
            if will_delete_config {
                match std::fs::remove_file(&config) {
                    Ok(()) => println!("{:<16} deleted", config),
                    Err(e) => println!("{:<16} FAILED: {}", config, e),
                }
            } else if keep_config {
                println!("{:<16} kept (--keep-config)", config);
            }
        }
    }
    Ok(())
}
