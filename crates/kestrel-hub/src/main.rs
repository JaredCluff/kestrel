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

#[derive(Parser)]
#[command(name = "kestrel-hub", about = "Kestrel fleet hub")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init {
        #[arg(long, default_value = "0.0.0.0")]
        bind: String,
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
        #[arg(long, default_value = "0.0.0.0:7273")]
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
    /// Append a new node to kestrel.toml. Restart `kestrel-hub start` to connect.
    AddNode {
        node_id: String,
        address: String,
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
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

            // Spawn one supervisor per configured node. Each supervisor handles its own
            // (re)connection lifecycle and emits status events to the registry's broadcast.
            let mut supervisors = Vec::with_capacity(cfg.nodes.len());
            for node in &cfg.nodes {
                let handle = supervisor::spawn(node.clone(), registry.clone(), psk.clone());
                supervisors.push(handle);
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
            let dash_registry = registry.clone();
            let dashboard_handle = tokio::spawn(async move {
                if let Err(e) = axum::serve(dash_listener, dashboard::router(dash_registry)).await {
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
            for s in supervisors {
                s.abort();
            }
            dashboard_handle.abort();
        }
        Command::AddNode { node_id, address, config } => {
            // Validate the address parses as SocketAddr before touching the file.
            let _: std::net::SocketAddr = address.parse()
                .map_err(|e| anyhow::anyhow!("invalid address '{}': {}", address, e))?;

            let contents = std::fs::read_to_string(&config)
                .map_err(|e| anyhow::anyhow!("read {}: {}", config, e))?;
            let mut doc: toml::Value = toml::from_str(&contents)
                .map_err(|e| anyhow::anyhow!("parse {}: {}", config, e))?;

            // Navigate to hub.nodes (an array of inline tables in [[hub.nodes]] form).
            let hub = doc.get_mut("hub")
                .and_then(|v| v.as_table_mut())
                .ok_or_else(|| anyhow::anyhow!("config has no [hub] section"))?;

            let nodes = hub.entry("nodes")
                .or_insert_with(|| toml::Value::Array(Vec::new()))
                .as_array_mut()
                .ok_or_else(|| anyhow::anyhow!("hub.nodes is not an array"))?;

            // Refuse duplicates.
            let duplicate = nodes.iter().any(|n| {
                n.as_table().and_then(|t| t.get("node_id")).and_then(|v| v.as_str()) == Some(node_id.as_str())
            });
            if duplicate {
                anyhow::bail!("node '{}' already exists in {}", node_id, config);
            }

            let mut entry = toml::value::Table::new();
            entry.insert("node_id".into(), toml::Value::String(node_id.clone()));
            entry.insert("address".into(), toml::Value::String(address.clone()));
            nodes.push(toml::Value::Table(entry));

            let serialized = toml::to_string_pretty(&doc)
                .map_err(|e| anyhow::anyhow!("serialize TOML: {}", e))?;
            std::fs::write(&config, serialized)
                .map_err(|e| anyhow::anyhow!("write {}: {}", config, e))?;

            println!("added '{}' at {}. restart `kestrel-hub start` to connect.", node_id, address);
        }
    }
    Ok(())
}
