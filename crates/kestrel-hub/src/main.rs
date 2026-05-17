// crates/kestrel-hub/src/main.rs
use clap::{Parser, Subcommand};
use kestrel_hub::{
    config::HubConfig,
    enrollment,
    mcp::KestrelMcp,
    router::NodeRegistry,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Init { bind } => {
            let psk = enrollment::generate_psk();
            enrollment::store_psk(&psk)?;
            println!("Key generated and stored in system credential store.");
            println!("Run this on each node machine:");
            println!("  {}", enrollment::enrollment_command(&bind, &psk));
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

            for node in &cfg.nodes {
                match transport::connect(node.address, &psk).await {
                    Ok((handle, _actor)) => {
                        println!("connected: {} ({})", handle.node_id, handle.os_info.name);
                        registry.register(handle).await;
                    }
                    Err(e) => tracing::error!("failed to connect to {}: {}", node.node_id, e),
                }
            }

            kestrel_hub::kvm::start(cfg.layout.clone(), registry.clone());

            println!("Kestrel hub started. Serving MCP via stdio.");
            let mcp = KestrelMcp::new(registry);
            let service = mcp.serve(stdio()).await.inspect_err(|e| {
                tracing::error!("MCP serve error: {e:?}");
            })?;
            service.waiting().await?;
        }
    }
    Ok(())
}
