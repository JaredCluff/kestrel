use clap::{Parser, Subcommand};
use kestrel_hub::{config::HubConfig, enrollment};

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
                let conn = kestrel_hub::transport::connect(node.address, &psk).await?;
                println!("connected: {} ({})", conn.node_id, conn.os_info.name);
            }
            tokio::signal::ctrl_c().await?;
            println!("shutting down");
        }
    }
    Ok(())
}
