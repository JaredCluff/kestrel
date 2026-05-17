use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "kestrel-agent", about = "Kestrel fleet node agent")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Start {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
    },
    Enroll {
        #[arg(long)]
        hub: String,
        #[arg(long)]
        key: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Start { config } => {
            let cfg = kestrel_agent::config::AgentConfig::from_file(&config)?;
            kestrel_agent::transport::serve(&cfg, None).await?;
        }
        Command::Enroll { hub: _, key } => {
            let psk = hex::decode(&key)?;
            let entry = keyring::Entry::new("kestrel", "psk")?;
            entry.set_password(&hex::encode(&psk))?;
            println!("PSK stored in system credential store.");
            println!("Start the agent with: kestrel-agent start");
        }
    }
    Ok(())
}
