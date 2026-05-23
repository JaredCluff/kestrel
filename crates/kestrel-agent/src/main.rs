use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "kestrel-agent", about = "Kestrel fleet node agent")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the agent with the given config file.
    Start {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
    },
    /// Store the hub's PSK and scaffold a starter agent kestrel.toml.
    Enroll {
        #[arg(long)]
        hub: String,
        #[arg(long)]
        key: String,
        /// Override the auto-detected node_id (defaults to the system hostname).
        #[arg(long)]
        node_id: Option<String>,
        /// Listen address for the agent's WSS server.
        #[arg(long, default_value = "0.0.0.0:7272")]
        listen: String,
        /// Path to write the agent config to.
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
    },
    /// Print the loaded config and verify the keyring PSK exists.
    Status {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
    },
    /// Clear the PSK from the keyring and (optionally) delete kestrel.toml.
    /// Destructive — requires `--yes` to actually take effect.
    Unenroll {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
        /// Skip the config file deletion; only clear the keyring PSK.
        #[arg(long)]
        keep_config: bool,
        /// Required to actually perform the unenroll.
        #[arg(long)]
        yes: bool,
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
        Command::Enroll { hub, key, node_id, listen, config } => {
            let psk = hex::decode(&key).map_err(|e| anyhow::anyhow!("invalid hex key: {}", e))?;
            anyhow::ensure!(psk.len() == 32, "PSK must be 32 bytes (64 hex chars); got {}", psk.len());
            let entry = keyring::Entry::new("kestrel", "psk")?;
            entry.set_password(&hex::encode(&psk))?;
            println!("PSK stored in system credential store.");
            let resolved_node_id = node_id.unwrap_or_else(|| {
                hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "agent".into())
            });
            match kestrel_agent::config::scaffold_agent_config(&config, &resolved_node_id, &listen) {
                Ok(()) => println!("Wrote starter config: {} (node_id={})", config, resolved_node_id),
                Err(e) => tracing::warn!("config not scaffolded: {}", e),
            }
            println!("Enrolled with hub at {}. Start the agent with: kestrel-agent start", hub);
        }
        Command::Status { config } => {
            let cfg = kestrel_agent::config::AgentConfig::from_file(&config)?;
            println!("node_id : {}", cfg.node_id);
            println!("listen  : {}", cfg.listen);
            let psk_present = keyring::Entry::new("kestrel", "psk")
                .and_then(|e| e.get_password())
                .is_ok();
            println!("psk     : {}", if psk_present { "(present in keyring)" } else { "(MISSING)" });
        }
        Command::Unenroll { config, keep_config, yes } => {
            let will_delete_config = !keep_config && std::path::Path::new(&config).exists();
            if !yes {
                println!("`kestrel-agent unenroll` would:");
                println!("  - clear keyring entry (kestrel, psk)");
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
            // Track whether any destructive step failed so we exit non-zero
            // — same rationale as the hub-side unenroll: scripted decommission
            // pipelines need a real exit code, not "always 0 with FAILED in
            // stdout".
            let mut any_failed = false;
            match keyring::Entry::new("kestrel", "psk").and_then(|e| e.delete_password()) {
                Ok(()) => println!("psk              cleared"),
                Err(keyring::Error::NoEntry) => println!("psk              (not found)"),
                Err(e) => {
                    println!("psk              FAILED: {}", e);
                    any_failed = true;
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
