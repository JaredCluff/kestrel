// crates/kestrel-hub/src/tui/mod.rs
pub mod client;
pub mod view;

#[derive(Debug, Clone)]
pub struct TuiArgs {
    pub hub_url: String,
}

/// Run the TUI to completion. Task 8 wires the actual interactive event loop.
pub async fn run(args: TuiArgs) -> anyhow::Result<()> {
    let client = client::HubClient::new(args.hub_url);
    let nodes = client.fetch_nodes().await?;
    println!("({} nodes — interactive TUI lands in task 8)", nodes.len());
    Ok(())
}
