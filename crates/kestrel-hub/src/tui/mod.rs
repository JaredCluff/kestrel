// crates/kestrel-hub/src/tui/mod.rs
use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use futures::stream::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

pub mod client;
pub mod view;

use crate::dashboard::api::NodeStatusDto;

#[derive(Debug, Clone)]
pub struct TuiArgs {
    pub hub_url: String,
}

pub async fn run(args: TuiArgs) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = event_loop(&mut terminal, args).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    args: TuiArgs,
) -> anyhow::Result<()> {
    let client = client::HubClient::new(args.hub_url);
    let mut nodes: Vec<NodeStatusDto> = client.fetch_nodes().await.unwrap_or_default();
    let mut events = Box::pin(client.subscribe_events());

    loop {
        terminal.draw(|f| view::render(f, &nodes))?;

        tokio::select! {
            // Poll the terminal for keypresses on a short cadence.
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if event::poll(Duration::from_millis(0))? {
                    if let Event::Key(k) = event::read()? {
                        if k.kind == KeyEventKind::Press {
                            match k.code {
                                KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                                KeyCode::Char('r') => {
                                    nodes = client.fetch_nodes().await.unwrap_or(nodes);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            // On each event, re-fetch the snapshot. This is simple and correct —
            // the event channel could be lossy, so always trust /api/nodes.
            evt = events.next() => {
                match evt {
                    Some(Ok(_)) => {
                        nodes = client.fetch_nodes().await.unwrap_or(nodes);
                    }
                    Some(Err(_)) | None => {
                        // SSE dropped — reconnect.
                        events = Box::pin(client.subscribe_events());
                    }
                }
            }
        }
    }
}
