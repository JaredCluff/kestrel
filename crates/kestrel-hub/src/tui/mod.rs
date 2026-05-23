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

pub mod view;

use crate::client::HubClient;
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

/// Decision a TUI key press should produce. Extracted from
/// `event_loop` so the dispatch logic can be unit-tested without
/// driving a real terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyAction {
    /// Exit the TUI cleanly.
    Quit,
    /// Re-fetch the node list from /api/nodes.
    Refresh,
    /// Ignore the key — nothing to do.
    Noop,
}

/// Pure key-dispatch function: takes a key code + event kind, returns
/// the action the event loop should take. Anything that isn't a Press
/// is Noop. Only `q`, `Esc`, and `r` produce non-trivial actions.
pub fn handle_key_press(code: KeyCode, kind: KeyEventKind) -> KeyAction {
    if kind != KeyEventKind::Press {
        return KeyAction::Noop;
    }
    match code {
        KeyCode::Char('q') | KeyCode::Esc => KeyAction::Quit,
        KeyCode::Char('r') => KeyAction::Refresh,
        _ => KeyAction::Noop,
    }
}

/// What the event loop should do with the next SSE item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SseAction {
    /// Got an event — refresh the node list (events themselves are
    /// just notifications; /api/nodes is authoritative).
    Refresh,
    /// Stream ended or errored — reconnect.
    Reconnect,
}

/// Classify a single SSE stream item.
pub fn classify_sse_item<T, E>(item: Option<Result<T, E>>) -> SseAction {
    match item {
        Some(Ok(_)) => SseAction::Refresh,
        Some(Err(_)) | None => SseAction::Reconnect,
    }
}

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    args: TuiArgs,
) -> anyhow::Result<()> {
    let client = HubClient::new(args.hub_url);
    let mut nodes: Vec<NodeStatusDto> = client.fetch_nodes().await.unwrap_or_default();
    let mut events = Box::pin(client.subscribe_events());

    loop {
        terminal.draw(|f| view::render(f, &nodes))?;

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if event::poll(Duration::from_millis(0))? {
                    if let Event::Key(k) = event::read()? {
                        match handle_key_press(k.code, k.kind) {
                            KeyAction::Quit => return Ok(()),
                            KeyAction::Refresh => {
                                nodes = client.fetch_nodes().await.unwrap_or(nodes);
                            }
                            KeyAction::Noop => {}
                        }
                    }
                }
            }
            evt = events.next() => {
                match classify_sse_item(evt) {
                    SseAction::Refresh => {
                        nodes = client.fetch_nodes().await.unwrap_or(nodes);
                    }
                    SseAction::Reconnect => {
                        events = Box::pin(client.subscribe_events());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_press_q_quits() {
        assert_eq!(
            handle_key_press(KeyCode::Char('q'), KeyEventKind::Press),
            KeyAction::Quit
        );
    }

    #[test]
    fn key_press_esc_quits() {
        assert_eq!(
            handle_key_press(KeyCode::Esc, KeyEventKind::Press),
            KeyAction::Quit
        );
    }

    #[test]
    fn key_press_r_refreshes() {
        assert_eq!(
            handle_key_press(KeyCode::Char('r'), KeyEventKind::Press),
            KeyAction::Refresh
        );
    }

    #[test]
    fn key_press_other_chars_noop() {
        // The TUI is intentionally lean: only q/Esc/r do anything.
        // A regression that hooked another key into quit (e.g. typo
        // matching 'Q' uppercase) would silently break user flow.
        for c in ['a', 'Q', 'R', 'x', '1', ' '] {
            assert_eq!(
                handle_key_press(KeyCode::Char(c), KeyEventKind::Press),
                KeyAction::Noop,
                "key {:?} should be Noop",
                c
            );
        }
    }

    #[test]
    fn key_release_never_acts() {
        // crossterm reports both Press and Release on some terminals.
        // We only act on Press — Release of 'q' must NOT quit.
        assert_eq!(
            handle_key_press(KeyCode::Char('q'), KeyEventKind::Release),
            KeyAction::Noop
        );
        assert_eq!(
            handle_key_press(KeyCode::Esc, KeyEventKind::Release),
            KeyAction::Noop
        );
    }

    #[test]
    fn sse_ok_refreshes() {
        let item: Option<Result<(), &str>> = Some(Ok(()));
        assert_eq!(classify_sse_item(item), SseAction::Refresh);
    }

    #[test]
    fn sse_err_reconnects() {
        let item: Option<Result<(), &str>> = Some(Err("network gone"));
        assert_eq!(classify_sse_item(item), SseAction::Reconnect);
    }

    #[test]
    fn sse_none_reconnects() {
        let item: Option<Result<(), &str>> = None;
        assert_eq!(classify_sse_item(item), SseAction::Reconnect);
    }
}
