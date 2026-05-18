// crates/kestrel-hub/src/tui/view.rs
use crate::dashboard::api::{NodeStateDto, NodeStatusDto};

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
};

const ACCENT: Color = Color::Rgb(0x6e, 0xa3, 0xe0);
const MUTED: Color = Color::Rgb(0x6b, 0x6b, 0x6b);

pub fn render(f: &mut Frame, nodes: &[NodeStatusDto]) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(0)])
        .split(f.area());

    let header_line = Line::from(vec![
        Span::styled(
            "KESTREL",
            Style::default().fg(MUTED).add_modifier(Modifier::BOLD),
        ),
        Span::raw("    "),
        Span::styled(format!("{} nodes", nodes.len()), Style::default().fg(MUTED)),
    ]);
    f.render_widget(Paragraph::new(header_line), layout[0]);

    if nodes.is_empty() {
        let empty = Paragraph::new(Line::from(Span::styled(
            "no nodes",
            Style::default().fg(MUTED),
        )));
        f.render_widget(empty, layout[1]);
        return;
    }

    let rows: Vec<Row> = nodes
        .iter()
        .map(|n| {
            let state_text = match n.state {
                NodeStateDto::Online => "online",
                NodeStateDto::Offline => "offline",
                NodeStateDto::Reconnecting => "reconnecting",
            };
            let state_style = match n.state {
                NodeStateDto::Online => Style::default().fg(ACCENT),
                _ => Style::default().fg(MUTED),
            };
            let latency = n
                .latency_ms
                .map(|ms| format!("{}ms", ms))
                .unwrap_or_else(|| "—".into());
            Row::new(vec![
                Cell::from(Span::raw(n.node_id.clone())),
                Cell::from(Span::styled(state_text, state_style)),
                Cell::from(Span::styled(latency, Style::default().fg(MUTED))),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(16),
            Constraint::Length(10),
        ],
    )
    .block(Block::default().borders(Borders::NONE));

    f.render_widget(table, layout[1]);
}
