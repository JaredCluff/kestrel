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
            // Third column doubles as latency (when Online) or retry
            // countdown (when Reconnecting/Offline) — latency_ms is meaningless
            // for a node that isn't currently connected.
            let third = match n.state {
                NodeStateDto::Online => n
                    .latency_ms
                    .map(|ms| format!("{}ms", ms))
                    .unwrap_or_else(|| "—".into()),
                _ => n
                    .next_retry_in_ms
                    .map(|ms| format!("retry {}s", ms / 1000))
                    .unwrap_or_else(|| "—".into()),
            };
            Row::new(vec![
                Cell::from(Span::raw(n.node_id.clone())),
                Cell::from(Span::styled(state_text, state_style)),
                Cell::from(Span::styled(third, Style::default().fg(MUTED))),
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

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    /// Render `nodes` to a synthetic 80x10 backend and return the buffer
    /// as a single newline-joined string of cell `symbol`s. Style/colors
    /// aren't asserted — they're cosmetic — but the text content is what
    /// operators read off the screen.
    fn render_to_string(nodes: &[NodeStatusDto]) -> String {
        let backend = TestBackend::new(80, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, nodes)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        let mut out = String::new();
        for y in 0..buffer.area.height {
            for x in 0..buffer.area.width {
                out.push_str(buffer[(x, y)].symbol());
            }
            out.push('\n');
        }
        out
    }

    fn dto(node_id: &str, state: NodeStateDto) -> NodeStatusDto {
        NodeStatusDto {
            node_id: node_id.into(),
            state,
            os_name: None,
            latency_ms: None,
            last_seen_unix: 0,
            next_retry_in_ms: None,
        }
    }

    #[test]
    fn empty_state_renders_kestrel_header_and_no_nodes_line() {
        let out = render_to_string(&[]);
        // Header is always there.
        assert!(out.contains("KESTREL"), "missing KESTREL header in:\n{}", out);
        assert!(out.contains("0 nodes"), "missing node count in:\n{}", out);
        // Empty-state message.
        assert!(out.contains("no nodes"), "missing empty-state line in:\n{}", out);
    }

    #[test]
    fn single_online_node_with_latency_renders_ms() {
        let mut n = dto("alpha", NodeStateDto::Online);
        n.latency_ms = Some(42);
        let out = render_to_string(&[n]);
        assert!(out.contains("alpha"));
        assert!(out.contains("online"));
        assert!(out.contains("42ms"), "expected latency in ms in:\n{}", out);
        assert!(out.contains("1 nodes"));
    }

    #[test]
    fn single_online_node_without_latency_renders_em_dash() {
        // Online but latency_ms is None — fresh post-register before
        // any ping has completed. The renderer must not crash and
        // should show a placeholder.
        let n = dto("alpha", NodeStateDto::Online);
        let out = render_to_string(&[n]);
        assert!(out.contains("alpha"));
        assert!(out.contains("online"));
        // The em-dash character is what view.rs writes for unknown latency.
        assert!(out.contains("—"), "expected em-dash placeholder in:\n{}", out);
    }

    #[test]
    fn reconnecting_node_with_retry_renders_seconds_not_latency() {
        // Reconnecting nodes have meaningless latency_ms but should
        // show their countdown in seconds.
        let mut n = dto("beta", NodeStateDto::Reconnecting);
        n.latency_ms = Some(999); // would be wrong to display
        n.next_retry_in_ms = Some(4_000);
        let out = render_to_string(&[n]);
        assert!(out.contains("reconnecting"));
        assert!(out.contains("retry 4s"), "expected retry countdown in:\n{}", out);
        // Latency MUST NOT leak through in this state.
        assert!(!out.contains("999ms"), "latency ms must not show for reconnecting node:\n{}", out);
    }

    #[test]
    fn offline_node_with_no_retry_renders_em_dash() {
        let n = dto("gamma", NodeStateDto::Offline);
        let out = render_to_string(&[n]);
        assert!(out.contains("gamma"));
        assert!(out.contains("offline"));
        assert!(out.contains("—"));
    }

    #[test]
    fn multiple_nodes_render_in_input_order() {
        // The view doesn't sort — callers sort. So 'zeta' first should
        // appear before 'alpha' in the rendered output. This pins the
        // contract for the dashboard handler which feeds sorted snapshots.
        let nodes = vec![
            dto("zeta", NodeStateDto::Online),
            dto("alpha", NodeStateDto::Reconnecting),
        ];
        let out = render_to_string(&nodes);
        let zeta_pos = out.find("zeta").expect("zeta missing");
        let alpha_pos = out.find("alpha").expect("alpha missing");
        assert!(zeta_pos < alpha_pos, "input order not preserved:\n{}", out);
        assert!(out.contains("2 nodes"));
    }

    #[test]
    fn long_node_ids_do_not_corrupt_layout() {
        // The node_id column is `Constraint::Min(20)`. A node_id that
        // exceeds the column should be truncated visually by ratatui,
        // not panic or shift the other columns. We assert renders cleanly
        // and the status column is still parseable.
        let n = dto(
            "this-is-a-very-long-hostname-that-should-not-corrupt-the-display",
            NodeStateDto::Online,
        );
        let out = render_to_string(&[n]);
        assert!(out.contains("online"));
        // A node count line should still be readable on the header.
        assert!(out.contains("1 nodes"));
    }
}
