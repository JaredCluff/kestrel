// crates/kestrel-hub/src/dashboard/templates.rs
use maud::{DOCTYPE, Markup, html};

use crate::events::{NodeState, NodeStatus};

pub fn page(nodes: &[NodeStatus]) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "Kestrel" }
                link rel="stylesheet" href="/assets/dashboard.css";
                script src="/assets/htmx.min.js" {}
                script src="/assets/htmx-sse.js" {}
            }
            body {
                main {
                    header {
                        span { "Nodes" }
                        span.count { (nodes.len()) }
                    }
                    table hx-ext="sse" sse-connect="/sse" {
                        tbody sse-swap="nodes" {
                            (nodes_rows(nodes))
                        }
                    }
                }
            }
        }
    }
}

pub fn nodes_rows(nodes: &[NodeStatus]) -> Markup {
    html! {
        @if nodes.is_empty() {
            tr {
                td.empty colspan="3" { "no nodes" }
            }
        } @else {
            @for n in nodes {
                tr {
                    td.id { (n.node_id) }
                    td.status {
                        @match n.state {
                            NodeState::Online        => span.online   { "online" },
                            NodeState::Reconnecting  => span          { "reconnecting" },
                            NodeState::Offline       => span          { "offline" },
                        }
                    }
                    // Latency column doubles as a retry-countdown when the
                    // node is not Online (latency_ms is meaningless then).
                    td.latency {
                        @if matches!(n.state, NodeState::Online) {
                            @if let Some(ms) = n.latency_ms { (ms) "ms" }
                            @else { "—" }
                        } @else if let Some(retry) = n.next_retry_in {
                            "retry " (retry.as_secs()) "s"
                        } @else {
                            "—"
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn node(id: &str, state: NodeState) -> NodeStatus {
        NodeStatus {
            node_id: id.into(),
            state,
            os: None,
            latency_ms: None,
            last_seen: SystemTime::now(),
            next_retry_in: None,
        }
    }

    #[test]
    fn rows_render_empty_state() {
        let html = nodes_rows(&[]).into_string();
        assert!(html.contains("no nodes"), "expected empty-state row, got: {}", html);
    }

    #[test]
    fn rows_render_three_nodes_with_correct_state_classes() {
        let nodes = vec![
            node("a", NodeState::Online),
            node("b", NodeState::Reconnecting),
            node("c", NodeState::Offline),
        ];
        let html = nodes_rows(&nodes).into_string();
        // 3 rows
        assert_eq!(html.matches("<tr>").count(), 3);
        // Online uses the .online class; the other two do not.
        assert!(html.contains(r#"<span class="online">online</span>"#));
        assert!(!html.contains(r#"<span class="online">offline</span>"#));
        // node ids present
        assert!(html.contains(">a<"));
        assert!(html.contains(">b<"));
        assert!(html.contains(">c<"));
    }

    #[test]
    fn page_includes_htmx_and_css_links() {
        let html = page(&[]).into_string();
        assert!(html.contains("/assets/dashboard.css"));
        assert!(html.contains("/assets/htmx.min.js"));
        assert!(html.contains("/assets/htmx-sse.js"));
        assert!(html.contains(r#"hx-ext="sse""#));
        assert!(html.contains(r#"sse-connect="/sse""#));
        assert!(html.contains(r#"sse-swap="nodes""#));
    }
}
