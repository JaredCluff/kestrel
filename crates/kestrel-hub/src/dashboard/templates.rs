// crates/kestrel-hub/src/dashboard/templates.rs
use maud::{DOCTYPE, Markup, html};

use crate::events::{NodeState, NodeStatus};

/// Full dashboard page. `authed` controls whether write controls (Add /
/// Remove forms, Sign-out button) are rendered: read-only viewers see no
/// editing surface, signed-in operators see all of it. The sign-in /
/// sign-out link in the header always renders and points the other way
/// from the current state.
pub fn page(nodes: &[NodeStatus], authed: bool) -> Markup {
    page_inner(nodes, authed, None)
}

/// Same page, but with an inline error message banner above the table.
/// Used when a UI form submission fails (bad address, duplicate node,
/// etc.) so the operator stays on the dashboard with feedback rather
/// than being bounced to a separate error page.
pub fn page_with_error(nodes: &[NodeStatus], authed: bool, error: &str) -> Markup {
    page_inner(nodes, authed, Some(error))
}

fn page_inner(nodes: &[NodeStatus], authed: bool, error: Option<&str>) -> Markup {
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
                        // Auth indicator pushed to the right via CSS flex.
                        // Always present so the page never shifts when
                        // login state changes.
                        span.auth {
                            @if authed {
                                form method="post" action="/logout" {
                                    button.linkish type="submit" { "Sign out" }
                                }
                            } @else {
                                a href="/login" { "Sign in" }
                            }
                        }
                    }
                    @if let Some(msg) = error {
                        p.error { (msg) }
                    }
                    table hx-ext="sse" sse-connect="/sse" {
                        tbody sse-swap="nodes" {
                            (nodes_rows_with_controls(nodes, authed))
                        }
                    }
                    @if authed {
                        // Add-node form. POSTs to /ui/nodes which redirects
                        // back here on success. SameSite=Strict on the
                        // session cookie defends against drive-by CSRF;
                        // form action is same-origin.
                        form.addnode method="post" action="/ui/nodes" {
                            input type="text" name="node_id" placeholder="node id" required;
                            input type="text" name="address" placeholder="host:port" required;
                            button type="submit" { "Add node" }
                        }
                    }
                }
            }
        }
    }
}

/// Login page. Rendered on `GET /login` and on `POST /login` when the
/// supplied token doesn't match (in which case `error` is set and the
/// page also returns a 401 status so automation sees the failure).
///
/// Intentionally minimal: same Linear-style monochrome aesthetic as the
/// main dashboard, no extra CSS, no client-side validation, no autofocus
/// gymnastics. The form posts to itself.
pub fn login_page(error: Option<&str>) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "Kestrel — sign in" }
                link rel="stylesheet" href="/assets/dashboard.css";
            }
            body {
                main {
                    header { span { "Sign in" } }
                    form method="post" action="/login" {
                        // type=password so it doesn't shoulder-surf or leak
                        // into clipboard managers. autocomplete=current-
                        // password lets browser password managers remember
                        // it for the operator.
                        input
                            type="password"
                            name="token"
                            placeholder="control token"
                            required
                            autocomplete="current-password"
                            autofocus;
                        button type="submit" { "Continue" }
                    }
                    @if let Some(msg) = error {
                        p.error { (msg) }
                    }
                }
            }
        }
    }
}

/// Variant of `nodes_rows` that also renders a per-row Remove form when
/// `authed` is true. Used by the index render and by the SSE stream when
/// the SSE connection was opened from an authenticated browser. The
/// no-controls version (`nodes_rows`) is kept for tests and any
/// hypothetical future SSE consumer that wants pure read-only output.
pub fn nodes_rows_with_controls(nodes: &[NodeStatus], authed: bool) -> Markup {
    html! {
        @if nodes.is_empty() {
            tr {
                td.empty colspan=(if authed { "4" } else { "3" }) { "no nodes" }
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
                    @if authed {
                        td.actions {
                            // Inline form so the entire interaction is one
                            // round trip. The browser confirm() guards
                            // against an accidental click. The POST target
                            // is same-origin and SameSite=Strict on the
                            // session cookie defeats cross-site forgeries.
                            form
                                method="post"
                                action=(format!("/ui/nodes/{}/delete", n.node_id))
                                onsubmit=(format!(
                                    "return confirm('Remove {}?');",
                                    n.node_id
                                ))
                            {
                                button.linkish type="submit" { "Remove" }
                            }
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
        let html = page(&[], false).into_string();
        assert!(html.contains("/assets/dashboard.css"));
        assert!(html.contains("/assets/htmx.min.js"));
        assert!(html.contains("/assets/htmx-sse.js"));
        assert!(html.contains(r#"hx-ext="sse""#));
        assert!(html.contains(r#"sse-connect="/sse""#));
        assert!(html.contains(r#"sse-swap="nodes""#));
    }

    #[test]
    fn page_unauthed_shows_sign_in_link_and_no_addnode_form() {
        // Public viewers see the Sign-in link in the header but NO
        // write controls. Pins the "read-only when not signed in"
        // promise.
        let html = page(&[], false).into_string();
        assert!(html.contains(r#"href="/login""#));
        assert!(html.contains("Sign in"));
        assert!(!html.contains("Sign out"));
        assert!(!html.contains(r#"action="/ui/nodes""#));
    }

    #[test]
    fn page_authed_shows_sign_out_and_addnode_form() {
        // Signed-in operators see the add-node form and the sign-out
        // button. The sign-in link must be absent.
        let html = page(&[], true).into_string();
        assert!(html.contains(r#"action="/logout""#));
        assert!(html.contains("Sign out"));
        assert!(html.contains(r#"action="/ui/nodes""#));
        assert!(html.contains(r#"name="node_id""#));
        assert!(html.contains(r#"name="address""#));
        assert!(!html.contains(r#"href="/login""#));
    }

    #[test]
    fn rows_with_controls_authed_renders_remove_form_per_row() {
        let nodes = vec![
            node("a", NodeState::Online),
            node("b", NodeState::Offline),
        ];
        let html = nodes_rows_with_controls(&nodes, true).into_string();
        assert!(html.contains(r#"action="/ui/nodes/a/delete""#));
        assert!(html.contains(r#"action="/ui/nodes/b/delete""#));
        // The browser confirm() guard must be wired in so an accidental
        // click is not a silent destructive action.
        assert!(html.contains("confirm("));
    }

    #[test]
    fn rows_with_controls_unauthed_omits_remove_forms() {
        let nodes = vec![node("a", NodeState::Online)];
        let html = nodes_rows_with_controls(&nodes, false).into_string();
        assert!(!html.contains("/delete"));
        assert!(!html.contains("Remove"));
    }

    #[test]
    fn page_with_error_renders_error_banner() {
        let html = page_with_error(&[], true, "invalid address 'foo'").into_string();
        assert!(html.contains(r#"<p class="error">"#));
        assert!(html.contains("invalid address 'foo'"));
    }
}
