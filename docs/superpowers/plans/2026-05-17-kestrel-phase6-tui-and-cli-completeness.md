# Kestrel Phase 6 — TUI Dashboard + CLI Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Kestrel fully configurable from the CLI (no hand-editing TOML) and ship a `kestrel-hub tui` subcommand that watches fleet status from any terminal.

**Architecture:** Add JSON twin endpoints (`GET /api/nodes`, `GET /api/events`) to the hub's existing axum dashboard server — the HTML SSE stream stays untouched. Extract config-mutation helpers into `config.rs` so `add-node`, `remove-node`, `list-nodes`, and `layout` all share one read-modify-write code path. The TUI is a separate subcommand that connects to a running hub via HTTP, subscribing to `/api/events` for live updates with the same restrained aesthetic as the web dashboard. The agent's `enroll` is extended to scaffold a starter agent TOML, and `kestrel-hub init` now writes a starter hub TOML — closing the manual-edit gap.

**Tech Stack additions:** `ratatui = "0.29"` (TUI), `crossterm = "0.28"` (terminal backend, already a transitive dep), `reqwest = "0.12"` (HTTP client for TUI, `rustls-tls` feature, no native TLS), `eventsource-client = "0.13"` (SSE client), `hostname = "0.4"` (default node_id for agent enroll).

---

## Context

Phases 1–5 built a working hub: TLS WS transport, 15 MCP tools, dashboard with auto-reconnect, structured events. Real-world use today still requires:
- Opening `kestrel.toml` in `$EDITOR` to write the `[hub]` section after `init`
- Opening another `kestrel.toml` on each agent host to set `node_id` and `listen` after `enroll`
- Restarting the hub after `add-node` (acceptable, but no way to confirm new node is reachable without that restart + dashboard)
- No way to remove a node, set KVM layout, or sanity-check connectivity without the dashboard running

The web dashboard is great for live monitoring, but you can't always open a browser — e.g., when SSH'd into the hub host. A TUI subcommand fills that gap.

**Outcome:** A first-time user runs `kestrel-hub init --bind 0.0.0.0` once, copies the printed enrollment command to each node, runs `kestrel-hub add-node <id> <addr>` on the hub once per node, runs `kestrel-hub start` — and that's it. Everything else (`status`, `list-nodes`, `remove-node`, `layout`, `tui`) is discoverable via `--help`.

---

## File Map

```
kestrel/
  Cargo.toml                                          # MODIFY: + ratatui, reqwest, eventsource-client, hostname
  crates/
    kestrel-hub/
      Cargo.toml                                      # MODIFY: + new deps
      src/
        config.rs                                     # MODIFY: + load_doc, save_doc, add_node, remove_node, set_layout, remove_layout
        dashboard/
          api.rs                                      # NEW: GET /api/nodes (JSON snapshot), GET /api/events (JSON SSE)
          mod.rs                                      # MODIFY: mount /api routes
        main.rs                                       # MODIFY: + list-nodes, remove-node, layout, status, tui subcommands
        enrollment.rs                                 # MODIFY: + scaffold_hub_config(path) — writes starter kestrel.toml
        status.rs                                     # NEW: probe configured nodes via transport::ping_once, return table rows
        tui/
          mod.rs                                      # NEW: TuiArgs, run() entry point, ratatui setup/teardown
          client.rs                                   # NEW: fetch /api/nodes, subscribe to /api/events
          view.rs                                     # NEW: ratatui rendering — node table + header
    kestrel-agent/
      Cargo.toml                                      # MODIFY: + hostname
      src/
        main.rs                                       # MODIFY: enroll writes starter agent TOML; + status subcommand
        config.rs                                     # MODIFY: + scaffold_agent_config(path, node_id, listen) — writes starter
```

---

## Implementation tasks

### Task 1: Hub config — extract reusable read/write/mutate helpers

**Files:**
- Modify: `crates/kestrel-hub/src/config.rs`

The current `HubConfig::from_str` and `add-node` handler each manage their own `toml::Value` navigation. Pull the shared logic into typed helpers so `list-nodes`, `remove-node`, and `layout` subcommands can reuse them.

- [ ] **Step 1: Write the failing tests**

Append to the `#[cfg(test)] mod tests` block in `crates/kestrel-hub/src/config.rs`:

```rust
#[test]
fn add_node_appends_to_document() {
    let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#;
    let mut doc: toml::Value = toml::from_str(toml).unwrap();
    super::add_node(&mut doc, "macstudio", "192.168.1.10:7272".parse().unwrap()).unwrap();
    let nodes = doc["hub"]["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["node_id"].as_str().unwrap(), "macstudio");
}

#[test]
fn add_node_refuses_duplicate() {
    let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
[[hub.nodes]]
node_id = "a"
address = "127.0.0.1:7272"
"#;
    let mut doc: toml::Value = toml::from_str(toml).unwrap();
    let err = super::add_node(&mut doc, "a", "127.0.0.1:7273".parse().unwrap()).unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

#[test]
fn remove_node_removes_matching_entry() {
    let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
[[hub.nodes]]
node_id = "a"
address = "127.0.0.1:7272"
[[hub.nodes]]
node_id = "b"
address = "127.0.0.1:7273"
"#;
    let mut doc: toml::Value = toml::from_str(toml).unwrap();
    super::remove_node(&mut doc, "a").unwrap();
    let nodes = doc["hub"]["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["node_id"].as_str().unwrap(), "b");
}

#[test]
fn remove_node_errors_on_missing() {
    let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#;
    let mut doc: toml::Value = toml::from_str(toml).unwrap();
    let err = super::remove_node(&mut doc, "ghost").unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[test]
fn set_layout_inserts_then_updates() {
    let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#;
    let mut doc: toml::Value = toml::from_str(toml).unwrap();
    super::set_layout(&mut doc, "a", 0, 0).unwrap();
    super::set_layout(&mut doc, "a", 1, 2).unwrap();
    let layout = doc["hub"]["layout"].as_array().unwrap();
    assert_eq!(layout.len(), 1);
    assert_eq!(layout[0]["position"]["col"].as_integer().unwrap(), 1);
    assert_eq!(layout[0]["position"]["row"].as_integer().unwrap(), 2);
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p kestrel-hub --lib config 2>&1 | tail -20
```

Expected: compile errors — `add_node`, `remove_node`, `set_layout` not defined.

- [ ] **Step 3: Implement the helpers**

Append to `crates/kestrel-hub/src/config.rs` (after the existing `impl HubConfig`):

```rust
/// Load `kestrel.toml` from `path` as a raw `toml::Value` for mutation.
pub fn load_doc(path: &str) -> anyhow::Result<toml::Value> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("read {}: {}", path, e))?;
    toml::from_str(&contents)
        .map_err(|e| anyhow::anyhow!("parse {}: {}", path, e))
}

/// Serialize `doc` and write it back to `path` (pretty-printed).
pub fn save_doc(path: &str, doc: &toml::Value) -> anyhow::Result<()> {
    let serialized = toml::to_string_pretty(doc)
        .map_err(|e| anyhow::anyhow!("serialize TOML: {}", e))?;
    std::fs::write(path, serialized)
        .map_err(|e| anyhow::anyhow!("write {}: {}", path, e))
}

fn hub_table_mut(doc: &mut toml::Value) -> anyhow::Result<&mut toml::value::Table> {
    doc.get_mut("hub")
        .and_then(|v| v.as_table_mut())
        .ok_or_else(|| anyhow::anyhow!("config has no [hub] section"))
}

pub fn add_node(doc: &mut toml::Value, node_id: &str, address: std::net::SocketAddr) -> anyhow::Result<()> {
    let hub = hub_table_mut(doc)?;
    let nodes = hub
        .entry("nodes")
        .or_insert_with(|| toml::Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("hub.nodes is not an array"))?;
    let duplicate = nodes.iter().any(|n| {
        n.as_table()
            .and_then(|t| t.get("node_id"))
            .and_then(|v| v.as_str()) == Some(node_id)
    });
    if duplicate {
        anyhow::bail!("node '{}' already exists", node_id);
    }
    let mut entry = toml::value::Table::new();
    entry.insert("node_id".into(), toml::Value::String(node_id.into()));
    entry.insert("address".into(), toml::Value::String(address.to_string()));
    nodes.push(toml::Value::Table(entry));
    Ok(())
}

pub fn remove_node(doc: &mut toml::Value, node_id: &str) -> anyhow::Result<()> {
    let hub = hub_table_mut(doc)?;
    let nodes = hub
        .get_mut("nodes")
        .and_then(|v| v.as_array_mut())
        .ok_or_else(|| anyhow::anyhow!("node '{}' not found", node_id))?;
    let before = nodes.len();
    nodes.retain(|n| {
        n.as_table()
            .and_then(|t| t.get("node_id"))
            .and_then(|v| v.as_str()) != Some(node_id)
    });
    if nodes.len() == before {
        anyhow::bail!("node '{}' not found", node_id);
    }
    Ok(())
}

pub fn set_layout(doc: &mut toml::Value, node_id: &str, col: i64, row: i64) -> anyhow::Result<()> {
    let hub = hub_table_mut(doc)?;
    let layout = hub
        .entry("layout")
        .or_insert_with(|| toml::Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("hub.layout is not an array"))?;
    // Remove any existing entry for this node_id, then insert fresh.
    layout.retain(|n| {
        n.as_table()
            .and_then(|t| t.get("node_id"))
            .and_then(|v| v.as_str()) != Some(node_id)
    });
    let mut position = toml::value::Table::new();
    position.insert("col".into(), toml::Value::Integer(col));
    position.insert("row".into(), toml::Value::Integer(row));
    let mut entry = toml::value::Table::new();
    entry.insert("node_id".into(), toml::Value::String(node_id.into()));
    entry.insert("position".into(), toml::Value::Table(position));
    layout.push(toml::Value::Table(entry));
    Ok(())
}

pub fn remove_layout(doc: &mut toml::Value, node_id: &str) -> anyhow::Result<()> {
    let hub = hub_table_mut(doc)?;
    let Some(layout) = hub.get_mut("layout").and_then(|v| v.as_array_mut()) else {
        anyhow::bail!("layout entry '{}' not found", node_id);
    };
    let before = layout.len();
    layout.retain(|n| {
        n.as_table()
            .and_then(|t| t.get("node_id"))
            .and_then(|v| v.as_str()) != Some(node_id)
    });
    if layout.len() == before {
        anyhow::bail!("layout entry '{}' not found", node_id);
    }
    Ok(())
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p kestrel-hub --lib config 2>&1 | tail -15
```

Expected: all 5 new tests + existing 2 pass = 7 config tests.

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-hub/src/config.rs
git commit -m "feat(hub): extract reusable config mutation helpers (add/remove/layout)"
```

---

### Task 2: Hub `init` scaffolds a starter `kestrel.toml`

**Files:**
- Modify: `crates/kestrel-hub/src/enrollment.rs`
- Modify: `crates/kestrel-hub/src/main.rs`

`kestrel-hub init` today only stores the PSK. The user is then on their own to write the `[hub]` section. Make it scaffold the TOML.

- [ ] **Step 1: Write the failing test**

Append to `crates/kestrel-hub/src/enrollment.rs`:

```rust
#[cfg(test)]
mod scaffold_tests {
    use super::*;

    #[test]
    fn scaffold_hub_config_writes_valid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kestrel.toml");
        let path_str = path.to_str().unwrap();
        scaffold_hub_config(path_str, "0.0.0.0:7273").unwrap();
        let contents = std::fs::read_to_string(path_str).unwrap();
        assert!(contents.contains("listen_mcp"));
        assert!(contents.contains("0.0.0.0:7273"));
        // Round-trip through HubConfig::from_str to ensure validity.
        crate::config::HubConfig::from_str(&contents).unwrap();
    }

    #[test]
    fn scaffold_hub_config_refuses_to_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kestrel.toml");
        let path_str = path.to_str().unwrap();
        std::fs::write(path_str, "existing").unwrap();
        let err = scaffold_hub_config(path_str, "0.0.0.0:7273").unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }
}
```

Add to `crates/kestrel-hub/Cargo.toml` `[dev-dependencies]`:

```toml
tempfile = "3"
```

(`tempfile` should also be added to the workspace `Cargo.toml` under `[workspace.dependencies]` as `tempfile = "3"`, and the hub's dev-dep then uses `{ workspace = true }` — preferred for consistency.)

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p kestrel-hub --lib scaffold 2>&1 | tail -10
```

Expected: compile error — `scaffold_hub_config` not defined.

- [ ] **Step 3: Implement scaffold_hub_config**

Append to `crates/kestrel-hub/src/enrollment.rs`:

```rust
/// Write a starter hub kestrel.toml at `path`. Refuses to overwrite if a file
/// already exists at that path.
pub fn scaffold_hub_config(path: &str, dashboard_addr: &str) -> anyhow::Result<()> {
    if std::path::Path::new(path).exists() {
        anyhow::bail!("{} already exists", path);
    }
    let contents = format!(
        r#"# Kestrel hub configuration. Edit by hand or use `kestrel-hub` subcommands
# (add-node, remove-node, layout) to mutate it programmatically.

[hub]
listen_mcp       = "stdio"
listen_dashboard = "{dashboard_addr}"

# Nodes the hub connects to. Add with `kestrel-hub add-node <id> <addr>`.

# Optional KVM layout for cursor-edge routing. Add with `kestrel-hub layout set <id> <col> <row>`.
"#,
        dashboard_addr = dashboard_addr,
    );
    std::fs::write(path, contents)
        .map_err(|e| anyhow::anyhow!("write {}: {}", path, e))
}
```

- [ ] **Step 4: Wire scaffold into the `init` subcommand**

In `crates/kestrel-hub/src/main.rs`, add a `--config` flag to `Init` and call `scaffold_hub_config` after `store_psk`:

```rust
Init {
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,
    #[arg(long, default_value = "kestrel.toml")]
    config: String,
    #[arg(long, default_value = "0.0.0.0:7273")]
    dashboard: String,
},
```

And the handler:

```rust
Command::Init { bind, config, dashboard } => {
    let psk = enrollment::generate_psk();
    enrollment::store_psk(&psk)?;
    match enrollment::scaffold_hub_config(&config, &dashboard) {
        Ok(()) => println!("Wrote starter config: {}", config),
        Err(e) => {
            // Already-exists is non-fatal — preserve the user's config.
            tracing::warn!("config not scaffolded: {}", e);
        }
    }
    println!("Key generated and stored in system credential store.");
    println!("Run this on each node machine:");
    println!("  {}", enrollment::enrollment_command(&bind, &psk));
    println!();
    println!("Then on this hub: kestrel-hub add-node <id> <addr> && kestrel-hub start");
}
```

- [ ] **Step 5: Run tests**

```bash
cargo test -p kestrel-hub --lib enrollment 2>&1 | tail -10
```

Expected: all 5 enrollment tests pass.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/kestrel-hub/Cargo.toml crates/kestrel-hub/src/enrollment.rs crates/kestrel-hub/src/main.rs
git commit -m "feat(hub): init scaffolds starter kestrel.toml and prints next-step hint"
```

---

### Task 3: Hub CLI — `list-nodes`, `remove-node`, `layout set/unset`

**Files:**
- Modify: `crates/kestrel-hub/src/main.rs`

Add four subcommands wired to the Task 1 helpers.

- [ ] **Step 1: Add the subcommand variants**

In `crates/kestrel-hub/src/main.rs` inside `enum Command`, after the existing `AddNode { ... }`, add:

```rust
/// Print configured nodes from kestrel.toml.
ListNodes {
    #[arg(long, default_value = "kestrel.toml")]
    config: String,
},
/// Remove a node from kestrel.toml.
RemoveNode {
    node_id: String,
    #[arg(long, default_value = "kestrel.toml")]
    config: String,
},
/// Set or update a KVM layout entry for a node.
LayoutSet {
    node_id: String,
    col: i64,
    row: i64,
    #[arg(long, default_value = "kestrel.toml")]
    config: String,
},
/// Remove a KVM layout entry.
LayoutUnset {
    node_id: String,
    #[arg(long, default_value = "kestrel.toml")]
    config: String,
},
```

- [ ] **Step 2: Add handlers**

In the `match cli.command` block, add arms after `AddNode`:

```rust
Command::ListNodes { config } => {
    let cfg = HubConfig::from_file(&config)?;
    if cfg.nodes.is_empty() {
        println!("(no nodes configured)");
    } else {
        for n in &cfg.nodes {
            println!("{:<24} {}", n.node_id, n.address);
        }
    }
}
Command::RemoveNode { node_id, config } => {
    let mut doc = kestrel_hub::config::load_doc(&config)?;
    kestrel_hub::config::remove_node(&mut doc, &node_id)?;
    kestrel_hub::config::save_doc(&config, &doc)?;
    println!("removed '{}'.", node_id);
}
Command::LayoutSet { node_id, col, row, config } => {
    let mut doc = kestrel_hub::config::load_doc(&config)?;
    kestrel_hub::config::set_layout(&mut doc, &node_id, col, row)?;
    kestrel_hub::config::save_doc(&config, &doc)?;
    println!("layout: '{}' -> ({}, {}).", node_id, col, row);
}
Command::LayoutUnset { node_id, config } => {
    let mut doc = kestrel_hub::config::load_doc(&config)?;
    kestrel_hub::config::remove_layout(&mut doc, &node_id)?;
    kestrel_hub::config::save_doc(&config, &doc)?;
    println!("layout cleared for '{}'.", node_id);
}
```

Also refactor the existing `AddNode` arm to use the same helpers (it currently inlines its own TOML mutation):

```rust
Command::AddNode { node_id, address, config } => {
    let address: std::net::SocketAddr = address.parse()
        .map_err(|e| anyhow::anyhow!("invalid address '{}': {}", address, e))?;
    let mut doc = kestrel_hub::config::load_doc(&config)?;
    kestrel_hub::config::add_node(&mut doc, &node_id, address)?;
    kestrel_hub::config::save_doc(&config, &doc)?;
    println!("added '{}' at {}. restart `kestrel-hub start` to connect.", node_id, address);
}
```

- [ ] **Step 3: Verify build + tests**

```bash
cargo build -p kestrel-hub 2>&1 | tail -10
cargo test -p kestrel-hub --lib 2>&1 | tail -10
```

Expected: clean build, 22+ lib tests still pass.

- [ ] **Step 4: Commit**

```bash
git add crates/kestrel-hub/src/main.rs
git commit -m "feat(hub): add list-nodes, remove-node, layout set/unset subcommands"
```

---

### Task 4: Hub `status` subcommand — probe configured nodes via ping_once

**Files:**
- Create: `crates/kestrel-hub/src/status.rs`
- Modify: `crates/kestrel-hub/src/lib.rs` (add `pub mod status;`)
- Modify: `crates/kestrel-hub/src/main.rs`

A one-shot ping pass over each configured node. Doesn't require a running hub — it's standalone. Useful for "did the network change?" / "is this agent up?" diagnostics.

- [ ] **Step 1: Write the failing test**

Create `crates/kestrel-hub/src/status.rs`:

```rust
// crates/kestrel-hub/src/status.rs
use std::time::Duration;

use crate::config::NodeConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeProbeResult {
    Reachable { rtt: Duration },
    Unreachable { reason: String },
}

#[derive(Debug, Clone)]
pub struct NodeProbe {
    pub node_id: String,
    pub address: std::net::SocketAddr,
    pub result: NodeProbeResult,
}

/// Probe one node by performing a single TLS handshake + Ping/Pong roundtrip,
/// then dropping the connection.
pub async fn probe_node(node: &NodeConfig, psk: &[u8], timeout: Duration) -> NodeProbe {
    let attempt = tokio::time::timeout(
        timeout,
        crate::transport::ping_once(node.address, psk),
    )
    .await;
    let result = match attempt {
        Ok(Ok(rtt)) => NodeProbeResult::Reachable { rtt },
        Ok(Err(e)) => NodeProbeResult::Unreachable { reason: e.to_string() },
        Err(_) => NodeProbeResult::Unreachable { reason: format!("timeout after {:?}", timeout) },
    };
    NodeProbe {
        node_id: node.node_id.clone(),
        address: node.address,
        result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn probe_unreachable_address_reports_unreachable() {
        let node = NodeConfig {
            node_id: "ghost".into(),
            address: "127.0.0.1:1".parse().unwrap(),  // port 1 is reserved and won't accept
        };
        let probe = probe_node(&node, b"any-psk-32-bytes-padded-padded!!", Duration::from_secs(2)).await;
        assert_eq!(probe.node_id, "ghost");
        assert!(matches!(probe.result, NodeProbeResult::Unreachable { .. }), "got: {:?}", probe.result);
    }
}
```

- [ ] **Step 2: Add to lib.rs**

Add `pub mod status;` to `crates/kestrel-hub/src/lib.rs` in alphabetical order.

- [ ] **Step 3: Run test**

```bash
cargo test -p kestrel-hub --lib status 2>&1 | tail -10
```

Expected: `probe_unreachable_address_reports_unreachable` passes (within ~2s).

- [ ] **Step 4: Wire up the subcommand**

In `crates/kestrel-hub/src/main.rs` add the variant:

```rust
/// Probe each configured node once and print reachability.
Status {
    #[arg(long, default_value = "kestrel.toml")]
    config: String,
    /// Per-node timeout in seconds.
    #[arg(long, default_value_t = 5)]
    timeout: u64,
},
```

And the handler:

```rust
Command::Status { config, timeout } => {
    let cfg = HubConfig::from_file(&config)?;
    let psk = enrollment::load_psk()?;
    if cfg.nodes.is_empty() {
        println!("(no nodes configured)");
    } else {
        for node in &cfg.nodes {
            let probe = kestrel_hub::status::probe_node(
                node,
                &psk,
                std::time::Duration::from_secs(timeout),
            )
            .await;
            match probe.result {
                kestrel_hub::status::NodeProbeResult::Reachable { rtt } => {
                    println!("{:<24} {:<24} online   {}ms", probe.node_id, probe.address, rtt.as_millis());
                }
                kestrel_hub::status::NodeProbeResult::Unreachable { reason } => {
                    println!("{:<24} {:<24} offline  ({})", probe.node_id, probe.address, reason);
                }
            }
        }
    }
}
```

- [ ] **Step 5: Run tests + build**

```bash
cargo build -p kestrel-hub 2>&1 | tail -10
cargo test -p kestrel-hub --lib status 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/kestrel-hub/src/status.rs crates/kestrel-hub/src/lib.rs crates/kestrel-hub/src/main.rs
git commit -m "feat(hub): add status subcommand that probes each configured node"
```

---

### Task 5: Agent CLI — `enroll` scaffolds config, add `status` subcommand

**Files:**
- Modify: `crates/kestrel-agent/Cargo.toml` (+ `hostname = { workspace = true }`)
- Modify: workspace `Cargo.toml` (+ `hostname = "0.4"`)
- Modify: `crates/kestrel-agent/src/config.rs` (add `scaffold_agent_config`)
- Modify: `crates/kestrel-agent/src/main.rs` (extend `Enroll`, add `Status`)

- [ ] **Step 1: Write the failing test**

Add to `crates/kestrel-agent/src/config.rs` test block:

```rust
#[test]
fn scaffold_agent_config_writes_valid_toml() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kestrel.toml");
    let path_str = path.to_str().unwrap();
    super::scaffold_agent_config(path_str, "macstudio", "0.0.0.0:7272").unwrap();
    let contents = std::fs::read_to_string(path_str).unwrap();
    assert!(contents.contains("node_id"));
    assert!(contents.contains("macstudio"));
    assert!(contents.contains("0.0.0.0:7272"));
}

#[test]
fn scaffold_agent_config_refuses_to_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kestrel.toml");
    let path_str = path.to_str().unwrap();
    std::fs::write(path_str, "existing").unwrap();
    let err = super::scaffold_agent_config(path_str, "x", "0.0.0.0:7272").unwrap_err();
    assert!(err.to_string().contains("already exists"));
}
```

Add to `crates/kestrel-agent/Cargo.toml`:

```toml
[dev-dependencies]
tempfile = { workspace = true }
```

(Workspace already has `tempfile` from Task 2.)

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p kestrel-agent scaffold 2>&1 | tail -10
```

Expected: compile error — `scaffold_agent_config` not defined.

- [ ] **Step 3: Implement scaffold_agent_config**

Append to `crates/kestrel-agent/src/config.rs`:

```rust
/// Write a starter agent kestrel.toml at `path`. Refuses to overwrite.
pub fn scaffold_agent_config(path: &str, node_id: &str, listen: &str) -> anyhow::Result<()> {
    if std::path::Path::new(path).exists() {
        anyhow::bail!("{} already exists", path);
    }
    let contents = format!(
        r#"# Kestrel agent configuration. The PSK lives in your system credential store,
# put there by `kestrel-agent enroll`.

[agent]
node_id = "{node_id}"
listen  = "{listen}"
"#,
        node_id = node_id,
        listen = listen,
    );
    std::fs::write(path, contents)
        .map_err(|e| anyhow::anyhow!("write {}: {}", path, e))
}
```

- [ ] **Step 4: Add `hostname` workspace dep**

In root `Cargo.toml` `[workspace.dependencies]`:

```toml
hostname = "0.4"
```

In `crates/kestrel-agent/Cargo.toml` `[dependencies]`:

```toml
hostname = { workspace = true }
```

- [ ] **Step 5: Extend the `Enroll` subcommand and add `Status`**

In `crates/kestrel-agent/src/main.rs`, update `enum Command`:

```rust
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
}
```

Update the `Enroll` handler:

```rust
Command::Enroll { hub, key, node_id, listen, config } => {
    let psk = hex::decode(&key).map_err(|e| anyhow::anyhow!("invalid hex key: {}", e))?;
    anyhow::ensure!(psk.len() == 32, "PSK must be 32 bytes (64 hex chars); got {}", psk.len());
    crate::enrollment::store_psk(&psk)?;
    let resolved_node_id = node_id.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "agent".into())
    });
    match crate::config::scaffold_agent_config(&config, &resolved_node_id, &listen) {
        Ok(()) => println!("Wrote starter config: {} (node_id={})", config, resolved_node_id),
        Err(e) => tracing::warn!("config not scaffolded: {}", e),
    }
    println!("Enrolled with hub at {}. Start the agent with: kestrel-agent start", hub);
}
```

(Note: this assumes `crate::enrollment` exists in the agent crate. Read `crates/kestrel-agent/src/main.rs` first to see how PSK storage is currently wired — the existing code stores via `keyring`. If `crate::enrollment` doesn't exist in the agent crate, call the keyring directly the same way the existing code does. Reuse the existing path.)

Add a `Status` arm:

```rust
Command::Status { config } => {
    let cfg = AgentConfig::from_file(&config)?;
    println!("node_id : {}", cfg.node_id);
    println!("listen  : {}", cfg.listen);
    let psk_present = keyring::Entry::new("kestrel", "psk")
        .and_then(|e| e.get_password())
        .is_ok();
    println!("psk     : {}", if psk_present { "(present in keyring)" } else { "(MISSING)" });
}
```

(Adjust the keyring service/account names to match the actual existing code — read `crates/kestrel-agent/src/main.rs` to confirm.)

- [ ] **Step 6: Run tests + build**

```bash
cargo build -p kestrel-agent 2>&1 | tail -10
cargo test -p kestrel-agent --lib config 2>&1 | tail -10
```

Expected: clean, scaffold tests pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/kestrel-agent/Cargo.toml crates/kestrel-agent/src/config.rs crates/kestrel-agent/src/main.rs
git commit -m "feat(agent): enroll scaffolds config and add status subcommand"
```

---

### Task 6: Hub JSON API endpoints (`/api/nodes`, `/api/events`)

**Files:**
- Create: `crates/kestrel-hub/src/dashboard/api.rs`
- Modify: `crates/kestrel-hub/src/dashboard/mod.rs`
- Modify: `crates/kestrel-hub/src/events.rs` (add `Serialize` derives)

The TUI needs machine-readable JSON. Keep the HTML SSE stream from Phase 5 untouched and add a parallel JSON twin at `/api/`.

- [ ] **Step 1: Add Serialize to NodeEvent and NodeStatus**

In `crates/kestrel-hub/src/events.rs`, update the derives so the types are JSON-serializable. Also expose `NodeState` similarly. The `kestrel_proto::OsInfo` already derives `Serialize`. The `Duration` and `SystemTime` types need `serde` features — check `Cargo.toml` to confirm `serde = { workspace = true, features = ["derive"] }` covers it; `Duration`/`SystemTime` serialize natively via serde without extra features.

Replace each `#[derive(Debug, Clone)]` on `NodeEvent`, `NodeStatus`, and `NodeState` with `#[derive(Debug, Clone, serde::Serialize)]`. For `NodeEvent`, additionally add `#[serde(tag = "type", rename_all = "snake_case")]`. For `NodeState`, add `#[serde(rename_all = "snake_case")]`. For the `Duration`/`SystemTime` fields, use `serde_with` or just convert at the API boundary — to avoid a new dep, convert `Duration` to `u64` ms and `SystemTime` to a unix timestamp inside the JSON DTO (see step 3).

Run:

```bash
cargo test -p kestrel-hub --lib events 2>&1 | tail -5
```

Expected: existing tests still pass.

- [ ] **Step 2: Write the failing test (JSON shape)**

Create `crates/kestrel-hub/src/dashboard/api.rs` with the test scaffolding (we'll add the implementation in step 3):

```rust
// crates/kestrel-hub/src/dashboard/api.rs

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{NodeState, NodeStatus};
    use std::time::{Duration, SystemTime};

    fn sample() -> NodeStatus {
        NodeStatus {
            node_id: "a".into(),
            state: NodeState::Online,
            os: None,
            latency_ms: Some(12),
            last_seen: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            next_retry_in: None,
        }
    }

    #[test]
    fn node_status_dto_serializes_with_unix_timestamp() {
        let dto: NodeStatusDto = (&sample()).into();
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains(r#""node_id":"a""#));
        assert!(json.contains(r#""state":"online""#));
        assert!(json.contains(r#""latency_ms":12"#));
        assert!(json.contains(r#""last_seen_unix":1700000000"#));
    }
}
```

- [ ] **Step 3: Implement the DTOs and handlers**

Replace `crates/kestrel-hub/src/dashboard/api.rs` with:

```rust
// crates/kestrel-hub/src/dashboard/api.rs
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, extract::State, response::sse::{Event, KeepAlive, Sse}};
use futures::stream::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::events::{NodeEvent, NodeState, NodeStatus};
use crate::router::NodeRegistry;

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeStatusDto {
    pub node_id: String,
    pub state: NodeStateDto,
    pub os_name: Option<String>,
    pub latency_ms: Option<u32>,
    pub last_seen_unix: u64,
    pub next_retry_in_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStateDto { Online, Offline, Reconnecting }

impl From<&NodeStatus> for NodeStatusDto {
    fn from(s: &NodeStatus) -> Self {
        NodeStatusDto {
            node_id: s.node_id.clone(),
            state: match s.state {
                NodeState::Online => NodeStateDto::Online,
                NodeState::Offline => NodeStateDto::Offline,
                NodeState::Reconnecting => NodeStateDto::Reconnecting,
            },
            os_name: s.os.as_ref().map(|o| o.name.clone()),
            latency_ms: s.latency_ms,
            last_seen_unix: s.last_seen.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            next_retry_in_ms: s.next_retry_in.map(|d| d.as_millis() as u64),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeEventDto {
    Connected { node_id: String, os_name: String },
    Disconnected { node_id: String, attempt: u32, next_retry_in_ms: u64 },
    Reconnecting { node_id: String, attempt: u32 },
}

impl From<&NodeEvent> for NodeEventDto {
    fn from(e: &NodeEvent) -> Self {
        match e {
            NodeEvent::Connected { node_id, os } => NodeEventDto::Connected {
                node_id: node_id.clone(),
                os_name: os.name.clone(),
            },
            NodeEvent::Disconnected { node_id, attempt, next_retry_in } => NodeEventDto::Disconnected {
                node_id: node_id.clone(),
                attempt: *attempt,
                next_retry_in_ms: next_retry_in.as_millis() as u64,
            },
            NodeEvent::Reconnecting { node_id, attempt } => NodeEventDto::Reconnecting {
                node_id: node_id.clone(),
                attempt: *attempt,
            },
        }
    }
}

pub async fn nodes_json(State(registry): State<Arc<NodeRegistry>>) -> Json<Vec<NodeStatusDto>> {
    let snap = registry.status_snapshot().await;
    Json(snap.iter().map(NodeStatusDto::from).collect())
}

pub fn events_stream(
    registry: Arc<NodeRegistry>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = registry.subscribe();
    let updates = BroadcastStream::new(rx).filter_map(|msg| match msg {
        Ok(evt) => {
            let dto: NodeEventDto = (&evt).into();
            let json = serde_json::to_string(&dto).unwrap_or_else(|_| "{}".into());
            Some(Ok(Event::default().event("event").data(json)))
        }
        Err(_) => None, // Lagged — drop, the client will refetch /api/nodes
    });
    Sse::new(updates).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

pub async fn events_handler(
    State(registry): State<Arc<NodeRegistry>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    events_stream(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{NodeState, NodeStatus};
    use std::time::{Duration, SystemTime};

    fn sample() -> NodeStatus {
        NodeStatus {
            node_id: "a".into(),
            state: NodeState::Online,
            os: None,
            latency_ms: Some(12),
            last_seen: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            next_retry_in: None,
        }
    }

    #[test]
    fn node_status_dto_serializes_with_unix_timestamp() {
        let dto: NodeStatusDto = (&sample()).into();
        let json = serde_json::to_string(&dto).unwrap();
        assert!(json.contains(r#""node_id":"a""#));
        assert!(json.contains(r#""state":"online""#));
        assert!(json.contains(r#""latency_ms":12"#));
        assert!(json.contains(r#""last_seen_unix":1700000000"#));
    }
}
```

- [ ] **Step 4: Wire the routes**

Update `crates/kestrel-hub/src/dashboard/mod.rs`:

Add `pub mod api;` near the other module declarations, then update `router(...)`:

```rust
Router::new()
    .route("/", get(index))
    .route("/sse", get(sse_handler))
    .route("/api/nodes", get(api::nodes_json))
    .route("/api/events", get(api::events_handler))
    .nest_service("/assets", ServeDir::new("crates/kestrel-hub/assets"))
    .with_state(state)
```

The existing `AppState` already carries `registry: Arc<NodeRegistry>`. The new handlers in `api.rs` use `State<Arc<NodeRegistry>>` directly. To make that work, also implement the FromRef extraction. The simplest approach: change `AppState` to be the `Arc<NodeRegistry>` directly:

Change `dashboard/mod.rs` to:

```rust
pub fn router(registry: Arc<NodeRegistry>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/sse", get(sse_handler))
        .route("/api/nodes", get(api::nodes_json))
        .route("/api/events", get(api::events_handler))
        .nest_service("/assets", ServeDir::new("crates/kestrel-hub/assets"))
        .with_state(registry)
}

async fn index(State(registry): State<Arc<NodeRegistry>>) -> maud::Markup {
    let snapshot = registry.status_snapshot().await;
    templates::page(&snapshot)
}

async fn sse_handler(State(registry): State<Arc<NodeRegistry>>) -> Sse<...> {
    sse::stream(registry)
}
```

Update `sse::stream` and `sse_handler` signatures accordingly. Remove the now-unused `AppState` struct.

- [ ] **Step 5: Run tests + build**

```bash
cargo build -p kestrel-hub 2>&1 | tail -10
cargo test -p kestrel-hub --lib dashboard 2>&1 | tail -15
```

Expected: clean build, all dashboard tests pass (including the new `node_status_dto_serializes_with_unix_timestamp`).

- [ ] **Step 6: Commit**

```bash
git add crates/kestrel-hub/src/dashboard/api.rs crates/kestrel-hub/src/dashboard/mod.rs crates/kestrel-hub/src/events.rs
git commit -m "feat(hub): add JSON twin endpoints /api/nodes and /api/events"
```

---

### Task 7: TUI — workspace deps + skeleton module

**Files:**
- Modify: workspace `Cargo.toml` (+ `ratatui`, `crossterm`, `reqwest`, `eventsource-client`)
- Modify: `crates/kestrel-hub/Cargo.toml` (+ the four)
- Create: `crates/kestrel-hub/src/tui/mod.rs`
- Create: `crates/kestrel-hub/src/tui/client.rs`
- Create: `crates/kestrel-hub/src/tui/view.rs`
- Modify: `crates/kestrel-hub/src/lib.rs` (+ `pub mod tui;`)

- [ ] **Step 1: Add workspace deps**

In root `Cargo.toml` `[workspace.dependencies]`:

```toml
ratatui            = "0.29"
crossterm          = "0.28"
reqwest            = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }
eventsource-client = "0.13"
```

In `crates/kestrel-hub/Cargo.toml` `[dependencies]`:

```toml
ratatui            = { workspace = true }
crossterm          = { workspace = true }
reqwest            = { workspace = true }
eventsource-client = { workspace = true }
```

- [ ] **Step 2: Skeleton client.rs**

Create `crates/kestrel-hub/src/tui/client.rs`:

```rust
// crates/kestrel-hub/src/tui/client.rs
use anyhow::Context;
use futures::stream::StreamExt;

use crate::dashboard::api::{NodeEventDto, NodeStatusDto};

/// HTTP client for a running kestrel-hub's JSON API.
pub struct HubClient {
    base_url: String,
    http: reqwest::Client,
}

impl HubClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        HubClient {
            base_url: base_url.into(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        }
    }

    pub async fn fetch_nodes(&self) -> anyhow::Result<Vec<NodeStatusDto>> {
        let url = format!("{}/api/nodes", self.base_url);
        let resp = self.http.get(&url).send().await
            .with_context(|| format!("GET {}", url))?;
        let nodes: Vec<NodeStatusDto> = resp.json().await
            .with_context(|| format!("decode JSON from {}", url))?;
        Ok(nodes)
    }

    /// Subscribe to /api/events. Returns a stream of parsed `NodeEventDto`s.
    /// Connection errors during the stream surface as `Err` items and the stream ends.
    pub fn subscribe_events(
        &self,
    ) -> impl futures::stream::Stream<Item = anyhow::Result<NodeEventDto>> {
        let url = format!("{}/api/events", self.base_url);
        let client = eventsource_client::ClientBuilder::for_url(&url)
            .expect("valid URL")
            .build();
        eventsource_client::Client::stream(&client).filter_map(|item| async move {
            match item {
                Ok(eventsource_client::SSE::Event(evt)) if evt.event_type == "event" => {
                    Some(serde_json::from_str::<NodeEventDto>(&evt.data)
                        .map_err(|e| anyhow::anyhow!("JSON decode failed: {} (body: {})", e, evt.data)))
                }
                Ok(_) => None, // comments, other event types, connect frames
                Err(e) => Some(Err(anyhow::anyhow!("SSE error: {:?}", e))),
            }
        })
    }
}
```

- [ ] **Step 3: Skeleton view.rs (just types for now)**

Create `crates/kestrel-hub/src/tui/view.rs`:

```rust
// crates/kestrel-hub/src/tui/view.rs
use crate::dashboard::api::{NodeStateDto, NodeStatusDto};

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Row, Table},
};

const ACCENT: Color = Color::Rgb(0x6e, 0xa3, 0xe0);
const MUTED: Color = Color::Rgb(0x6b, 0x6b, 0x6b);

pub fn render(f: &mut Frame, nodes: &[NodeStatusDto]) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(0)])
        .split(f.area());

    let header_line = Line::from(vec![
        Span::styled("KESTREL", Style::default().fg(MUTED).add_modifier(Modifier::BOLD)),
        Span::raw("    "),
        Span::styled(format!("{} nodes", nodes.len()), Style::default().fg(MUTED)),
    ]);
    f.render_widget(Paragraph::new(header_line), layout[0]);

    if nodes.is_empty() {
        let empty = Paragraph::new(Line::from(Span::styled("no nodes", Style::default().fg(MUTED))));
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
            let latency = n.latency_ms.map(|ms| format!("{}ms", ms)).unwrap_or_else(|| "—".into());
            Row::new(vec![
                Span::raw(n.node_id.clone()).into(),
                Span::styled(state_text, state_style).into(),
                Span::styled(latency, Style::default().fg(MUTED)).into(),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [Constraint::Min(20), Constraint::Length(16), Constraint::Length(10)],
    )
    .block(Block::default().borders(Borders::NONE));

    f.render_widget(table, layout[1]);
}
```

- [ ] **Step 4: Skeleton mod.rs (no event loop yet)**

Create `crates/kestrel-hub/src/tui/mod.rs`:

```rust
// crates/kestrel-hub/src/tui/mod.rs
pub mod client;
pub mod view;

#[derive(Debug, Clone)]
pub struct TuiArgs {
    pub hub_url: String,
}

/// Run the TUI to completion. Returns when the user presses 'q' or an unrecoverable
/// error occurs. The event loop is wired in Task 8.
pub async fn run(args: TuiArgs) -> anyhow::Result<()> {
    let client = client::HubClient::new(args.hub_url);
    let nodes = client.fetch_nodes().await?;
    println!("({} nodes — interactive TUI lands in task 8)", nodes.len());
    Ok(())
}
```

- [ ] **Step 5: Export from lib.rs**

Add `pub mod tui;` to `crates/kestrel-hub/src/lib.rs` (alphabetical, after `transport`).

- [ ] **Step 6: Verify build**

```bash
cargo build -p kestrel-hub 2>&1 | tail -15
```

Expected: clean build. (The TUI loop is not yet running — just the types compile.)

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates/kestrel-hub/Cargo.toml crates/kestrel-hub/src/tui/ crates/kestrel-hub/src/lib.rs
git commit -m "feat(hub): scaffold TUI module with HTTP client and ratatui view"
```

---

### Task 8: TUI — interactive event loop + `kestrel-hub tui` subcommand

**Files:**
- Modify: `crates/kestrel-hub/src/tui/mod.rs`
- Modify: `crates/kestrel-hub/src/main.rs`

- [ ] **Step 1: Implement the event loop**

Replace `crates/kestrel-hub/src/tui/mod.rs` with:

```rust
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
            // the event channel could be lossy.
            evt = events.next() => {
                match evt {
                    Some(Ok(_)) => {
                        nodes = client.fetch_nodes().await.unwrap_or(nodes);
                    }
                    Some(Err(_)) | None => {
                        // SSE dropped — reconnect on next iteration.
                        events = Box::pin(client.subscribe_events());
                    }
                }
            }
        }
    }
}
```

- [ ] **Step 2: Add `Tui` subcommand to main.rs**

In `crates/kestrel-hub/src/main.rs`:

```rust
/// Open the live TUI dashboard against a running hub.
Tui {
    /// Base URL of the running hub's dashboard HTTP server.
    #[arg(long, default_value = "http://127.0.0.1:7273")]
    hub: String,
},
```

Handler:

```rust
Command::Tui { hub } => {
    kestrel_hub::tui::run(kestrel_hub::tui::TuiArgs { hub_url: hub }).await?;
}
```

- [ ] **Step 3: Verify build + manual smoke**

```bash
cargo build -p kestrel-hub 2>&1 | tail -10
```

Expected: clean. The TUI is interactive and can't be unit-tested in CI; manual verification covers it.

- [ ] **Step 4: Commit**

```bash
git add crates/kestrel-hub/src/tui/mod.rs crates/kestrel-hub/src/main.rs
git commit -m "feat(hub): add interactive TUI subcommand with live SSE updates"
```

---

### Task 9: Integration test — `add-node`/`remove-node`/`list-nodes` round-trip

**Files:**
- Create: `crates/kestrel-hub/tests/phase6_cli.rs`

End-to-end test that exercises the new config mutation commands by invoking the helpers (not the CLI process) and asserting state.

- [ ] **Step 1: Create the test file**

```rust
// crates/kestrel-hub/tests/phase6_cli.rs
use kestrel_hub::config::{HubConfig, add_node, load_doc, remove_node, save_doc, set_layout};

fn starter_toml(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("kestrel.toml");
    let contents = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#;
    std::fs::write(&path, contents).unwrap();
    path
}

#[test]
fn add_then_list_then_remove_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path());
    let path_str = path.to_str().unwrap();

    // Add two nodes.
    let mut doc = load_doc(path_str).unwrap();
    add_node(&mut doc, "macstudio", "192.168.1.10:7272".parse().unwrap()).unwrap();
    add_node(&mut doc, "linux-dev", "192.168.1.20:7272".parse().unwrap()).unwrap();
    save_doc(path_str, &doc).unwrap();

    // List via HubConfig.
    let cfg = HubConfig::from_file(path_str).unwrap();
    let ids: Vec<&str> = cfg.nodes.iter().map(|n| n.node_id.as_str()).collect();
    assert_eq!(ids, vec!["macstudio", "linux-dev"]);

    // Remove one.
    let mut doc = load_doc(path_str).unwrap();
    remove_node(&mut doc, "macstudio").unwrap();
    save_doc(path_str, &doc).unwrap();

    let cfg = HubConfig::from_file(path_str).unwrap();
    let ids: Vec<&str> = cfg.nodes.iter().map(|n| n.node_id.as_str()).collect();
    assert_eq!(ids, vec!["linux-dev"]);
}

#[test]
fn set_layout_persists_position() {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path());
    let path_str = path.to_str().unwrap();

    let mut doc = load_doc(path_str).unwrap();
    set_layout(&mut doc, "macstudio", 0, 0).unwrap();
    set_layout(&mut doc, "linux-dev", 1, 0).unwrap();
    save_doc(path_str, &doc).unwrap();

    let cfg = HubConfig::from_file(path_str).unwrap();
    assert_eq!(cfg.layout.len(), 2);
    let mac = cfg.layout.iter().find(|l| l.node_id == "macstudio").unwrap();
    assert_eq!((mac.col, mac.row), (0, 0));
    let lin = cfg.layout.iter().find(|l| l.node_id == "linux-dev").unwrap();
    assert_eq!((lin.col, lin.row), (1, 0));
}
```

- [ ] **Step 2: Run the tests**

```bash
cargo test -p kestrel-hub --test phase6_cli 2>&1 | tail -10
```

Expected: 2 passed.

- [ ] **Step 3: Run all workspace tests**

```bash
cargo test --workspace 2>&1 | tail -20
```

Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add crates/kestrel-hub/tests/phase6_cli.rs
git commit -m "test(hub): add phase 6 CLI integration tests for config mutation round-trips"
```

---

### Task 10: README / CLAUDE.md — document the full CLI workflow

**Files:**
- Modify: `README.md` (or create if missing)

A short "Setup" section that walks through the now-complete CLI flow, plus a "Subcommand reference" table.

- [ ] **Step 1: Read existing README**

```bash
ls README.md 2>/dev/null || echo "(no README)"
cat README.md 2>/dev/null | head -40
```

If no README exists, create one.

- [ ] **Step 2: Add/update Setup section**

Append (or create) at the top of `README.md`:

```markdown
## Setup

On the hub host:

    cargo install --path crates/kestrel-hub
    kestrel-hub init --bind 0.0.0.0           # generates PSK, writes starter kestrel.toml

This prints an enrollment command. Copy it to each node and run there:

    cargo install --path crates/kestrel-agent
    kestrel-agent enroll --hub <hub-ip> --key <hex-from-hub>

Back on the hub, register each node and start the hub:

    kestrel-hub add-node macstudio 192.168.1.10:7272
    kestrel-hub add-node linux-dev 192.168.1.20:7272
    kestrel-hub status                          # one-shot reachability check
    kestrel-hub start                           # serves MCP via stdio + dashboard at :7273

In another terminal (or another host):

    kestrel-hub tui --hub http://<hub-ip>:7273  # live TUI dashboard

## Subcommand reference

| Hub command | What it does |
|---|---|
| `init` | Generate PSK, store in keyring, scaffold `kestrel.toml` |
| `connect` | One-shot test: connect to each configured node, then exit |
| `start` | Long-running: supervisors + KVM + dashboard + MCP-on-stdio |
| `add-node <id> <addr>` | Append `[[hub.nodes]]` to config |
| `remove-node <id>` | Remove a node from config |
| `list-nodes` | Print configured nodes |
| `status` | Probe each configured node (one ping per node) |
| `layout set <id> <col> <row>` | Set/update KVM grid position |
| `layout unset <id>` | Remove a KVM grid entry |
| `tui [--hub URL]` | Interactive TUI against a running hub |

| Agent command | What it does |
|---|---|
| `enroll --hub <ip> --key <hex>` | Store PSK + scaffold agent `kestrel.toml` |
| `start` | Run the agent's WSS server |
| `status` | Print loaded config + verify keyring PSK |
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: update README with full Phase 6 CLI workflow and subcommand reference"
```

---

## Verification

End-to-end manual test (single Mac, loopback):

1. `cargo build --release` — clean.
2. `cargo test --workspace` — all green (existing 47 tests + ~10 new).
3. `rm kestrel.toml` (if it exists), then `cargo run --bin kestrel-hub -- init --bind 127.0.0.1`. Verify a fresh `kestrel.toml` appears with `[hub]` section.
4. In a second terminal: `cargo run --bin kestrel-agent -- enroll --hub 127.0.0.1 --key $(cat /tmp/key)` (you need the key value from the init command — copy it). Verify a separate agent `kestrel.toml` appears.
5. `cargo run --bin kestrel-hub -- add-node loopback 127.0.0.1:7272`. Check `kestrel.toml`.
6. `cargo run --bin kestrel-hub -- list-nodes` → shows `loopback`.
7. `cargo run --bin kestrel-hub -- status` → shows `loopback ... offline (...)` (agent not running yet).
8. In another terminal: `cd /tmp/agent && cargo run --bin kestrel-agent -- start --config kestrel.toml`.
9. Back on hub: `cargo run --bin kestrel-hub -- status` → shows `loopback ... online ...ms`.
10. `cargo run --bin kestrel-hub -- start` — dashboard binds, MCP attaches to stdio.
11. In a fourth terminal: `cargo run --bin kestrel-hub -- tui --hub http://127.0.0.1:7273`. Verify:
    - Header renders "KESTREL    1 nodes" (or similar)
    - Single row showing `loopback` with `online` in accent color, latency in muted
    - Press `q` to quit cleanly
12. Kill the agent. Within ~1-3s the TUI row flips to `reconnecting` then `offline`. Restart the agent — flips back to `online`.
13. `cargo run --bin kestrel-hub -- remove-node loopback` + `cargo run --bin kestrel-hub -- list-nodes` → `(no nodes configured)`.
14. **Aesthetic gate (TUI):** No emoji, no box-drawing chrome beyond what ratatui adds for tables. One accent color used only for `online`. Reads as deliberate.

---

## Out of scope (deferred to Phase 7)

- Hot-reload of supervisors when `add-node`/`remove-node` mutates a running hub (still requires restart)
- Dashboard auth / signed URLs (LAN-only assumed)
- TUI interactivity beyond `q`/`r` (no node selection, no in-TUI shell pane, no command palette)
- TUI snapshot tests (would need `insta` or similar; out of scope)
- Embedded assets via `include_bytes!` for deployable hub binary (relies on CWD today)
- Agent `unenroll` to clear keyring (could land here but adds risk of accidental data loss)
- Per-tool latency tracking in the dashboard (no Pings beyond keepalive)
