use std::net::SocketAddr;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct HubConfig {
    pub listen_mcp: String,
    pub listen_dashboard: SocketAddr,
    pub nodes: Vec<NodeConfig>,
    pub layout: Vec<NodeLayout>,
    /// Optional sandbox VM bootstrap config. When present, the Tart
    /// backend auto-installs the kestrel-agent into freshly provisioned
    /// VMs over SSH. Absent = today's behavior (operator installs the
    /// agent manually inside the VM image).
    pub sandbox_bootstrap: Option<crate::sandbox_bootstrap::SandboxBootstrapConfig>,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: String,
    pub address: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct NodeLayout {
    pub node_id: String,
    pub col: i32,
    pub row: i32,
}

impl HubConfig {
    /// Parse a TOML-formatted string into a HubConfig. Named explicitly
    /// rather than as a `FromStr` impl so the name doesn't shadow the
    /// trait — callers reading `HubConfig::from_str(...)` are clearly
    /// invoking this method, not a hypothetical `str.parse()` path.
    pub fn from_toml_str(s: &str) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct Raw { hub: RawHub, #[serde(default)] sandbox: Option<RawSandbox> }
        #[derive(Deserialize)]
        struct RawHub {
            listen_mcp: String,
            listen_dashboard: String,
            #[serde(default)]
            nodes: Vec<RawNode>,
            #[serde(default)]
            layout: Vec<RawLayout>,
        }
        #[derive(Deserialize)]
        struct RawSandbox {
            #[serde(default)]
            bootstrap: Option<crate::sandbox_bootstrap::SandboxBootstrapConfig>,
        }
        #[derive(Deserialize)]
        struct RawNode { node_id: String, address: String }
        #[derive(Deserialize)]
        struct RawLayout { node_id: String, position: RawPosition }
        #[derive(Deserialize)]
        struct RawPosition { col: i32, row: i32 }

        let raw: Raw = toml::from_str(s)?;
        Ok(HubConfig {
            listen_mcp: raw.hub.listen_mcp,
            listen_dashboard: raw.hub.listen_dashboard.parse()?,
            nodes: raw.hub.nodes.into_iter()
                .map(|n| -> anyhow::Result<NodeConfig> {
                    Ok(NodeConfig { node_id: n.node_id, address: n.address.parse()? })
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
            layout: raw.hub.layout.into_iter().map(|l| NodeLayout {
                node_id: l.node_id,
                col: l.position.col,
                row: l.position.row,
            }).collect(),
            sandbox_bootstrap: raw.sandbox.and_then(|s| s.bootstrap),
        })
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Self::from_toml_str(&std::fs::read_to_string(path)?)
    }
}

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

/// Like `remove_node`, but distinguishes three cases:
/// - `Ok(true)` — node was present and was removed.
/// - `Ok(false)` — node was legitimately absent from a **well-formed**
///   config (no `[[hub.nodes]]` entries at all, OR no entry matched).
/// - `Err` — structural problem: missing `[hub]` section, OR `hub.nodes`
///   exists but is not an array (e.g. `nodes = "foo"` instead of an array
///   of tables). Pass 8 split this from the `Ok(false)` case after Pass 7
///   noticed the original `let Some(...) = ... else` collapsed both.
///
/// The dashboard's DELETE handler uses this to return 500 on structural
/// errors and 404 on legitimate absence.
pub fn try_remove_node(doc: &mut toml::Value, node_id: &str) -> anyhow::Result<bool> {
    let hub = hub_table_mut(doc)?; // Err on missing/malformed [hub]
    // Split get_mut and as_array_mut so we can tell "no `nodes` key" apart
    // from "`nodes` key present but not an array".
    match hub.get_mut("nodes") {
        None => Ok(false), // well-formed empty-fleet
        Some(v) => match v.as_array_mut() {
            None => anyhow::bail!("hub.nodes is not an array"),
            Some(nodes) => {
                let before = nodes.len();
                nodes.retain(|n| {
                    n.as_table()
                        .and_then(|t| t.get("node_id"))
                        .and_then(|v| v.as_str()) != Some(node_id)
                });
                Ok(nodes.len() != before)
            }
        },
    }
}

pub fn set_layout(doc: &mut toml::Value, node_id: &str, col: i64, row: i64) -> anyhow::Result<()> {
    let hub = hub_table_mut(doc)?;
    let layout = hub
        .entry("layout")
        .or_insert_with(|| toml::Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("hub.layout is not an array"))?;
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

/// Variant of [`remove_layout`] that distinguishes "layout entry not
/// present" (`Ok(false)`) from structural config errors like
/// `hub.layout = "not an array"` (`Err`). Pairs with [`try_remove_node`]:
/// the dashboard delete endpoint maps these to 404 and 500 respectively
/// so the operator gets actionable feedback instead of a misleading 404
/// when their config is actually malformed.
pub fn try_remove_layout(doc: &mut toml::Value, node_id: &str) -> anyhow::Result<bool> {
    let hub = hub_table_mut(doc)?;
    match hub.get_mut("layout") {
        None => Ok(false),
        Some(v) => match v.as_array_mut() {
            None => anyhow::bail!("hub.layout is not an array"),
            Some(layout) => {
                let before = layout.len();
                layout.retain(|n| {
                    n.as_table()
                        .and_then(|t| t.get("node_id"))
                        .and_then(|v| v.as_str()) != Some(node_id)
                });
                Ok(layout.len() != before)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hub_config() {
        let s = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"

[[hub.nodes]]
node_id = "linux-dev"
address = "192.168.1.20:7272"

[[hub.nodes]]
node_id = "mac-studio"
address = "192.168.1.10:7272"

[[hub.layout]]
node_id = "mac-studio"
position = { col = 0, row = 0 }

[[hub.layout]]
node_id = "linux-dev"
position = { col = 1, row = 0 }
"#;
        let cfg = HubConfig::from_toml_str(s).unwrap();
        assert_eq!(cfg.nodes.len(), 2);
        assert_eq!(cfg.nodes[0].node_id, "linux-dev");
        assert_eq!(cfg.nodes[1].address.port(), 7272);
        assert_eq!(cfg.layout.len(), 2);
        assert_eq!(cfg.layout[0].col, 0);
        assert_eq!(cfg.layout[1].col, 1);
    }

    #[test]
    fn invalid_node_address_returns_err() {
        let s = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"

[[hub.nodes]]
node_id = "bad-node"
address = "not-a-valid-address"
"#;
        assert!(HubConfig::from_toml_str(s).is_err());
    }

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

    // ── Pass 6 coverage additions ────────────────────────────────────────────

    #[test]
    fn remove_layout_removes_matching_entry() {
        let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
[[hub.layout]]
node_id = "a"
position = { col = 0, row = 0 }
[[hub.layout]]
node_id = "b"
position = { col = 1, row = 0 }
"#;
        let mut doc: toml::Value = toml::from_str(toml).unwrap();
        super::remove_layout(&mut doc, "a").unwrap();
        let layout = doc["hub"]["layout"].as_array().unwrap();
        assert_eq!(layout.len(), 1);
        assert_eq!(layout[0]["node_id"].as_str().unwrap(), "b");
    }

    #[test]
    fn remove_layout_errors_on_missing_node() {
        let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
[[hub.layout]]
node_id = "a"
position = { col = 0, row = 0 }
"#;
        let mut doc: toml::Value = toml::from_str(toml).unwrap();
        let err = super::remove_layout(&mut doc, "ghost").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn remove_layout_errors_when_layout_array_missing() {
        let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#;
        let mut doc: toml::Value = toml::from_str(toml).unwrap();
        let err = super::remove_layout(&mut doc, "a").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn add_node_errors_when_hub_section_missing() {
        let toml = r#"[other]
foo = "bar"
"#;
        let mut doc: toml::Value = toml::from_str(toml).unwrap();
        let err = super::add_node(
            &mut doc,
            "a",
            "127.0.0.1:7272".parse().unwrap(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("[hub]"));
    }

    #[test]
    fn set_layout_errors_when_hub_section_missing() {
        let toml = "";
        let mut doc: toml::Value = toml::from_str(toml).unwrap();
        let err = super::set_layout(&mut doc, "a", 0, 0).unwrap_err();
        assert!(err.to_string().contains("[hub]"));
    }

    #[test]
    fn load_doc_errors_on_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.toml");
        let err = super::load_doc(path.to_str().unwrap()).unwrap_err();
        // Error message should name the path so the operator knows what to
        // look at.
        assert!(err.to_string().contains("does-not-exist.toml"));
    }

    #[test]
    fn load_doc_errors_on_invalid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken.toml");
        std::fs::write(&path, "this is not valid = [toml").unwrap();
        let err = super::load_doc(path.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("parse"));
    }

    #[test]
    fn save_doc_then_load_doc_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("k.toml");
        let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#;
        let mut doc: toml::Value = toml::from_str(toml).unwrap();
        super::add_node(&mut doc, "alpha", "127.0.0.1:7272".parse().unwrap()).unwrap();
        super::save_doc(path.to_str().unwrap(), &doc).unwrap();
        // Round-trip: load it back and verify.
        let loaded = super::load_doc(path.to_str().unwrap()).unwrap();
        let nodes = loaded["hub"]["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0]["node_id"].as_str().unwrap(), "alpha");
    }

    #[test]
    fn try_remove_node_returns_ok_false_when_no_nodes_key() {
        let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#;
        let mut doc: toml::Value = toml::from_str(toml).unwrap();
        // Empty-fleet config — try_remove returns Ok(false), not Err.
        assert!(!super::try_remove_node(&mut doc, "anything").unwrap());
    }

    #[test]
    fn try_remove_node_returns_true_when_present_false_when_not() {
        let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
[[hub.nodes]]
node_id = "alpha"
address = "127.0.0.1:7272"
"#;
        let mut doc: toml::Value = toml::from_str(toml).unwrap();
        assert!(super::try_remove_node(&mut doc, "alpha").unwrap());
        // Removing again — no longer present → Ok(false), not Err.
        assert!(!super::try_remove_node(&mut doc, "alpha").unwrap());
    }

    #[test]
    fn try_remove_node_errors_when_nodes_is_not_an_array() {
        // Operator with broken config: `nodes = "foo"` instead of `[[hub.nodes]]`.
        // Pass 8 fix: must return Err so the dashboard surfaces 500, not 404.
        let toml = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
nodes = "this is not an array"
"#;
        let mut doc: toml::Value = toml::from_str(toml).unwrap();
        let err = super::try_remove_node(&mut doc, "x").unwrap_err();
        assert!(
            err.to_string().contains("not an array"),
            "expected structural error mentioning 'not an array', got: {}",
            err
        );
    }

    #[test]
    fn try_remove_node_errors_when_hub_section_missing() {
        let toml = "[other]\nfoo = 1\n";
        let mut doc: toml::Value = toml::from_str(toml).unwrap();
        let err = super::try_remove_node(&mut doc, "x").unwrap_err();
        assert!(err.to_string().contains("[hub]"));
    }
}
