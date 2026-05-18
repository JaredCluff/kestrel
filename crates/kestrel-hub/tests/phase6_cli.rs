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
