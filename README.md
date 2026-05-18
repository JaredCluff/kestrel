# Kestrel

Rust-native fleet control: TLS WebSocket transport, MCP-compatible operator hub, agents on every machine. Built for Claude Code to control multi-machine dev environments.

## Setup

On the hub host:

    cargo install --path crates/kestrel-hub
    kestrel-hub init --bind 0.0.0.0           # generates PSK, scaffolds kestrel.toml

This prints an enrollment command. Copy it to each node and run there:

    cargo install --path crates/kestrel-agent
    kestrel-agent enroll --hub <hub-ip> --key <hex-from-hub>

Back on the hub, register each node and start the hub:

    kestrel-hub add-node macstudio 192.168.1.10:7272
    kestrel-hub add-node linux-dev 192.168.1.20:7272
    kestrel-hub status                         # one-shot reachability check

> **Hot-reload:** `add-node` and `remove-node` apply live against a running hub
> (via its HTTP control endpoint at `:7273`). If the hub is down, the change
> takes effect at next `start`. Pass `--hub <url>` to target a non-local hub.

    kestrel-hub start                          # serves MCP via stdio + dashboard at :7273

In another terminal (or another host):

    kestrel-hub tui --hub http://<hub-ip>:7273 # live TUI dashboard

## Subcommand reference

### Hub

| Command | What it does |
|---|---|
| `init` | Generate PSK, store in keyring, scaffold `kestrel.toml` |
| `connect` | One-shot test: connect to each configured node, then exit |
| `start` | Long-running: supervisors + KVM + dashboard + MCP-on-stdio |
| `add-node <id> <addr>` | Append `[[hub.nodes]]` to config; applies live if hub is running |
| `remove-node <id>` | Remove a node from config; applies live if hub is running |
| `list-nodes` | Print configured nodes |
| `status` | Probe each configured node (one ping per node) |
| `layout-set <id> <col> <row>` | Set/update KVM grid position |
| `layout-unset <id>` | Remove a KVM grid entry |
| `tui [--hub URL]` | Interactive TUI against a running hub |

### Agent

| Command | What it does |
|---|---|
| `enroll --hub <ip> --key <hex>` | Store PSK + scaffold agent `kestrel.toml` |
| `start` | Run the agent's WSS server |
| `status` | Print loaded config + verify keyring PSK |
