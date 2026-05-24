# Kestrel

Rust-native fleet control: TLS WebSocket transport, MCP-compatible operator hub, agents on every machine. Built for Claude Code to control multi-machine dev environments.

## Security model

Hub↔agent uses TLS WebSocket with a one-direction PSK-HMAC challenge-response (the agent challenges the hub), **bound to the TLS session via the TLS-exporter**. The agent generates a one-shot self-signed cert at startup (not pinned); a LAN MITM that terminates TLS on each leg sees a different exporter than the legitimate endpoint, so the proxied MAC won't verify on the far side and the connection dies.

**Per-node PSKs.** The hub stores a single `master_secret` in its system keyring. Each agent's PSK is `HKDF-SHA256(master_secret, info = "kestrel-node-psk-v1:" || node_id)`. The master never leaves the hub; only the derived per-node PSK is given to its agent. Consequences:

- A leaked agent PSK exposes only that one node — no other agent and not the master.
- Rotating the master invalidates every agent enrollment in one step (re-run `init` then re-enroll).
- Misconfiguration is self-detecting: an agent enrolled under `beta` cannot authenticate to a hub trying to reach it as `alpha`. Cross-node and cross-master PSK rejection are pinned by tests.

The hub's dashboard at `:7273` is plain HTTP. Read-only endpoints (HTML dashboard, `/api/nodes` GET, `/api/events` SSE) are open. Mutation endpoints (`POST`/`DELETE /api/nodes`) require a Bearer control token from the keyring. LAN-only assumed for the dashboard host.

**Out of scope (today):** mutual auth beyond the PSK.

**Dashboard TLS.** Default is plain HTTP (LAN-trusted). Pass `--dashboard-cert <pem>` and `--dashboard-key <pem>` to `kestrel-hub start` to serve HTTPS. Both flags must be provided together; either alone is rejected at startup.

**MCP audit log.** `kestrel-hub start --audit-log /var/log/kestrel.jsonl` appends one JSON Lines entry per MCP tool call (timestamp, tool, node_id, args summary, status, duration). `type_text`, `clipboard_write`, and `shell_write` log only the byte length of their text/data — never the bytes themselves; `shell_run` does log the command (highest-value audit signal).

## Next-gen surface (Phases 6–13)

Kestrel beyond the original Phase 1–5 MVP adds eight capability domains. Every MCP tool listed below is audited.

### World state — `world_state` / `world_diff_since`

The agent runs a `WorldObserver` that observes local state every ~2s (focused app, mouse position, clipboard fingerprint, displays, screen fingerprint via 8×8 luminance hash) and pushes deltas to the hub. The AI calls `world_state(node_id)` for a cheap JSON snapshot (~1–2 KB) and `world_diff_since(node_id, since_unix)` to get only what's new — no re-screenshotting needed to know "what app is on screen."

Dashboard surface: each row gets a live "Focused" column; `GET /api/world/:id` returns the cached state as JSON.

Schema: see `crates/kestrel-proto/src/world.rs`. Persistence to a JSONL dump across hub restarts is available via `NodeRegistry::persist_world_to` / `load_world_from`.

### Async long-running jobs — `job_start_shell` / `job_status` / `job_output` / `job_cancel`

Shell commands that take more than 30s block today's `shell_run`. Use the job pattern instead: `job_start_shell(node_id, command)` returns a job_id immediately; `job_status(job_id)` returns lifecycle (pending/running/completed/failed/cancelled); `job_output(job_id, since_offset)` streams output incrementally; `job_cancel(job_id)` aborts the spawn task.

### Capability advertisement + smart routing — `fleet_find`

Agents advertise `{os, has_gpu, has_display, has_sudo, has_docker}` in the handshake (after `SystemInfo`). The AI asks for a capability instead of hardcoding a `node_id`:

```jsonc
fleet_find({ has_gpu: true, has_display: true, os: "linux" })
// → ["dev-box-1", "dev-box-3"]
```

### Workflow choreography — `workflow_run`

Declarative cross-machine workflows. The AI specifies steps with targets (explicit `node` or capability `needs`), ops (`shell_run`, `screenshot`, `world_state`, `describe`, `type_text`, `clipboard_read`, `clipboard_write`), and per-step `on_error` (`continue` / `fail`). Earlier captures referenced via `${step_name.output}`. Conditional execution via `when: "step_name == ok"`.

```jsonc
workflow_run({
  steps: [
    { name: "build",  node: "linux-dev",  op: "shell_run", args: { command: "cargo build --release" } },
    { name: "deploy", needs: { has_sudo: true }, op: "shell_run",
      args: { command: "scp target/release/app remote:/usr/local/bin/" },
      when: "build == ok" }
  ],
  timeout_secs: 600
})
```

### Sandbox provisioning — `sandbox_spawn` / `sandbox_destroy` / `sandbox_list`

Ephemeral VMs on demand. Backend per host OS: **Tart** on macOS (`tart clone <image> <vm-name>`), **Lima** on Linux (`limactl start --name=<inst>`), **Hyper-V** on Windows (`New-VM -VHDPath`). The hub auto-tears-down after the TTL (default 1h). The relevant binary (`tart` / `limactl` / `powershell`) must be on the hub's PATH.

Agent installation into the new VM is the next step — operators currently bake the agent into the Tart image / Lima template / VHDX file.

### Multi-tenant identity + approval gates

Configure `kestrel-policy.toml` with users + per-op/per-node policy rules:

```toml
[[users]]
user_id = "alice@example.com"
bearer_token = "ak_..."
[[users.policies]]
op = "*"
node = "*"
action = { type = "allow" }
[[users.policies]]
op = "shell_*"
node = "production"
action = { type = "require_approval", approvers = ["bob@example.com"], ttl_secs = 60 }
```

Policy decision: `deny` > `require_approval` > `allow`; no matching rule = deny. Approval-gated requests block on the operator clicking approve in the dashboard `/api/approvals` queue. OIDC providers (Google, Okta, Auth0, Keycloak, custom) wire into the same `user_id` lookup via `crates/kestrel-hub/src/oidc.rs`. GitHub is intentionally absent — it doesn't expose OIDC for human users (only for Actions workflow tokens), so it can't be plugged in here.

### Plugin model — `plugin_list` / `plugin_invoke`

Vendor-extensible executables in `~/.kestrel/plugins/` on each agent speak JSON-RPC over stdio. ABI:

```jsonc
// Request from agent → plugin
{ "jsonrpc": "2.0", "id": 1, "method": "info", "params": null }
// → { "name": "myapp", "version": "1.0", "description": "...", "tools": ["select_layer"] }

{ "jsonrpc": "2.0", "id": 2, "method": "call", "params": { "tool": "select_layer", "args": { "name": "Background" } } }
// → { "result": ... }
```

AI calls `plugin_list(node_id)` to discover, `plugin_invoke(node_id, plugin, tool, args_json)` to run.

### WebRTC real-time streaming — `/api/webrtc/session`

Sub-second interactive screen streaming. Signalling layer + browser-side JS client are shipped (`/assets/webrtc.js`). The hub-side `RTCPeerConnection` + agent-side capture/encode pipeline is the next step; the signalling exchange (`POST /api/webrtc/session`, `/offer`, `/answer`, `/ice`) is testable against any WebRTC stack.

## Out of scope (or deferred)

- Mutual auth beyond the PSK
- Hub-side WebRTC `RTCPeerConnection` + RTP send (signalling layer + JS client are ready)
- Agent auto-install into freshly-provisioned sandboxes (bake the agent into the image for now)
- Wayland mouse position (Wayland intentionally doesn't expose this; X11 fallback is a TODO)
- Linux focused-app via AT-SPI returns the first application as a proxy for active; refining to true-active is a follow-up

## Setup

On the hub host:

    cargo install --path crates/kestrel-hub

    # Loopback (single-machine) — defaults to 127.0.0.1
    kestrel-hub init

    # OR LAN/fleet — explicit bind + dashboard so other machines can reach you
    kestrel-hub init --bind 192.168.1.10 --dashboard 192.168.1.10:7273

`init` generates a `master_secret` + a Bearer control token (both stored in the hub's system keyring) and scaffolds `kestrel.toml`. The per-node PSK for each agent is HKDF-derived from the master at `add-node` time, so the enrollment line varies per node.

Register each node, copy the printed enrollment line to that node's machine, and start the hub:

    kestrel-hub add-node macstudio 192.168.1.10:7272
    # → prints: kestrel-agent enroll --hub <hub-ip> --node-id macstudio --key <derived-hex>

    cargo install --path crates/kestrel-agent
    # On macstudio:
    kestrel-agent enroll --hub 192.168.1.10 --node-id macstudio --key <derived-hex>

You can re-derive a node's enrollment line at any time without rotating the fleet:

    kestrel-hub key macstudio --bind 192.168.1.10

Back on the hub:

    kestrel-hub status                         # one-shot reachability check

> **Hot-reload:** `add-node` and `remove-node` apply live against a running hub
> (via its HTTP control endpoint at `:7273`). If the hub is down, the change
> takes effect at next `start`. Pass `--hub <url>` to target a non-local hub.

> **Auth:** Mutations (`POST`/`DELETE` on `/api/nodes`) require a Bearer token
> generated by `kestrel-hub init`. Local CLI calls pick it up from the keyring
> automatically. To use `--hub` against a remote hub, copy the token printed by
> `init` and `export KESTREL_TOKEN=<value>` on the calling machine. Read-only
> endpoints (HTML dashboard, `/api/nodes` GET, `/api/events` SSE) stay open.

    kestrel-hub start                          # serves MCP via stdio + dashboard at :7273

In another terminal (or another host):

    kestrel-hub tui --hub http://<hub-ip>:7273 # live TUI dashboard

## Subcommand reference

### Hub

| Command | What it does |
|---|---|
| `init` | Generate `master_secret` + control token, store both in keyring, scaffold `kestrel.toml` |
| `connect` | Connect to each configured node, print result, block until Ctrl-C (smoke test) |
| `start` | Long-running: supervisors + KVM + dashboard + MCP-on-stdio |
| `add-node <id> <addr>` | Append `[[hub.nodes]]`; applies live if hub is running; prints the agent enrollment line |
| `remove-node <id>` | Remove a node from config; applies live if hub is running |
| `list-nodes` | Print configured nodes |
| `key <id> [--bind <ip>]` | Re-derive and print the agent enrollment line for `<id>` (deterministic — does not rotate) |
| `status` | Probe each configured node (one ping per node) |
| `layout-set <id> <col> <row>` | Set/update KVM grid position |
| `layout-unset <id>` | Remove a KVM grid entry |
| `tui [--hub URL]` | Interactive TUI against a running hub |
| `unenroll [--yes] [--keep-config]` | Clear `master_secret` + control token (and legacy `psk` entry) from keyring; delete `kestrel.toml` unless `--keep-config`. Dry-run unless `--yes`. |

### Agent

| Command | What it does |
|---|---|
| `enroll --hub <ip> --node-id <id> --key <hex>` | Store per-node PSK + scaffold agent `kestrel.toml` |
| `start` | Run the agent's WSS server |
| `status` | Print loaded config + verify keyring PSK |
| `unenroll [--yes] [--keep-config]` | Clear PSK from keyring; delete `kestrel.toml` unless `--keep-config`. Dry-run unless `--yes`. |

## Fuzzing

Two cargo-fuzz targets at `fuzz/` exercise the wire-facing parsers:

- `bincode_decode` — feeds arbitrary bytes into the KestrelMessage decoder. Asserts no panic on malformed input from a hostile WebSocket peer.
- `input_event_json` — feeds arbitrary bytes-via-lossy-UTF8 into the InputEvent JSON parser. Asserts no panic on malformed input from a browser tab over the WebRTC data channel.

Both run on demand, not in CI:

```bash
rustup install nightly
cargo install cargo-fuzz
cd fuzz
cargo +nightly fuzz run bincode_decode        # runs until you Ctrl-C
cargo +nightly fuzz run input_event_json
```

Crashes land in `fuzz/artifacts/<target>/`. Add them to a checked-in corpus (`fuzz/corpus/<target>/`) when you want them to live as regression cases — the corpus seeds future fuzz runs, ensuring the fix doesn't regress.

## Rotation playbook

### Rotate the master (full-fleet key rotation)

When you suspect the hub's `master_secret` has leaked (compromised laptop, stolen backup), rotate everything:

```bash
kestrel-hub rotate-master --bind <hub-lan-ip>            # dry-run; shows what will happen
kestrel-hub rotate-master --bind <hub-lan-ip> --yes      # do it; prints a new enrollment line per node
```

Run the printed enrollment line on each agent machine to install the new derived PSK, then restart the hub. Until every agent has re-enrolled, its supervisor will keep failing authentication — that's the desired property.

### Rotate one node's PSK (suspected single-agent compromise)

Per-node PSKs are deterministic from `(master_secret, node_id)`, so changing a single node's PSK requires changing one of those inputs. The lightest-touch path:

1. `kestrel-hub remove-node <id>` on the hub
2. `kestrel-agent unenroll --yes` on the compromised host
3. `kestrel-hub add-node <id>-v2 <addr>` on the hub (or any new id — the agent is going to re-enroll)
4. Run the printed enrollment line on the host

The other agents' PSKs are unaffected since `master_secret` didn't change.

## Migrating from shared-PSK installs

Older installs stored a single fleet-wide PSK under the `kestrel/psk` keyring entry and used the same key on every agent. New installs use a hub-side `master_secret` from which each agent's PSK is HKDF-derived per `node_id`. To migrate:

1. **Hub:** `kestrel-hub unenroll --yes` (clears the legacy `kestrel/psk` and the control token).
2. **Hub:** `kestrel-hub init --bind <hub-lan-ip>` (writes a fresh `master_secret` and prints next steps).
3. **For each existing agent:**
   - `kestrel-agent unenroll --yes` on the agent machine.
   - `kestrel-hub key <node_id> --bind <hub-lan-ip>` on the hub — copy the printed enrollment line.
   - Run it on the agent machine to re-enroll under the new derivation.
4. **Hub:** `kestrel-hub start`.

The `psk` keyring entry is best-effort-cleared by `unenroll` for older installs (NotFound is reported, not fatal). No on-disk `kestrel.toml` schema changed — only the keyring and the enrollment-line format.
