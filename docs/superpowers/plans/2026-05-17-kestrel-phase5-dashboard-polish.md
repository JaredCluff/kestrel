# Kestrel Phase 5 — Dashboard + Polish

## Context

Phases 1–4 made the hub a functional MCP server: agents enroll, the hub fans out keystrokes, screenshots, clipboard, PTY shells, and accessibility trees to a fleet via WebSocket. But the hub itself has rough edges that show up the moment a node drops or someone tries to operate it as a real long-running daemon:

- **No auto-reconnect.** If a node drops, `run_actor` exits silently. `NodeRegistry` keeps a stale handle. MCP calls return a generic "actor channel closed" error. The hub has to be restarted to recover.
- **No event surface.** `tracing::info!` strings are the only signal that anything changed. Nothing can subscribe to connection state.
- **MCP errors are stringly typed.** `anyhow::Error::to_string()` flows straight back to Claude, with no node context or remediation hint.
- **Enrollment is awkward.** Hub-side onboarding is "edit `kestrel.toml` by hand, restart the binary."
- **No dashboard.** `listen_dashboard: SocketAddr` was pre-allocated in config but never bound.

Phase 5 fixes all four polish gaps and adds a web dashboard at `http://<hub>:7273` with deliberately restrained Linear-style aesthetics — sans-serif, high-contrast dark mode, single accent color, no gradients, no glassmorphism, no rounded-card chrome. The dashboard is the visible payoff; the connection lifecycle work is the foundation everything else stands on.

**Outcome:** A hub that survives node drops gracefully, exposes a structured event stream, gives Claude actionable error messages, lets you add nodes with a CLI subcommand, and renders a live status page that doesn't look AI-generated.

---

## Approach

Five tightly-linked changes, in order:

1. **Event surface** — Introduce `NodeEvent { Connected | Disconnected | Reconnecting }` and a `tokio::sync::broadcast` channel on `NodeRegistry`. Track per-node `NodeStatus` in a `RwLock<HashMap<String, NodeStatus>>` so the dashboard can render the union of live + offline nodes.

2. **Supervisor + auto-reconnect** — Refactor `transport::connect()` to return `(NodeHandle, JoinHandle<()>)` so the caller owns the actor task lifetime. Add `NodeSupervisor` which spawns a long-lived task per configured node: connect → register → await actor exit → emit `Disconnected` → exponential backoff (1s → 30s cap) → retry. Replace the one-shot connect loop in `Start` with one supervisor per configured node.

3. **Web dashboard** — Bind axum to `cfg.listen_dashboard`. Routes: `GET /` (full HTML), `GET /sse` (HTMX `text/event-stream` of fragment swaps), `GET /assets/*` (CSS + vendored HTMX). Server-rendered HTML via `maud` macros — no JS framework, no build step. SSE wraps the broadcast receiver from step 1.

4. **MCP error context** — Wrap each `NodeRegistry` method's error path with `format!("{op} on '{node_id}': {e} (hint: {hint})")` so MCP tool failures tell Claude *which* node, *what* operation, and *what to try*.

5. **Enrollment polish** — Add `kestrel-hub add-node <node_id> <address>` subcommand that reads `kestrel.toml`, appends a node, writes the file back, and prints "added; restart `kestrel-hub start` to connect." Hot-reload of running supervisors is out of scope for Phase 5.

### Aesthetic spec (the anti-AI checklist)

The user explicitly does NOT want the standard AI-UI tells. Enforce in code review:

- **Banned:** purple/pink/violet gradients, `bg-gradient-*` of any kind, glassmorphism (frosted blur), drop shadows on cards, emoji icons, "Get started in seconds" microcopy, animated borders, glowing buttons, Tailwind utility soup, Google Fonts imports.
- **Required:** one hand-written CSS file (~80 lines), CSS custom properties at the top for the palette (`--bg`, `--fg`, `--muted`, `--divider`, `--accent`). System sans (`-apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", sans-serif`). Dark mode by default; light mode via `prefers-color-scheme`. One accent color: a desaturated cyan-blue (`#6ea3e0`-ish) used sparingly for the online dot only. 1px dividers between rows, 24px vertical rhythm, 32–48px page padding, max-width 720px centered.
- **Layout:** A title (`KESTREL` or "Nodes" in muted small-caps), a single table-like list of nodes — `node_id` left, status mid, latency right. That's it. No nav. No footer. No tooltips. The page should be visibly server-rendered, not "an SPA pretending to be minimal."
- **HTMX usage:** Only `hx-ext="sse"` + `sse-connect="/sse"` + `sse-swap="nodes"` on the `<tbody>`. No `hx-trigger`, no `hx-target` gymnastics. SSE pushes a complete `<tbody>` fragment every state change.

This is concrete enough that the implementer cannot accidentally drift into "modern SaaS dashboard" defaults.

---

## File Map

```
kestrel/
  Cargo.toml                                  # MODIFY: + axum, maud, tower-http, tokio-stream, futures
  crates/kestrel-hub/
    Cargo.toml                                # MODIFY: + new deps
    assets/
      dashboard.css                           # NEW (~80 lines)
      htmx.min.js                             # NEW (vendored, ~14 KB, sse extension included)
    src/
      events.rs                               # NEW: NodeEvent enum, NodeStatus, NodeState
      supervisor.rs                           # NEW: NodeSupervisor::spawn(node_cfg, registry, psk)
      dashboard/
        mod.rs                                # NEW: axum Router builder, serve()
        templates.rs                          # NEW: maud HTML for full page + nodes fragment
        sse.rs                                # NEW: broadcast → SSE stream of fragment swaps
      router.rs                               # MODIFY: + status map, + broadcast tx, + subscribe()
      transport.rs                            # MODIFY: connect() returns (NodeHandle, JoinHandle<()>)
      main.rs                                 # MODIFY: supervisor loop, dashboard serve, add-node cmd
      config.rs                               # MODIFY: + write_to_file(); accept missing dashboard field gracefully for back-compat
      mcp.rs                                  # MODIFY: error wrapping at each tool call site (15 spots)
      lib.rs                                  # MODIFY: pub mod events, supervisor, dashboard
    tests/
      phase5_reconnect.rs                     # NEW: start agent → connect via supervisor → kill agent → re-start agent → assert reconnect within 5s
```

### Reuse, not reinvent

- **`transport::connect()`** already does TLS + handshake + spawns the actor. Phase 5 only changes its return shape to also hand back the `JoinHandle`. The handshake code stays.
- **`NodeRegistry`'s `register` / `get`** patterns continue working; we add `register_status`, `subscribe()`, and `status_snapshot()` alongside.
- **`rmcp`** stays the MCP transport. axum is additive, not a replacement.
- **`tracing`** stays for diagnostic logging. `NodeEvent` is a parallel structured channel, not a replacement for tracing.
- **`tokio::sync::broadcast`** is already a transitive dep via tokio — no new dep needed for the event channel itself.

---

## Implementation tasks

Each task is committed independently; spec-and-quality reviewed via subagent-driven-development.

### Task 1 — Workspace deps + `events.rs`

Add to workspace `Cargo.toml`:
```toml
axum         = { version = "0.7", features = ["macros"] }
tower-http   = { version = "0.5", features = ["fs"] }
tokio-stream = "0.1"
maud         = { version = "0.26", features = ["axum"] }
futures      = "0.3"
```

Add the four to `crates/kestrel-hub/Cargo.toml`. Create `crates/kestrel-hub/src/events.rs` with:

```rust
use kestrel_proto::OsInfo;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone)]
pub enum NodeEvent {
    Connected { node_id: String, os: OsInfo },
    Disconnected { node_id: String, attempt: u32, next_retry_in: Duration },
    Reconnecting { node_id: String, attempt: u32 },
}

#[derive(Debug, Clone)]
pub enum NodeState { Online, Offline, Reconnecting }

#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub node_id: String,
    pub state: NodeState,
    pub os: Option<OsInfo>,
    pub latency_ms: Option<u32>,
    pub last_seen: SystemTime,
    pub next_retry_in: Option<Duration>,
}
```

Export `pub mod events;` in `lib.rs`. Test: `cargo check -p kestrel-hub`.

### Task 2 — `NodeRegistry`: status map + broadcast channel

Modify `router.rs`. Add fields:

```rust
status: Arc<RwLock<HashMap<String, NodeStatus>>>,
event_tx: tokio::sync::broadcast::Sender<NodeEvent>,
```

`new()` constructs both (channel capacity 64). Add `subscribe() -> broadcast::Receiver<NodeEvent>`. Add `status_snapshot() -> Vec<NodeStatus>` (clones the inner Vec, sorted by node_id). Update `register()` to also write to `status` and `event_tx.send(NodeEvent::Connected)`. Add `mark_disconnected(node_id, attempt, next_retry_in)` and `mark_reconnecting(...)`. Keep existing methods unchanged. Add unit tests asserting `subscribe()` receives events after `register()`.

### Task 3 — `transport::connect()` returns `JoinHandle`

Change `pub async fn connect(addr, psk) -> anyhow::Result<NodeHandle>` to:

```rust
pub async fn connect(addr, psk) -> anyhow::Result<(NodeHandle, tokio::task::JoinHandle<()>)>;
```

`run_actor`'s task is no longer detached — return its `JoinHandle` to the caller. All existing call sites update accordingly: `Connect` and `Start` in `main.rs`, integration tests in `phase1.rs`/`phase2.rs`/`phase3.rs`/`phase4.rs`. They just drop the handle (one-line `let (handle, _) = ...`). Run full workspace test suite.

### Task 4 — `NodeSupervisor` with exponential backoff

Create `supervisor.rs`:

```rust
pub fn spawn(
    node_cfg: NodeConfig,
    registry: Arc<NodeRegistry>,
    psk: Vec<u8>,
) -> tokio::task::JoinHandle<()>;
```

Inside, an infinite `loop { ... }`:

1. Increment `attempt`. If `attempt > 1`, emit `Reconnecting`, then `sleep(backoff)`.
2. `transport::connect(addr, &psk).await` — if Err, mark disconnected with `next_retry_in = backoff_for(attempt)`, continue.
3. On Ok: `registry.register(handle).await` (which emits `Connected` and resets `attempt = 0`).
4. `actor_join_handle.await` — this resolves when the actor exits. Mark disconnected, increment `attempt`, compute next backoff.

Backoff: `Duration::from_millis(1000 * 2u64.pow(attempt.min(5)))` capped at 30s. Reset on successful connection.

### Task 5 — Wire `Start` to use supervisors

In `main.rs::Start`, replace the loop `for node in &cfg.nodes { transport::connect(...).await }` with `for node in &cfg.nodes { supervisor::spawn(node.clone(), registry.clone(), psk.clone()); }`. The supervisors run forever; the main task continues to start KVM + MCP. Initial connection failures no longer abort startup — they become "Reconnecting" status.

### Task 6 — Dashboard module: routes, templates, CSS

Create `dashboard/mod.rs`, `dashboard/templates.rs`. Maud templates: `page(status: &[NodeStatus])` and `nodes_table(status: &[NodeStatus])`. The page references `/assets/dashboard.css` and `/assets/htmx.min.js`. axum router:

```rust
Router::new()
    .route("/", get(index))
    .route("/sse", get(sse_stream))
    .nest_service("/assets", ServeDir::new("crates/kestrel-hub/assets"))
```

Hand-write `assets/dashboard.css` (~80 lines, exact palette + spacing per the Aesthetic spec). Vendor HTMX 2.0.4 + the SSE extension (single concatenated file, ~16 KB) at `assets/htmx.min.js`. Verify visually that the page renders as plain monochrome HTML in a text browser (`curl http://localhost:7273 | head -50`) before adding styling.

Decision point during implementation: if `ServeDir::new("crates/kestrel-hub/assets")` is awkward at runtime (path relative to CWD), inline the CSS and JS as `&'static [u8]` via `include_bytes!` and serve them from two more routes — but only if pathing causes friction. Default to `ServeDir`.

### Task 7 — SSE live updates

Create `dashboard/sse.rs`. The `/sse` handler returns `axum::response::sse::Sse<...>` driven by:

```rust
let mut rx = registry.subscribe();
let stream = async_stream::stream! {
    yield render_initial(&registry.status_snapshot()); // first event with current state
    while let Ok(_) = rx.recv().await {
        let snapshot = registry.status_snapshot();
        yield render_fragment(&snapshot);
    }
};
```

Each yielded event is a `sse::Event::default().event("nodes").data(html_string)`. The HTML string is the `<tbody>` fragment for the table. HTMX's `sse-swap="nodes"` on the `<tbody>` swaps in the new fragment.

Don't render every individual event; render the full snapshot on each event. Coalesce is implicit: if events arrive faster than the SSE flushes, the broadcast receiver may lag — handle `RecvError::Lagged` by re-rendering the snapshot anyway.

### Task 8 — Wire dashboard into `Start`

In `main.rs::Start`, after spawning supervisors and before `mcp.serve(stdio())`, spawn the axum server:

```rust
let listener = tokio::net::TcpListener::bind(cfg.listen_dashboard).await?;
tokio::spawn(async move { axum::serve(listener, dashboard::router(registry.clone())).await });
println!("Dashboard at http://{}", cfg.listen_dashboard);
```

Manually verify: `kestrel-hub start` → open `http://localhost:7273`. Kill an agent, watch its row flip to "offline" within 1s. Restart the agent, watch it flip back.

### Task 9 — MCP error context

In `crates/kestrel-hub/src/mcp.rs`, every `.await.map_err(|e| McpError::internal_error(e.to_string(), None))` becomes `.await.map_err(|e| McpError::internal_error(format!("{op} on '{node_id}': {e}"), None))` where `op` is a literal per tool (`"screenshot"`, `"shell_run"`, etc.). For 15 tools. Where applicable (node not connected), append a hint: `" (hint: check that the node is online — see /sse or kestrel-hub list)"`.

Don't introduce a new error type. Just consistent string formatting at each tool's call site.

### Task 10 — `add-node` subcommand + integration test

Add to `main.rs` `Command::AddNode { node_id: String, address: String, config: String }`. Implementation: read TOML as a `toml::Value`, splice in a new `[[hub.nodes]]` entry, write back via `std::fs::write`. Print "Added '{node_id}' at {address}. Restart `kestrel-hub start` to connect."

Add `crates/kestrel-hub/tests/phase5_reconnect.rs`:

```rust
#[tokio::test]
async fn supervisor_reconnects_after_agent_restart() {
    let (addr, shutdown1) = start_agent("recon-node").await;
    let registry = Arc::new(NodeRegistry::new());
    let mut events = registry.subscribe();
    kestrel_hub::supervisor::spawn(NodeConfig { node_id: "recon-node".into(), address: addr }, registry.clone(), test_psk());

    // 1. Connected
    assert!(matches!(events.recv().await.unwrap(), NodeEvent::Connected { .. }));

    // 2. Kill agent → Disconnected
    shutdown1.send(()).unwrap();
    let evt = tokio::time::timeout(Duration::from_secs(3), events.recv()).await.unwrap().unwrap();
    assert!(matches!(evt, NodeEvent::Disconnected { .. }));

    // 3. Restart agent on same addr → Connected again
    let (_, _shutdown2) = start_agent_on("recon-node", addr).await;
    let evt = tokio::time::timeout(Duration::from_secs(10), events.recv()).await.unwrap().unwrap();
    // May see one or more Reconnecting events first; loop until Connected
    // assert eventual Connected
}
```

---

## Verification

End-to-end manual test sequence:

1. `cargo build --release` — no warnings beyond pre-existing AX/ObjC ones.
2. `cargo test --workspace` — all green; `phase5_reconnect::supervisor_reconnects_after_agent_restart` passes.
3. `kestrel-hub init --bind 0.0.0.0` then create a `kestrel.toml` referencing one local agent, then `kestrel-hub start`. Output should include `Dashboard at http://0.0.0.0:7273`.
4. Open `http://localhost:7273` in browser. Verify:
   - Page is rendered in system sans, dark background, no gradients, no shadows, no rounded cards.
   - View source: HTML is server-rendered with no JS bundles beyond `/assets/htmx.min.js`; no inline `<style>` blocks beyond what's necessary; CSS is in one `dashboard.css` file.
   - Curl `http://localhost:7273` → readable plain HTML.
5. Kill the agent process. Within ~1s the node row should flip to "offline" with no page reload. Restart the agent. Within 1–3s the row flips back to "online".
6. From a separate terminal: `kestrel-hub add-node test-node 127.0.0.1:9999 --config kestrel.toml` — the file should gain a new `[[hub.nodes]]` entry.
7. Trigger a real MCP tool with a disconnected node: error message should read like `shell_run on 'test-node': node 'test-node' not connected (hint: check that the node is online — see /sse or kestrel-hub list)` rather than the prior bare `actor channel closed`.
8. **Aesthetic gate (do this with a colleague or yourself with fresh eyes):** Look at the dashboard. If it could pass for "I asked ChatGPT to build me a dashboard" — gradient header, glowy buttons, rounded cards on shadows, emoji status indicators, "Welcome to your fleet 🚀" copy — go back and strip until it reads as deliberate.

---

## Out of scope (deferred)

- TUI dashboard (deferred to Phase 6 if there's appetite)
- Dashboard auth (assumes the hub is on a trusted LAN; phase 6 can add a session cookie + token)
- Hot-reload of supervisors when `add-node` mutates config (requires SIGHUP or IPC; phase 6)
- Latency monitoring beyond what ping intervals give us (no per-tool latency yet)
- Per-node shell session pane in the dashboard (heavy; phase 6)
- Live screenshot thumbnails in the dashboard (heavy; phase 6)
