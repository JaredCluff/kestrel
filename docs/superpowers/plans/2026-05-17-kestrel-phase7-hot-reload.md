# Kestrel Phase 7 — Hot-Reload of Supervisors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `kestrel-hub add-node` and `kestrel-hub remove-node` take effect immediately against a running hub — no "restart `kestrel-hub start` to apply" line. CLI still works when the hub is down (falls back to file mutation).

**Architecture:** Promote `AppState` to carry the config path, PSK, and a `SupervisorMap` (`HashMap<String, JoinHandle<()>>`) alongside the registry. Add `POST /api/nodes` and `DELETE /api/nodes/:node_id` handlers that mutate the config file (under a tokio Mutex), reconcile the in-memory supervisor map, and rely on the existing event broadcast to notify SSE consumers. The CLI tries HTTP first (1s timeout); on connection-refused or timeout, it falls back to the existing file-mutation path with a "restart hub" message.

**Tech Stack:** No new deps. Reuses `axum`, `reqwest`, `tokio::sync::Mutex`, the existing `config::{add_node, remove_node}` helpers, the existing `supervisor::spawn`.

---

## Context

After Phase 6, `kestrel-hub add-node alpha 192.168.1.10:7272` writes the TOML but prints `restart \`kestrel-hub start\` to connect`. A live hub keeps running with the old node list. The user has to ssh in, kill the hub process, and restart it — at which point the dashboard goes dark for several seconds and any in-flight MCP calls fail.

**Outcome:** After this PR, the same `add-node` command applies live. The supervisor spawns within ~50ms, the dashboard's SSE pushes a new row, and the TUI updates without a refresh keypress. Same for `remove-node` — the supervisor is aborted, the row disappears, and MCP calls to that node fast-fail with the same "not connected" hint they already produce today.

---

## Approach

Four tightly-linked changes:

1. **`SupervisorMap` and richer AppState.** Promote `AppState` from "registry only" to "registry + config_path + psk + supervisors + config_write_lock". The supervisors map is the new piece — keyed by node_id so handlers can abort one without aborting all. The config_write_lock is a `tokio::sync::Mutex<()>` that serializes file mutations.

2. **`NodeRegistry::forget_node(node_id)`.** Removes the entry from both `nodes` and `status` maps and emits a `Disconnected` event so SSE consumers refresh. Dropping the NodeHandle closes its `cmd_tx`, which lets the actor task exit naturally — no explicit actor abort needed.

3. **`POST /api/nodes` and `DELETE /api/nodes/:node_id`.** Two new handlers in `dashboard/api.rs`. Both acquire the config write lock, load the file, mutate, save, then reconcile the supervisor map. POST spawns a new supervisor; DELETE aborts the existing one and forgets the node from the registry.

4. **CLI `add-node`/`remove-node` try HTTP first.** They build a `reqwest::Client` with a 1s connect timeout, attempt the HTTP call, and fall back to file mutation on `ConnectError`/timeout. Success messages are different in each case ("(live via http://...)" vs "restart hub to apply").

### Design notes

- **The supervisor map is on AppState, not on NodeRegistry.** Registry tracks live connections; supervisors track the desired set of configured nodes. Two different lifecycles. Keeping them separate avoids muddling "is this node connected right now?" with "do we have a reconnect loop running for it?".

- **The config_write_lock is a `tokio::sync::Mutex<()>` not a `RwLock`.** Writes happen under the lock, reads (via `load_doc`) happen outside it. Race window is tiny (one read-modify-write cycle, typically <10ms). A real production system might prefer a single owner thread with mpsc commands, but for a LAN tool this is fine.

- **Actor cleanup is implicit.** When `forget_node` removes the NodeHandle from the registry's `nodes` map, the `Arc<NodeHandle>` ref count drops, `cmd_tx` is dropped, the actor's `cmd_rx.recv()` returns None, the actor exits. This was already the case today — Phase 7 just relies on it.

- **No auth on the HTTP endpoints.** Same threat model as the dashboard (LAN-only assumed). A `Authorization: Bearer <token>` flow can land in a future phase if needed.

- **Layout edits don't hot-reload yet.** `layout-set` / `layout-unset` still require a hub restart to take effect. Out of scope for Phase 7 — the KVM router would need its own reconciliation path.

---

## File Map

```
kestrel/
  crates/kestrel-hub/
    src/
      dashboard/
        mod.rs                    # MODIFY: extend AppState; mount POST/DELETE routes
        api.rs                    # MODIFY: + AddNodeArgs DTO + post_node/delete_node handlers
      router.rs                   # MODIFY: + NodeRegistry::forget_node
      supervisor.rs               # MODIFY: ensure spawn() works for hot-reload (no signature change)
      client.rs                   # NEW: HubControlClient (reqwest wrapper for POST/DELETE)
      lib.rs                      # MODIFY: + pub mod client
      main.rs                     # MODIFY: thread config_path/psk into AppState; CLI tries HTTP
    tests/
      phase7_hot_reload.rs        # NEW: integration test for POST/DELETE + supervisor reconcile
```

---

## Implementation Tasks

### Task 1: `NodeRegistry::forget_node`

**Files:**
- Modify: `crates/kestrel-hub/src/router.rs`

- [ ] **Step 1: Write the failing tests**

Append to the `#[cfg(test)] mod tests` block in `crates/kestrel-hub/src/router.rs`:

```rust
#[tokio::test]
async fn forget_node_removes_from_status_and_emits_disconnect() {
    let r = NodeRegistry::new();
    let mut rx = r.subscribe();
    r.mark_reconnecting("a", 1).await;
    let _ = rx.recv().await; // consume Reconnecting

    r.forget_node("a").await;

    // Status row gone.
    let snap = r.status_snapshot().await;
    assert!(snap.iter().all(|s| s.node_id != "a"));

    // Disconnected event broadcast.
    let evt = rx.recv().await.unwrap();
    match evt {
        NodeEvent::Disconnected { node_id, .. } => assert_eq!(node_id, "a"),
        other => panic!("expected Disconnected, got {:?}", other),
    }
}

#[tokio::test]
async fn forget_node_is_idempotent() {
    let r = NodeRegistry::new();
    r.forget_node("ghost").await; // no panic; just emits an event with attempt=0
    r.forget_node("ghost").await; // still no panic
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p kestrel-hub --lib forget_node 2>&1 | tail -10
```

Expected: compile error — `forget_node` not defined.

- [ ] **Step 3: Implement `forget_node`**

Append this method to the `impl NodeRegistry` block, after `mark_reconnecting`:

```rust
/// Remove a node from both `nodes` and `status` maps and broadcast a
/// terminal `Disconnected` event. Idempotent — repeated calls for the same
/// node_id are safe and each still emits the event.
pub async fn forget_node(&self, node_id: &str) {
    self.nodes.write().await.remove(node_id);
    self.status.write().await.remove(node_id);
    let _ = self.event_tx.send(NodeEvent::Disconnected {
        node_id: node_id.to_string(),
        attempt: 0,
        next_retry_in: Duration::from_secs(0),
    });
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p kestrel-hub --lib forget_node 2>&1 | tail -10
```

Expected: both new tests pass. Existing `subscribe_receives_disconnect_event`, `status_snapshot_includes_reconnecting`, `status_snapshot_sorted_by_node_id` still pass.

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-hub/src/router.rs
git commit -m "feat(hub): add NodeRegistry::forget_node for hot-reload removal"
```

---

### Task 2: `AppState` carries config path, PSK, and supervisor map

**Files:**
- Modify: `crates/kestrel-hub/src/dashboard/mod.rs`

- [ ] **Step 1: Read the current AppState**

The current state is something like:

```rust
#[derive(Clone)]
struct AppState {
    registry: Arc<NodeRegistry>,
}
```

And `Router::with_state(state)` plus a `FromRef<AppState> for Arc<NodeRegistry>` impl that lets the JSON API handlers extract just the registry.

- [ ] **Step 2: Define `SupervisorMap` type alias and the new AppState**

Replace `dashboard/mod.rs`'s AppState definition (and the existing `FromRef` impl) with:

```rust
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Map of node_id → live supervisor task handle.
/// Hot-reload mutates this under the `config_write_lock` to keep file + memory in sync.
pub type SupervisorMap = Arc<tokio::sync::RwLock<std::collections::HashMap<String, JoinHandle<()>>>>;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<crate::router::NodeRegistry>,
    pub config_path: String,
    pub psk: Vec<u8>,
    pub supervisors: SupervisorMap,
    /// Serializes config file read-modify-write cycles across concurrent HTTP requests.
    pub config_write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl AppState {
    pub fn new(
        registry: Arc<crate::router::NodeRegistry>,
        config_path: String,
        psk: Vec<u8>,
    ) -> Self {
        AppState {
            registry,
            config_path,
            psk,
            supervisors: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            config_write_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

// Existing FromRef impl stays — extract Arc<NodeRegistry> for handlers that only need that.
impl axum::extract::FromRef<AppState> for Arc<crate::router::NodeRegistry> {
    fn from_ref(state: &AppState) -> Self {
        state.registry.clone()
    }
}
```

- [ ] **Step 3: Update `router(...)` signature**

Change the signature from `pub fn router(registry: Arc<NodeRegistry>) -> Router` to:

```rust
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/sse", get(sse_handler))
        .route("/api/nodes", get(api::nodes_json).post(api::post_node))
        .route("/api/nodes/:node_id", axum::routing::delete(api::delete_node))
        .route("/api/events", get(api::events_handler))
        .nest_service("/assets", ServeDir::new("crates/kestrel-hub/assets"))
        .with_state(state)
}
```

`api::post_node` and `api::delete_node` don't exist yet — Task 3 adds them. For now leave them as placeholder routes (compile error is fine; we add the handlers next).

Actually, to keep this task green: temporarily add stub handlers in `dashboard/mod.rs` that return `axum::http::StatusCode::NOT_IMPLEMENTED`:

```rust
// Temporary stubs — Task 3 replaces these with real handlers in api.rs.
async fn post_node_stub() -> axum::http::StatusCode {
    axum::http::StatusCode::NOT_IMPLEMENTED
}
async fn delete_node_stub() -> axum::http::StatusCode {
    axum::http::StatusCode::NOT_IMPLEMENTED
}
```

And use those names in the route definitions instead of `api::post_node`/`api::delete_node`. Task 3 will switch the routes over.

- [ ] **Step 4: Update `main.rs` to construct the new AppState**

In `crates/kestrel-hub/src/main.rs`, `Command::Start`, change the dashboard setup. Find the section that does:

```rust
let dashboard_handle = tokio::spawn(async move {
    if let Err(e) = axum::serve(dash_listener, dashboard::router(dash_registry)).await {
        ...
    }
});
```

Replace `dashboard::router(dash_registry)` with `dashboard::router(state.clone())` where `state` is built earlier in the function:

```rust
let state = dashboard::AppState::new(registry.clone(), config.clone(), psk.clone());

// Spawn supervisors, tracking their handles in state.supervisors:
for node in &cfg.nodes {
    let handle = supervisor::spawn(node.clone(), registry.clone(), psk.clone());
    state.supervisors.write().await.insert(node.node_id.clone(), handle);
    println!("supervising: {} ({})", node.node_id, node.address);
}

// Then the dashboard task:
let dash_listener = tokio::net::TcpListener::bind(cfg.listen_dashboard)
    .await
    .map_err(|e| anyhow::anyhow!("dashboard bind to {} failed: {}", cfg.listen_dashboard, e))?;
let dash_addr = dash_listener.local_addr()?;
let dash_state = state.clone();
let dashboard_handle = tokio::spawn(async move {
    if let Err(e) = axum::serve(dash_listener, dashboard::router(dash_state)).await {
        tracing::error!("dashboard server error: {}", e);
    }
});
println!("Dashboard at http://{}", dash_addr);
```

And update the cleanup at the end of `Start`:

```rust
// Best-effort cleanup — abort all supervisors and dashboard when MCP exits.
for (_, h) in state.supervisors.write().await.drain() {
    h.abort();
}
dashboard_handle.abort();
```

(Drop the old `for s in supervisors { s.abort(); }` block — supervisors now live in `state.supervisors`.)

- [ ] **Step 5: Build + test**

```bash
cargo build -p kestrel-hub 2>&1 | tail -10
cargo test -p kestrel-hub --lib 2>&1 | tail -10
```

Expected: clean build. Lib tests still 34+ passing (Phase 6 + Phase 7 Task 1). No new tests in this task yet.

- [ ] **Step 6: Commit**

```bash
git add crates/kestrel-hub/src/dashboard/mod.rs crates/kestrel-hub/src/main.rs
git commit -m "feat(hub): extend AppState with config_path, psk, supervisor map"
```

---

### Task 3: HTTP handlers `POST /api/nodes` and `DELETE /api/nodes/:node_id`

**Files:**
- Modify: `crates/kestrel-hub/src/dashboard/api.rs`
- Modify: `crates/kestrel-hub/src/dashboard/mod.rs` (swap stub routes for real handlers)

- [ ] **Step 1: Add the request DTO and handlers**

Append to `crates/kestrel-hub/src/dashboard/api.rs`:

```rust
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::config::{add_node, load_doc, remove_node, save_doc};
use crate::dashboard::AppState;
use crate::supervisor;

#[derive(Debug, serde::Deserialize)]
pub struct AddNodeBody {
    pub node_id: String,
    pub address: String,
}

/// POST /api/nodes — body: { node_id, address }
/// Atomically: mutates config file, spawns supervisor, registers in state.
pub async fn post_node(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::Json(body): axum::Json<AddNodeBody>,
) -> Result<(StatusCode, axum::Json<NodeStatusDto>), (StatusCode, String)> {
    let address: std::net::SocketAddr = body.address.parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid address: {}", e)))?;

    // Acquire the write lock for the full read-modify-write cycle.
    let _lock = state.config_write_lock.lock().await;

    let mut doc = load_doc(&state.config_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    add_node(&mut doc, &body.node_id, address)
        .map_err(|e| (StatusCode::CONFLICT, e.to_string()))?;
    save_doc(&state.config_path, &doc)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Spawn the supervisor; the registry will fill in once it connects.
    let handle = supervisor::spawn(
        crate::config::NodeConfig { node_id: body.node_id.clone(), address },
        state.registry.clone(),
        state.psk.clone(),
    );
    state.supervisors.write().await.insert(body.node_id.clone(), handle);

    // The supervisor will mark_reconnecting before its first connect attempt;
    // return a synthetic Reconnecting status so the caller has something to render.
    let snap_status = NodeStatusDto {
        node_id: body.node_id.clone(),
        state: NodeStateDto::Reconnecting,
        os_name: None,
        latency_ms: None,
        last_seen_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
        next_retry_in_ms: None,
    };

    Ok((StatusCode::CREATED, axum::Json(snap_status)))
}

/// DELETE /api/nodes/:node_id
/// Atomically: mutates config file, aborts supervisor, forgets node from registry.
pub async fn delete_node(
    axum::extract::State(state): axum::extract::State<AppState>,
    Path(node_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let _lock = state.config_write_lock.lock().await;

    let mut doc = load_doc(&state.config_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    remove_node(&mut doc, &node_id)
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    save_doc(&state.config_path, &doc)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if let Some(handle) = state.supervisors.write().await.remove(&node_id) {
        handle.abort();
    }
    state.registry.forget_node(&node_id).await;

    Ok(StatusCode::NO_CONTENT)
}
```

- [ ] **Step 2: Switch the routes from stubs to real handlers**

In `crates/kestrel-hub/src/dashboard/mod.rs`, update the route definitions to use `api::post_node` and `api::delete_node`, and delete the `post_node_stub` / `delete_node_stub` placeholder fns.

Final routes section:

```rust
Router::new()
    .route("/", get(index))
    .route("/sse", get(sse_handler))
    .route("/api/nodes", get(api::nodes_json).post(api::post_node))
    .route("/api/nodes/:node_id", axum::routing::delete(api::delete_node))
    .route("/api/events", get(api::events_handler))
    .nest_service("/assets", ServeDir::new("crates/kestrel-hub/assets"))
    .with_state(state)
```

- [ ] **Step 3: Make `AppState` accessible to handlers (FromRef chain)**

The new handlers take `State<AppState>` directly, not via `FromRef`. The existing handlers (`index`, `sse_handler`, `nodes_json`, `events_handler`) extract `State<Arc<NodeRegistry>>` via the `FromRef<AppState> for Arc<NodeRegistry>` impl from Task 2 — so they keep working.

If `nodes_json` / `events_handler` extract `Arc<NodeRegistry>` directly via `State<Arc<NodeRegistry>>`, they'll pick up the FromRef. Verify they still compile.

- [ ] **Step 4: Verify build**

```bash
cargo build -p kestrel-hub 2>&1 | tail -15
```

Expected: clean build. If you see `unused import` warnings for the placeholder stubs, remove the unused imports — those stubs are gone.

- [ ] **Step 5: Lib tests**

```bash
cargo test -p kestrel-hub --lib 2>&1 | tail -10
```

Expected: all existing tests still pass; no new tests yet in this task (the integration test in Task 6 covers the new handlers end-to-end).

- [ ] **Step 6: Commit**

```bash
git add crates/kestrel-hub/src/dashboard/api.rs crates/kestrel-hub/src/dashboard/mod.rs
git commit -m "feat(hub): add POST/DELETE /api/nodes for hot-reload"
```

---

### Task 4: Promote `HubClient` to a top-level `client` module

**Files:**
- Create: `crates/kestrel-hub/src/client.rs`
- Modify: `crates/kestrel-hub/src/lib.rs` (+ `pub mod client;`)
- Modify: `crates/kestrel-hub/src/tui/client.rs` (remove duplicate, re-export from top-level)
- Modify: `crates/kestrel-hub/src/tui/mod.rs` (if it `use`s anything from `tui::client`)

The TUI's `HubClient` does `fetch_nodes` and `subscribe_events`. Phase 7 needs `add_node_via_http` and `remove_node_via_http` too. Promote to `crate::client` and add the new methods so both the TUI and the CLI use one client type.

- [ ] **Step 1: Move + extend the client**

Create `crates/kestrel-hub/src/client.rs`:

```rust
// crates/kestrel-hub/src/client.rs
use std::time::Duration;

use anyhow::Context;
use futures::stream::StreamExt;

use crate::dashboard::api::{AddNodeBody, NodeEventDto, NodeStatusDto};

/// HTTP client for a running kestrel-hub's JSON API. Used by both the TUI
/// (read-only: fetch_nodes + subscribe_events) and the CLI (mutating:
/// add_node + remove_node).
#[derive(Clone)]
pub struct HubClient {
    base_url: String,
    http: reqwest::Client,
}

impl HubClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        HubClient {
            base_url: base_url.into(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        }
    }

    /// Like `new`, but with a short connect timeout — used by the CLI to
    /// quickly fall back to file mutation when the hub isn't running.
    pub fn with_quick_timeout(base_url: impl Into<String>) -> Self {
        HubClient {
            base_url: base_url.into(),
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_millis(1000))
                .timeout(Duration::from_secs(5))
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
                Ok(_) => None,
                Err(e) => Some(Err(anyhow::anyhow!("SSE error: {:?}", e))),
            }
        })
    }

    /// POST /api/nodes — returns the created node's initial status.
    /// Errors carry the HTTP status text so the CLI can surface it.
    pub async fn add_node(&self, node_id: &str, address: &str) -> anyhow::Result<NodeStatusDto> {
        let url = format!("{}/api/nodes", self.base_url);
        let resp = self.http.post(&url)
            .json(&AddNodeBody { node_id: node_id.into(), address: address.into() })
            .send().await
            .with_context(|| format!("POST {}", url))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("hub returned {} {}: {}", status.as_u16(), status.canonical_reason().unwrap_or(""), body);
        }
        let dto: NodeStatusDto = resp.json().await
            .with_context(|| format!("decode JSON from {}", url))?;
        Ok(dto)
    }

    /// DELETE /api/nodes/{node_id}.
    pub async fn remove_node(&self, node_id: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/nodes/{}", self.base_url, node_id);
        let resp = self.http.delete(&url).send().await
            .with_context(|| format!("DELETE {}", url))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("hub returned {} {}: {}", status.as_u16(), status.canonical_reason().unwrap_or(""), body);
        }
        Ok(())
    }
}
```

- [ ] **Step 2: Export from lib.rs**

Add `pub mod client;` to `crates/kestrel-hub/src/lib.rs` in alphabetical order.

- [ ] **Step 3: Migrate the TUI to the top-level client**

In `crates/kestrel-hub/src/tui/mod.rs`, replace `use crate::tui::client::HubClient;` (or however it's imported) with `use crate::client::HubClient;`. Delete `crates/kestrel-hub/src/tui/client.rs` since it's now redundant. Remove `pub mod client;` from `tui/mod.rs`.

- [ ] **Step 4: Build + test**

```bash
cargo build -p kestrel-hub 2>&1 | tail -10
cargo test -p kestrel-hub --lib 2>&1 | tail -10
```

Expected: clean. TUI still compiles. No test changes.

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-hub/src/client.rs crates/kestrel-hub/src/lib.rs crates/kestrel-hub/src/tui/mod.rs
git rm crates/kestrel-hub/src/tui/client.rs
git commit -m "refactor(hub): promote HubClient to top-level client module + add mutation methods"
```

---

### Task 5: CLI `add-node` and `remove-node` try HTTP first

**Files:**
- Modify: `crates/kestrel-hub/src/main.rs`

- [ ] **Step 1: Add `--hub` flag to AddNode and RemoveNode**

In `enum Command`, update the existing variants:

```rust
/// Append a node to kestrel.toml. If the hub is running, applies live;
/// otherwise the change takes effect at next `start`.
AddNode {
    node_id: String,
    address: String,
    #[arg(long, default_value = "kestrel.toml")]
    config: String,
    /// Hub control URL (HTTP). If reachable, the change applies live.
    #[arg(long, default_value = "http://127.0.0.1:7273")]
    hub: String,
},

/// Remove a node from kestrel.toml. If the hub is running, applies live.
RemoveNode {
    node_id: String,
    #[arg(long, default_value = "kestrel.toml")]
    config: String,
    /// Hub control URL (HTTP). If reachable, the change applies live.
    #[arg(long, default_value = "http://127.0.0.1:7273")]
    hub: String,
},
```

- [ ] **Step 2: Rewrite the AddNode handler**

```rust
Command::AddNode { node_id, address, config, hub } => {
    // Validate the address once up front so we get a clean error before any I/O.
    let parsed_addr: std::net::SocketAddr = address.parse()
        .map_err(|e| anyhow::anyhow!("invalid address '{}': {}", address, e))?;

    // Try HTTP first — the running hub will write the file itself.
    let client = kestrel_hub::client::HubClient::with_quick_timeout(&hub);
    match client.add_node(&node_id, &address).await {
        Ok(_status) => {
            println!("added '{}' at {} (live via {}).", node_id, parsed_addr, hub);
        }
        Err(e) => {
            // Hub unreachable — fall back to local file mutation.
            tracing::debug!("hub not reachable ({}), writing file directly", e);
            let mut doc = kestrel_hub::config::load_doc(&config)?;
            kestrel_hub::config::add_node(&mut doc, &node_id, parsed_addr)?;
            kestrel_hub::config::save_doc(&config, &doc)?;
            println!("added '{}' at {}. start `kestrel-hub start` (or restart it) to connect.", node_id, parsed_addr);
        }
    }
}
```

- [ ] **Step 3: Rewrite the RemoveNode handler**

```rust
Command::RemoveNode { node_id, config, hub } => {
    let client = kestrel_hub::client::HubClient::with_quick_timeout(&hub);
    match client.remove_node(&node_id).await {
        Ok(()) => {
            println!("removed '{}' (live via {}).", node_id, hub);
        }
        Err(e) => {
            tracing::debug!("hub not reachable ({}), writing file directly", e);
            let mut doc = kestrel_hub::config::load_doc(&config)?;
            kestrel_hub::config::remove_node(&mut doc, &node_id)?;
            kestrel_hub::config::save_doc(&config, &doc)?;
            println!("removed '{}' from {}. (hub not running)", node_id, config);
        }
    }
}
```

- [ ] **Step 4: Build + verify**

```bash
cargo build -p kestrel-hub 2>&1 | tail -10
```

Expected: clean build.

- [ ] **Step 5: Manual smoke test (with no hub running)**

```bash
TMPDIR=$(mktemp -d)
cat > "$TMPDIR/kestrel.toml" <<'EOF'
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
EOF

# No hub running — should fall back to file mutation.
cargo run --bin kestrel-hub -- add-node alpha 127.0.0.1:7272 --config "$TMPDIR/kestrel.toml" 2>&1 | grep -v warning | tail -3
# Expected: "added 'alpha' at 127.0.0.1:7272. start `kestrel-hub start` (or restart it) to connect."
cat "$TMPDIR/kestrel.toml" | grep -A1 nodes

cargo run --bin kestrel-hub -- remove-node alpha --config "$TMPDIR/kestrel.toml" 2>&1 | grep -v warning | tail -3
# Expected: "removed 'alpha' from $TMPDIR/kestrel.toml. (hub not running)"

rm -rf "$TMPDIR"
```

- [ ] **Step 6: Commit**

```bash
git add crates/kestrel-hub/src/main.rs
git commit -m "feat(hub): CLI add-node/remove-node try HTTP first, fall back to file write"
```

---

### Task 6: Integration test for hot-reload

**Files:**
- Create: `crates/kestrel-hub/tests/phase7_hot_reload.rs`

- [ ] **Step 1: Create the integration test**

The test spawns a fresh axum router with the new AppState, sends `POST /api/nodes` and `DELETE /api/nodes/:id` directly via `tower::ServiceExt`, and asserts state changes.

```rust
// crates/kestrel-hub/tests/phase7_hot_reload.rs
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kestrel_hub::dashboard::{router, AppState};
use kestrel_hub::router::NodeRegistry;
use tower::ServiceExt;

fn test_psk() -> Vec<u8> {
    b"kestrel-test-psk-32bytes-padded!".to_vec()
}

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

#[tokio::test]
async fn post_node_then_delete_node_round_trip_updates_config_and_supervisors() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = starter_toml(dir.path());
    let config_path_str = config_path.to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry.clone(), config_path_str.clone(), test_psk());
    let app = router(state.clone());

    // POST /api/nodes
    let body = serde_json::json!({
        "node_id": "alpha",
        "address": "127.0.0.1:65535"  // a port the agent definitely isn't listening on
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Supervisor map should now contain "alpha".
    assert!(state.supervisors.read().await.contains_key("alpha"));

    // Config file should now contain the node.
    let written = std::fs::read_to_string(&config_path_str).unwrap();
    assert!(written.contains("alpha"), "config should contain node 'alpha':\n{}", written);
    assert!(written.contains("127.0.0.1:65535"));

    // DELETE /api/nodes/alpha
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/nodes/alpha")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Supervisor map should no longer contain "alpha".
    assert!(!state.supervisors.read().await.contains_key("alpha"));

    // Config file should no longer contain the node.
    let written = std::fs::read_to_string(&config_path_str).unwrap();
    assert!(!written.contains("alpha"), "config should NOT contain 'alpha' after delete:\n{}", written);
}

#[tokio::test]
async fn post_node_rejects_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = starter_toml(dir.path());
    let config_path_str = config_path.to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, config_path_str, test_psk());
    let app = router(state);

    let body = serde_json::json!({"node_id": "x", "address": "127.0.0.1:65501"});
    let first_req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let first_resp = app.clone().oneshot(first_req).await.unwrap();
    assert_eq!(first_resp.status(), StatusCode::CREATED);

    let dup_req = Request::builder()
        .method("POST")
        .uri("/api/nodes")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let dup_resp = app.oneshot(dup_req).await.unwrap();
    assert_eq!(dup_resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn delete_node_404_when_missing() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = starter_toml(dir.path());
    let config_path_str = config_path.to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, config_path_str, test_psk());
    let app = router(state);

    let req = Request::builder()
        .method("DELETE")
        .uri("/api/nodes/ghost")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
```

- [ ] **Step 2: Add `tower` dev-dep**

In `crates/kestrel-hub/Cargo.toml` `[dev-dependencies]`, append:

```toml
tower = { workspace = true, features = ["util"] }
```

(`tower` should already be in `[workspace.dependencies]` as a transitive dep of axum — confirm and add if missing.)

If `tower` isn't in workspace deps, add `tower = "0.5"` to root `Cargo.toml` `[workspace.dependencies]`.

- [ ] **Step 3: Run the test**

```bash
cargo test -p kestrel-hub --test phase7_hot_reload 2>&1 | tail -15
```

Expected: 3 passed (`post_node_then_delete_node_round_trip_updates_config_and_supervisors`, `post_node_rejects_duplicate`, `delete_node_404_when_missing`).

If `oneshot` isn't available, the issue is likely the missing `tower` feature `util`. Confirm `tower::ServiceExt` resolves.

- [ ] **Step 4: Workspace tests**

```bash
cargo test --workspace 2>&1 | grep -E "test result" | tail -15
```

Expected: all green. Specifically the existing `tests/phase6_cli.rs` 2 tests still pass.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/kestrel-hub/Cargo.toml crates/kestrel-hub/tests/phase7_hot_reload.rs
git commit -m "test(hub): add phase 7 integration tests for POST/DELETE hot-reload"
```

---

### Task 7: README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a hot-reload note**

Find the "## Setup" section in `README.md`. After the `kestrel-hub status` example line, add:

```markdown

> **Hot-reload:** `add-node` and `remove-node` apply live against a running hub
> (via its HTTP control endpoint at `:7273`). If the hub is down, the change
> takes effect at next `start`. Pass `--hub <url>` to target a non-local hub.
```

Find the "## Subcommand reference" hub table and update the `add-node` and `remove-node` rows:

| `add-node <id> <addr>` | Append `[[hub.nodes]]` to config and apply live if hub is running |
| `remove-node <id>` | Remove from config and apply live if hub is running |

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document hot-reload behavior in setup and subcommand reference"
```

---

## Verification

End-to-end manual test (single Mac, loopback agent):

1. `cargo build --release` — clean.
2. `cargo test --workspace` — all green; new tests in `phase7_hot_reload.rs` pass.
3. Spin up a fresh hub config:

   ```bash
   TMPDIR=$(mktemp -d)
   cd "$TMPDIR"
   cargo run --bin kestrel-hub -- init --bind 127.0.0.1 --config "$TMPDIR/kestrel.toml"
   ```

4. In terminal A: `cargo run --bin kestrel-hub -- start --config "$TMPDIR/kestrel.toml"`. Confirm `Dashboard at http://127.0.0.1:7273` appears.

5. In terminal B: `cargo run --bin kestrel-hub -- tui --hub http://127.0.0.1:7273`. Confirm the TUI shows "no nodes".

6. In terminal C: `cargo run --bin kestrel-hub -- add-node alpha 127.0.0.1:7272 --config "$TMPDIR/kestrel.toml"`. Expected output: `added 'alpha' at 127.0.0.1:7272 (live via http://127.0.0.1:7273).`

7. **Watch terminal B** — the TUI should show `alpha` as `reconnecting` within ~1 second, with no manual refresh.

8. In terminal C: `cargo run --bin kestrel-hub -- remove-node alpha --config "$TMPDIR/kestrel.toml"`. Expected: `removed 'alpha' (live via http://127.0.0.1:7273).` The TUI row in terminal B should disappear within ~1 second.

9. **Stop the hub** (Ctrl-C in terminal A). Then in terminal C: `cargo run --bin kestrel-hub -- add-node beta 127.0.0.1:7273 --config "$TMPDIR/kestrel.toml"`. Expected: `added 'beta' at 127.0.0.1:7273. start \`kestrel-hub start\` (or restart it) to connect.` (Note the different message — HTTP fallback path.) The TOML file should contain `beta`.

---

## Out of scope (deferred)

- Hot-reload for `layout-set` / `layout-unset` (would need KVM router reconciliation)
- Authentication on the HTTP control endpoints (`Authorization: Bearer <token>` keyed on the hub's keyring entry — concrete but adds scope)
- File-watcher for users who edit `kestrel.toml` by hand outside the CLI (not strictly necessary now that CLI works against running hubs)
- A `POST /api/nodes/{id}/reconnect` to force an immediate reconnect attempt (currently the supervisor backoff has to drain)
- Cascading abort of the inner actor task when a supervisor is aborted (already a known limitation since Phase 5; closing the cmd_tx via NodeHandle drop is the current path)
