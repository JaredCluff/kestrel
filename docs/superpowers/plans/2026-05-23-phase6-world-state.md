# Phase 6: Persistent World State + Diffs

> **Goal:** Stop forcing the AI to poll. Maintain a structured per-node world state in the hub, updated by agent push events, queryable as cheap snapshots and deltas.
>
> **For agentic workers:** Use `superpowers:subagent-driven-development` to implement task-by-task.

## Intent (one paragraph)

Today every MCP turn that needs to know "what's on screen" or "is anything happening" costs a full screenshot (~hundreds of KB of base64) or an AX walk (~tens of KB of JSON). Most of the time, nothing changed since the last turn. After Phase 6, the AI calls `world_state(node_id)` and gets a 1–2 KB JSON snapshot (focused app name, mouse position, clipboard metadata, open shells, last-input timestamps) maintained by the hub from agent push events. For "did anything change?" the AI calls `world_diff_since(node_id, t)` and gets only the deltas. Heavy screenshot/AX calls now happen on the agent's *initiative* (when its observer detects a change) instead of the AI's poll loop.

## Architecture

```
Agent                              Hub                              MCP client (AI)
─────────────────                  ──────────────                   ───────────────
WorldObserver (2s loop)
  - sample local state                                              tool: world_state(node)
  - compare vs last sent      ───►  Payload::WorldUpdate            ─────────────────►
  - emit WorldUpdate                ─────────────────────►
                                    NodeRegistry.world_cache         tool: world_diff_since(node, t)
                                    ┌─────────────────────┐         ──────────────────────────────►
                                    │ node_id → WorldState│         responds from cache, no
                                    │ (with last_observed)│         agent round-trip
                                    └─────────────────────┘
                                       │
                                       │ broadcast on change
                                       ▼
                                    NodeEvent::WorldChanged
                                       │
                                       │ SSE
                                       ▼
                                    Dashboard / TUI
```

## The world state schema

```rust
pub struct WorldState {
    pub focused_app: Option<FocusedApp>,
    pub mouse: Option<MousePosition>,
    pub displays: Vec<DisplayInfo>,
    pub clipboard: Option<ClipboardMetadata>,
    pub shells: Vec<ShellSession>,
    pub last_observed_unix: u64,
}

pub struct FocusedApp { pub name: String, pub pid: u32, pub window_title: Option<String> }
pub struct MousePosition { pub x: i32, pub y: i32, pub display: u8 }
pub struct ClipboardMetadata { pub kind: ClipboardKind, pub byte_len: u64, pub fingerprint_hex: String }
pub enum ClipboardKind { Text, Image }
pub struct ShellSession { pub pty_id: u32, pub alive: bool, pub buffered_bytes: u64, pub last_write_unix: u64 }
```

**No payload bytes anywhere.** Clipboard content is summarized as `(kind, length, fingerprint)` — letting the AI detect "the clipboard changed" without leaking what was on it. Screen content is not in the world state at all (screenshots are a separate, on-demand tool).

## File map

```
crates/kestrel-proto/src/
  world.rs                   # NEW: WorldState + sub-structs, all #[derive(Serialize, Deserialize)]
  message.rs                 # MODIFY: + Payload::WorldUpdate { state: WorldState }
  lib.rs                     # MODIFY: re-export WorldState et al.

crates/kestrel-agent/src/
  capabilities/world.rs      # NEW: WorldObserver task — periodic sampler with change detection
  transport.rs               # MODIFY: spawn observer alongside select! loop, send WorldUpdate events

crates/kestrel-hub/src/
  events.rs                  # MODIFY: + NodeEvent::WorldChanged variant
  router.rs                  # MODIFY: + world_cache, + observe_world_update, + world_state_for, + world_diff_since
  transport.rs               # MODIFY: handle inbound WorldUpdate in run_actor
  mcp.rs                     # MODIFY: + world_state, + world_diff_since MCP tools
  dashboard/api.rs           # MODIFY: + /api/world/:node_id (read-only, optional auth)
  dashboard/templates.rs     # MODIFY: + focused-app column on the dashboard row

crates/kestrel-proto/tests/  # roundtrip test for WorldUpdate variant
crates/kestrel-hub/tests/    # integration: end-to-end world state propagates through hub
crates/kestrel-agent/tests/  # observer: sampling produces stable WorldState
```

## Implementation tasks (5 PRs)

Each PR is independently mergeable and builds on the previous one.

---

### PR-6.1 — proto: WorldState wire types

**Files:** `kestrel-proto/src/world.rs` (new), `message.rs` (modify), `lib.rs` (modify), inline tests.

**Goal:** Establish the wire types. Pure additive — no callers yet.

**Tasks:**
- [ ] Add `world.rs` module with `WorldState`, `FocusedApp`, `MousePosition`, `ClipboardMetadata`, `ClipboardKind`, `ShellSession`. All `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]`. `WorldState::empty()` constructor returning a state with all `None`/`vec![]`/`last_observed_unix = 0`.
- [ ] Extend `Payload` with `WorldUpdate { state: WorldState }`. Append at the END of the variant list — wire-stable. Bump no version number; this is an additive extension and old agents that don't emit it stay compatible.
- [ ] Re-export new types from `lib.rs`.
- [ ] Roundtrip test for `Payload::WorldUpdate` with a fully-populated `WorldState` and an empty one. Pin every field.

**Done when:** `cargo test -p kestrel-proto` passes with the new tests.

---

### PR-6.2 — agent: WorldObserver task

**Files:** `kestrel-agent/src/capabilities/world.rs` (new), `transport.rs` (modify), inline tests.

**Goal:** Agent periodically samples its own state and pushes `WorldUpdate` events.

**Tasks:**
- [ ] New module `capabilities/world.rs` with a `WorldObserver` struct that:
  - Holds a `Sender<KestrelMessage>` (same channel transport.rs already uses for shell events).
  - Has an async `run()` method that loops every 2s.
  - Calls platform observers (macOS first; Linux/Windows stub with fallback like AX).
  - Compares against last-sent state; sends `WorldUpdate` only if anything changed.
- [ ] Platform-specific observers (one file each, cfg-gated):
  - `world/macos.rs`: focused app via NSWorkspace frontmost (already used by ax.rs), mouse via `CGEventSourceGetMouseCursorPosition`, displays via existing `screen::list_displays`.
  - `world/linux.rs`: focused app via AT-SPI registry (best-effort, fallback `None` on failure).
  - `world/windows.rs`: focused app via UIAutomation foreground window.
- [ ] Clipboard fingerprint helper: read clipboard, hash its bytes with SHA-256 (truncated 16 hex chars), report `(kind, length, fingerprint)`. Doesn't include content. Skipped on backends where clipboard polling is too expensive (TODO comment).
- [ ] Shell sessions: pull from a shared `Arc<Mutex<HashMap<pty_id, ShellMeta>>>` that `ShellManager` maintains. `ShellMeta { alive, buffered_bytes, last_write_unix }`.
- [ ] Hook `WorldObserver::run()` into `transport::handle_conn` — spawn alongside the shell-event pump.
- [ ] Inline unit tests:
  - `WorldObserver::diff_*` helpers detect changed fields correctly.
  - Empty observation produces no `WorldUpdate` send (no spurious wake-ups).

**Done when:** `cargo test -p kestrel-agent` passes; manual smoke test against a real hub shows `WorldUpdate` frames every ~2s on the wire.

**Caveats:** Mouse position sampling on macOS requires `CGEventSourceGetMouseCursorPosition` which is in CoreGraphics. We already link CoreGraphics for `lock_cursor` in kvm.rs; reuse the linkage. No new system frameworks.

---

### PR-6.3 — hub: world cache + event broadcasting

**Files:** `kestrel-hub/src/events.rs` (modify), `router.rs` (modify), `transport.rs` (modify), inline tests.

**Goal:** Hub accepts inbound `WorldUpdate`, stores by node_id, broadcasts on the existing event channel.

**Tasks:**
- [ ] Extend `NodeEvent` with `WorldChanged { node_id: String, state: WorldState }`. Existing `Connected/Disconnected/Reconnecting` variants unchanged.
- [ ] `NodeRegistry` gains:
  - `world_cache: Arc<RwLock<HashMap<String, WorldState>>>`.
  - `observe_world_update(&self, node_id: &str, state: WorldState)` — updates the cache, broadcasts a `WorldChanged` event, no-op if state hasn't changed (defense in depth on top of the agent's side check).
  - `world_state_for(&self, node_id: &str) -> Option<WorldState>` — clones from cache.
  - `world_diff_since(&self, node_id: &str, since_unix: u64) -> Option<WorldStateDiff>` — for now, returns the full state if `last_observed_unix > since`, else `None`. Field-granular diffs are a follow-up.
- [ ] In `transport::run_actor`, route incoming `Payload::WorldUpdate` to `registry.observe_world_update`. Already have the registry handle in scope via `NodeHandle`.
- [ ] Wire `NodeRegistry` references in. The hub-side actor doesn't currently have a registry handle (it just has a cmd channel back from the supervisor). The cleanest plumbing: when the supervisor calls `registry.register(handle)`, it can hand the handle a weak `Arc<NodeRegistry>`. The actor's frame-receive loop forwards WorldUpdate to the registry.
- [ ] Unit tests:
  - `observe_world_update` is no-op when state is identical (Eq derive).
  - `observe_world_update` emits a `WorldChanged` event when state changes.
  - `world_state_for` returns `None` for unknown nodes.

**Done when:** `cargo test -p kestrel-hub` passes; the SSE event stream emits `WorldChanged` deltas observable via curl.

---

### PR-6.4 — MCP tools

**Files:** `kestrel-hub/src/mcp.rs` (modify), inline tests.

**Goal:** Expose the world cache to the AI.

**Tasks:**
- [ ] New tool `world_state(node_id)`:
  - Calls `registry.world_state_for(node_id)`.
  - Returns the JSON-serialized `WorldState` (uses serde_json::to_string_pretty).
  - 404-style McpError when node unknown OR when observer hasn't run yet.
  - Audited via existing `audit_call` helper.
- [ ] New tool `world_diff_since(node_id, since_unix_secs: u64)`:
  - Calls `registry.world_diff_since(node_id, since_unix_secs)`.
  - Returns `null` (text "null") when nothing has changed since `since_unix_secs`.
  - Otherwise returns the full state JSON. Field-granular diffs are a follow-up.
  - Audited.
- [ ] Inline tests via the existing `KestrelMcp::with_audit` ctor pattern: build a registry, seed a world state, call the tool, assert the JSON.

**Done when:** `cargo test -p kestrel-hub` passes; Claude Code with a Kestrel-hub MCP entry can call both tools and gets reasonable output.

---

### PR-6.5 — dashboard surface

**Files:** `kestrel-hub/src/dashboard/api.rs` (modify), `dashboard/templates.rs` (modify), inline tests.

**Goal:** Operators can see what's happening across the fleet without re-screenshotting.

**Tasks:**
- [ ] `GET /api/world/:node_id` returns `WorldState` as JSON. Auth: same as `/api/nodes` (no auth required for read; same as the rest of the read-only API surface). 404 on unknown node.
- [ ] Dashboard row gains a "focused" column showing the focused app's name + window title (truncated to ~40 chars). Empty cell when no world state yet.
- [ ] SSE stream gains support for `WorldChanged` events — re-renders the affected row's "focused" cell. (Existing SSE-swap mechanism handles this if the row template includes the focused cell unconditionally.)
- [ ] Tests:
  - `/api/world/:id` returns 200 + JSON when state exists.
  - `/api/world/:id` returns 404 when state missing.
  - Page renders the focused cell when world state is non-empty.

**Done when:** `cargo test --workspace` passes; loading the dashboard against a running hub shows live focused-app names in each row.

---

## Verification

End-to-end manual test:
1. `kestrel-hub start` against a Mac with the agent running.
2. Open Safari. Within ~2s, dashboard shows "Safari" in the focused column.
3. Switch to Terminal. Dashboard updates to "Terminal" within ~2s.
4. Call `world_state(macstudio)` from Claude Code. Get a JSON snapshot with current focused app.
5. Move the mouse. Call `world_state` again. Mouse position has updated.
6. `world_diff_since(macstudio, <unix-seconds-ago>)` returns the current state. `world_diff_since(macstudio, <future-timestamp>)` returns null.

## Non-goals (deferred)

- **Visual diff of screenshots.** Phase 6b if there's demand. Requires perceptual hashing (`image-hasher` crate) + region detection. Adds dep weight.
- **Field-granular diff JSON.** v1 returns the full state when anything changed. Operators wanting bandwidth optimization can compute the diff client-side.
- **World state persistence across hub restarts.** The cache is in-memory; on restart, the agent's next `WorldUpdate` re-populates within 2s. Persistence buys little.
- **Time-travel / world-state replay.** Useful for debugging but out of scope for v1.

## How long this should take

Each PR is ~1–2 hours of focused work. Total Phase 6: ~5–10 hours.
