# Kestrel Phase 4 — Accessibility Tree Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the stub `AccessibilityNode::unavailable()` response with a real macOS AX tree walk, and expose it as a `describe` MCP tool so Claude Code can reason about live on-screen UI elements without needing a screenshot.

**Architecture:** Agent gains `capabilities/ax.rs` (macOS-only; returns `unavailable()` on other platforms) that walks the focused application's AX tree via the `accessibility` crate. The agent transport's `DescribeReq` handler switches from returning the stub to calling `ax::describe()` in a `spawn_blocking`. Hub gains a `NodeHandle::describe()` method, a `NodeRegistry::describe()` delegation, and a new `describe` MCP tool that returns the tree as pretty-printed JSON. Tests for the real AX walk are `#[ignore]` since they require macOS TCC Accessibility permission.

**Tech Stack additions:** `accessibility = "0.1"` (macOS platform dep only)

---

## Prerequisites

Before starting, merge the Phase 3 branch to master:

```bash
git push -u origin feat/phase3-clipboard-shell
gh pr create --title "feat: Phase 3 — clipboard + PTY shell" \
  --body "Adds clipboard read/write and PTY shell (interactive + one-shot) capabilities with 14 MCP tools total."
gh pr merge --squash
git checkout master && git pull
git checkout -b feat/phase4-ax-tree
```

---

## File Map

```
kestrel/
  Cargo.toml                               # Add accessibility platform dep for macos
  crates/
    kestrel-agent/
      Cargo.toml                           # Add accessibility (macos-only target dep)
      src/
        capabilities/
          ax.rs                            # NEW: macOS AX tree walk via accessibility crate
          mod.rs                           # Add pub mod ax
        transport.rs                       # Change DescribeReq handler to call ax::describe
    kestrel-hub/
      src/
        transport.rs                       # Add NodeHandle::describe()
        router.rs                          # Add NodeRegistry::describe()
        mcp.rs                             # Add DescribeArgs + describe tool (15 tools total)
      tests/
        phase4.rs                          # Integration: describe returns valid tree (fallback ok)
```

---

### Task 1: Agent AX capability

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Modify: `crates/kestrel-agent/Cargo.toml`
- Create: `crates/kestrel-agent/src/capabilities/ax.rs`
- Modify: `crates/kestrel-agent/src/capabilities/mod.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/kestrel-agent/src/capabilities/ax.rs` with test-only content:

```rust
// crates/kestrel-agent/src/capabilities/ax.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_returns_some_node() {
        // On headless CI this returns fallback=true; on a desktop with AX permission it returns real data.
        // Either way it must return a valid node (not panic).
        let node = describe();
        assert!(!node.role.is_empty(), "role must be non-empty");
    }

    #[test]
    #[ignore = "requires Accessibility permission (macOS TCC → System Settings → Privacy & Security → Accessibility); run manually"]
    fn describe_real_ax_tree_not_fallback() {
        let node = describe();
        assert!(!node.fallback, "expected real AX tree, got fallback");
        assert!(!node.children.is_empty() || !node.role.is_empty(), "root should have content");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p kestrel-agent capabilities::ax 2>&1 | tail -10
```

Expected: compile error — `describe` not defined.

- [ ] **Step 3: Add accessibility dep to workspace Cargo.toml**

In `Cargo.toml` (workspace root), inside `[workspace.dependencies]`, append:

```toml
accessibility = "0.1"
```

- [ ] **Step 4: Add platform-gated accessibility dep to kestrel-agent Cargo.toml**

In `crates/kestrel-agent/Cargo.toml`, append this section after `[dependencies]`:

```toml
[target.'cfg(target_os = "macos")'.dependencies]
accessibility = { workspace = true }
```

- [ ] **Step 5: Implement ax.rs**

Replace `crates/kestrel-agent/src/capabilities/ax.rs` with:

```rust
// crates/kestrel-agent/src/capabilities/ax.rs
use kestrel_proto::AccessibilityNode;

/// Walk the focused application's AX tree up to 5 levels deep.
/// Returns `AccessibilityNode::unavailable()` on non-macOS or if AX permission is denied.
pub fn describe() -> AccessibilityNode {
    #[cfg(target_os = "macos")]
    {
        macos::describe()
    }
    #[cfg(not(target_os = "macos"))]
    {
        AccessibilityNode::unavailable()
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use accessibility::{AXAttribute, AXUIElement};
    use kestrel_proto::AccessibilityNode;

    pub fn describe() -> AccessibilityNode {
        let system = AXUIElement::system_wide();
        match system.attribute(&AXAttribute::focused_application()) {
            Ok(app) => walk(&app, 5),
            Err(_) => AccessibilityNode::unavailable(),
        }
    }

    fn walk(elem: &AXUIElement, depth: u8) -> AccessibilityNode {
        let role = elem
            .attribute(&AXAttribute::role())
            .unwrap_or_else(|_| "unknown".into());

        let label = elem
            .attribute(&AXAttribute::title())
            .ok()
            .filter(|s: &String| !s.is_empty())
            .or_else(|| {
                elem.attribute(&AXAttribute::description())
                    .ok()
                    .filter(|s: &String| !s.is_empty())
            });

        let focused = elem.attribute(&AXAttribute::focused()).unwrap_or(false);
        let enabled = elem.attribute(&AXAttribute::enabled()).unwrap_or(true);

        let children = if depth == 0 {
            vec![]
        } else {
            elem.attribute(&AXAttribute::children())
                .unwrap_or_default()
                .iter()
                .map(|c| walk(c, depth - 1))
                .collect()
        };

        AccessibilityNode {
            role,
            label,
            value: None,
            focused,
            enabled,
            bounds: None,
            children,
            fallback: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_returns_some_node() {
        let node = describe();
        assert!(!node.role.is_empty(), "role must be non-empty");
    }

    #[test]
    #[ignore = "requires Accessibility permission (macOS TCC → System Settings → Privacy & Security → Accessibility); run manually"]
    fn describe_real_ax_tree_not_fallback() {
        let node = describe();
        assert!(!node.fallback, "expected real AX tree, got fallback");
        assert!(!node.children.is_empty() || !node.role.is_empty(), "root should have content");
    }
}
```

- [ ] **Step 6: Add pub mod ax to mod.rs**

Edit `crates/kestrel-agent/src/capabilities/mod.rs`:

```rust
pub mod ax;
pub mod clipboard;
pub mod input;
pub mod screen;
pub mod shell;
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
cargo test -p kestrel-agent capabilities::ax 2>&1 | tail -15
```

Expected:
```
test capabilities::ax::tests::describe_returns_some_node ... ok
test capabilities::ax::tests::describe_real_ax_tree_not_fallback ... ignored
```

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/kestrel-agent/Cargo.toml \
        crates/kestrel-agent/src/capabilities/ax.rs \
        crates/kestrel-agent/src/capabilities/mod.rs
git commit -m "feat(agent): add AX tree capability using accessibility crate"
```

---

### Task 2: Agent transport — wire DescribeReq to real AX

**Files:**
- Modify: `crates/kestrel-agent/src/transport.rs`

The current handler at line ~168 returns `AccessibilityNode::unavailable()`. Replace it with a `spawn_blocking` call to `ax::describe()`.

- [ ] **Step 1: Locate and update the DescribeReq arm in transport.rs**

The current arm is:

```rust
Payload::DescribeReq { .. } => {
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id, kind: MsgKind::Response,
        payload: Payload::DescribeResp { tree: AccessibilityNode::unavailable() },
    })?)).await?;
}
```

Replace it with:

```rust
Payload::DescribeReq { .. } => {
    let tree = tokio::task::spawn_blocking(crate::capabilities::ax::describe)
        .await
        .unwrap_or_else(|_| AccessibilityNode::unavailable());
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id, kind: MsgKind::Response,
        payload: Payload::DescribeResp { tree },
    })?)).await?;
}
```

No import changes are needed — `AccessibilityNode` is already imported at the top of `transport.rs`, and `crate::capabilities::ax::describe` is fully qualified.

- [ ] **Step 2: Verify all agent tests still pass**

```bash
cargo test -p kestrel-agent 2>&1 | tail -15
```

Expected: all existing tests pass, `describe_real_ax_tree_not_fallback` ignored.

- [ ] **Step 3: Commit**

```bash
git add crates/kestrel-agent/src/transport.rs
git commit -m "feat(agent): wire DescribeReq to real AX tree via spawn_blocking"
```

---

### Task 3: Hub — NodeHandle::describe and NodeRegistry::describe

**Files:**
- Modify: `crates/kestrel-hub/src/transport.rs`
- Modify: `crates/kestrel-hub/src/router.rs`

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` block in `crates/kestrel-hub/src/router.rs` (inside the existing `mod tests`):

```rust
#[test]
fn registry_has_describe_method() {
    // Compile-only: verify describe exists on NodeRegistry.
    let _: fn(&NodeRegistry, &str, u8) -> _ = |r: &NodeRegistry, id: &str, d: u8| {
        let _ = r.describe(id, d);
    };
    let r = NodeRegistry::new();
    assert!(r.list_sync().is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p kestrel-hub registry_has_describe 2>&1 | tail -10
```

Expected: compile error — `describe` not defined on `NodeRegistry`.

- [ ] **Step 3: Add describe to NodeHandle in transport.rs**

In `crates/kestrel-hub/src/transport.rs`, add the import for `AccessibilityNode` to the existing `kestrel_proto` use statement at the top:

```rust
use kestrel_proto::{
    AccessibilityNode, ClipboardContent, hmac_response, Button, KeyCode, KestrelMessage, Modifiers, MsgKind,
    OsInfo, Payload, PressRelease, Rect,
};
```

Then add this method to the `NodeHandle` impl block, after `clipboard_write` and before `spawn_shell`:

```rust
// ── Phase 4 accessibility ─────────────────────────────────────────────────

pub async fn describe(&self, display: u8) -> anyhow::Result<AccessibilityNode> {
    let reply = self.request(Payload::DescribeReq { display }).await?;
    match reply.payload {
        Payload::DescribeResp { tree } => Ok(tree),
        _ => anyhow::bail!("expected DescribeResp"),
    }
}
```

- [ ] **Step 4: Add describe to NodeRegistry in router.rs**

In `crates/kestrel-hub/src/router.rs`, add `AccessibilityNode` to the `kestrel_proto` import:

```rust
use kestrel_proto::{Button, ClipboardContent, KeyCode, Modifiers, AccessibilityNode, OsInfo, PressRelease, Rect};
```

Then add after the `clipboard_write` method and before the `run_shell` method:

```rust
// ── Phase 4 accessibility ─────────────────────────────────────────────────

pub async fn describe(&self, node_id: &str, display: u8) -> anyhow::Result<AccessibilityNode> {
    self.get(node_id).await?.describe(display).await
}
```

And add the compile-only test to `mod tests`:

```rust
#[test]
fn registry_has_describe_method() {
    let _: fn(&NodeRegistry, &str, u8) -> _ = |r: &NodeRegistry, id: &str, d: u8| {
        let _ = r.describe(id, d);
    };
    let r = NodeRegistry::new();
    assert!(r.list_sync().is_empty());
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p kestrel-hub 2>&1 | tail -15
```

Expected: all existing tests pass plus `registry_has_describe_method ... ok`.

- [ ] **Step 6: Commit**

```bash
git add crates/kestrel-hub/src/transport.rs crates/kestrel-hub/src/router.rs
git commit -m "feat(hub): add describe method to NodeHandle and NodeRegistry"
```

---

### Task 4: Hub MCP — describe tool

**Files:**
- Modify: `crates/kestrel-hub/src/mcp.rs`

The existing `KestrelMcp` has 14 tools. We add 1 more: `describe`.

- [ ] **Step 1: Verify baseline test passes**

```bash
cargo test -p kestrel-hub mcp_server_constructs 2>&1 | tail -5
```

Expected: `mcp_server_constructs ... ok`.

- [ ] **Step 2: Add DescribeArgs struct**

In `crates/kestrel-hub/src/mcp.rs`, add `AccessibilityNode` to the `kestrel_proto` import line:

```rust
use kestrel_proto::{Button, ClipboardContent, KeyCode};
```

becomes:

```rust
use kestrel_proto::{Button, ClipboardContent, KeyCode};
```

(No change needed — `AccessibilityNode` is not needed directly in mcp.rs since we just serialize the return value.)

Add `DescribeArgs` after `ShellPtyArgs`:

```rust
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DescribeArgs {
    pub node_id: String,
    /// Display index (0-based). Currently ignored on the agent — always describes the focused app.
    pub display: Option<u8>,
}
```

- [ ] **Step 3: Add the describe tool**

Inside the `#[tool_router] impl KestrelMcp` block, add after `shell_close`:

```rust
// ── Phase 4 accessibility tool ────────────────────────────────────────────

#[tool(description = "Get the accessibility tree of the focused application on a node. Returns a JSON tree of UI elements with role, label, focused, enabled, and children. Requires Accessibility permission on macOS; returns {\"fallback\":true} if denied.")]
async fn describe(
    &self,
    Parameters(args): Parameters<DescribeArgs>,
) -> Result<CallToolResult, McpError> {
    let tree = self
        .registry
        .describe(&args.node_id, args.display.unwrap_or(0))
        .await
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
    let json = serde_json::to_string_pretty(&tree)
        .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e));
    Ok(CallToolResult::success(vec![Content::text(json)]))
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p kestrel-hub mcp 2>&1 | tail -10
```

Expected: `mcp_server_constructs ... ok`.

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-hub/src/mcp.rs
git commit -m "feat(hub): add describe MCP tool for accessibility tree (15 tools total)"
```

---

### Task 5: Integration tests

**Files:**
- Create: `crates/kestrel-hub/tests/phase4.rs`

- [ ] **Step 1: Create phase4.rs**

Create `crates/kestrel-hub/tests/phase4.rs`:

```rust
// crates/kestrel-hub/tests/phase4.rs
use std::net::SocketAddr;
use kestrel_agent::config::AgentConfig;
use kestrel_hub::transport::connect;

fn test_psk() -> Vec<u8> {
    b"kestrel-test-psk-32bytes-padded!".to_vec()
}

async fn start_agent(node_id: &str) -> SocketAddr {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let cfg = AgentConfig::new("127.0.0.1:0".parse().unwrap(), node_id.into(), test_psk());
    tokio::spawn(async move {
        kestrel_agent::transport::serve(&cfg, Some(ready_tx)).await.unwrap();
    });
    ready_rx.await.expect("agent did not send bound address")
}

#[tokio::test]
async fn test_describe_returns_valid_node() {
    // On CI/headless this returns fallback=true with role="root".
    // On a desktop with AX permission it returns the real focused app tree.
    // Either way the response must be a valid AccessibilityNode (no panic, no error).
    let addr = start_agent("ax-fallback-node").await;
    let handle = connect(addr, &test_psk()).await.unwrap();
    let tree = handle.describe(0).await.unwrap();
    assert!(!tree.role.is_empty(), "role must be non-empty even in fallback mode");
}

#[tokio::test]
#[ignore = "requires Accessibility permission (macOS TCC → System Settings → Privacy & Security → Accessibility); run manually"]
async fn test_describe_real_ax_tree() {
    // Grant Accessibility access to the terminal (or IDE) running this test, then un-ignore.
    let addr = start_agent("ax-real-node").await;
    let handle = connect(addr, &test_psk()).await.unwrap();
    let tree = handle.describe(0).await.unwrap();
    assert!(
        !tree.fallback,
        "expected real AX tree but got fallback — check Accessibility permission"
    );
    // The focused app should have at least a role.
    assert!(!tree.role.is_empty(), "real tree root must have a non-empty role");
}
```

- [ ] **Step 2: Run the tests**

```bash
cargo test -p kestrel-hub --test phase4 2>&1 | tail -15
```

Expected:
```
test test_describe_returns_valid_node ... ok
test test_describe_real_ax_tree ... ignored, requires Accessibility permission ...
test result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out
```

- [ ] **Step 3: Run all workspace tests to confirm no regressions**

```bash
cargo test --workspace 2>&1 | tail -30
```

Expected: all tests pass, AX/screen/clipboard headless tests still ignored.

- [ ] **Step 4: Commit**

```bash
git add crates/kestrel-hub/tests/phase4.rs
git commit -m "test(hub): add phase 4 integration tests for AX tree describe"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] Real macOS AX tree walk (agent: `accessibility` crate, `ax::describe`, depth=5)
- [x] Non-macOS fallback (returns `unavailable()` on Linux/Windows)
- [x] AX permission denied fallback (returns `unavailable()` when `AXFocusedApplication` fails)
- [x] Agent transport wiring (DescribeReq → spawn_blocking → ax::describe)
- [x] Hub NodeHandle::describe()
- [x] Hub NodeRegistry::describe()
- [x] Hub MCP describe tool (15 tools total)
- [x] Integration test: returns valid node on headless CI (fallback path)
- [x] Integration test: real AX tree (ignored, requires TCC permission)

**Type consistency:**
- `AccessibilityNode` defined in `kestrel-proto` — used in Task 1 (ax.rs), Task 2 (transport), Task 3 (NodeHandle + NodeRegistry), Task 4 (serialized to JSON in MCP tool)
- `Payload::DescribeReq { display: u8 }` and `Payload::DescribeResp { tree: AccessibilityNode }` already defined in proto — no proto changes needed
- `NodeHandle::describe(display: u8)` → used in Task 3 NodeRegistry and Task 5 tests
- `NodeRegistry::describe(node_id: &str, display: u8)` → used in Task 4 MCP tool

**Placeholder scan:** No TBD, TODO, or "see above" patterns. All code blocks are complete.

**Known limitation:** `bounds` is `None` for all nodes — AX frame is in absolute screen pixels which doesn't map cleanly to the normalized `Rect` type without screen dimension context. Add bounds in Phase 5 alongside element-querying (find by label/role).
