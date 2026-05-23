// crates/kestrel-agent/src/capabilities/ax/mod.rs
//
// Cross-platform accessibility-tree describe. The public surface is
// `pub fn describe() -> AccessibilityNode`. Per-OS implementations live
// in submodules and are dispatched via `cfg` at the call site below.
//
// Adding a new platform:
//   1. Create a submodule `crates/.../capabilities/ax/<os>.rs` exposing
//      `pub fn describe() -> AccessibilityNode`.
//   2. Add a target-conditional dependency to `kestrel-agent/Cargo.toml`
//      (see `[target.'cfg(target_os = "...")'.dependencies]`).
//   3. Add a `#[cfg(target_os = "<os>")] mod <os>;` line below and a
//      matching arm in `describe()`.
//
// Per-OS modules should:
//   - Return a populated `AccessibilityNode` with `fallback: false`
//     when the platform AX query succeeds.
//   - Return `AccessibilityNode::unavailable()` (which sets
//     `fallback: true`) when the OS denies permission, the AX bus is
//     down, or any required runtime piece isn't present. The MCP
//     `describe` tool's caller uses `fallback: true` as the signal to
//     fall back to `screenshot` instead.

use kestrel_proto::AccessibilityNode;

#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "windows")]
mod windows;

/// Walk the focused application's accessibility tree (up to 5 levels
/// deep on macOS; depth-1 root-only on Linux and Windows for now).
///
/// On platforms without a backend, or when the OS rejects the AX query
/// (permission denied, no AX bus, etc.), returns
/// `AccessibilityNode::unavailable()` so callers know to fall back to
/// a screenshot.
pub fn describe() -> AccessibilityNode {
    #[cfg(target_os = "macos")]
    {
        macos::describe()
    }
    #[cfg(target_os = "linux")]
    {
        linux::describe()
    }
    #[cfg(target_os = "windows")]
    {
        windows::describe()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        AccessibilityNode::unavailable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_returns_some_node() {
        // Smoke test: the public entry point always returns SOMETHING
        // with a non-empty role, even on platforms that aren't yet
        // wired up. This pins that the dispatch can never panic.
        let node = describe();
        assert!(!node.role.is_empty(), "role must be non-empty");
    }

    #[test]
    #[cfg(target_os = "macos")]
    #[ignore = "requires Accessibility permission (macOS TCC → System Settings → Privacy & Security → Accessibility); run manually"]
    fn describe_real_ax_tree_not_fallback() {
        let node = describe();
        assert!(!node.fallback, "expected real AX tree, got fallback");
        assert!(
            !node.children.is_empty() || !node.role.is_empty(),
            "root should have content"
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires a running AT-SPI bus (at-spi-bus-launcher); run manually under a desktop session"]
    fn describe_linux_attaches_to_bus_or_falls_back() {
        let node = describe();
        // Either: AT-SPI was reachable and we got the documented depth-1
        // root ("desktop"), OR the bus wasn't reachable and we got the
        // unavailable() fallback. Both are valid outcomes from the
        // backend; what's NOT valid is a panic or an empty role.
        assert!(!node.role.is_empty(), "role must be non-empty");
        assert!(node.role == "desktop" || node.fallback);
    }

    #[test]
    #[cfg(target_os = "windows")]
    #[ignore = "requires a Windows desktop session with a focused window; run manually"]
    fn describe_windows_returns_focused_or_falls_back() {
        let node = describe();
        // Either: UIAutomation gave us a focused element (real role +
        // potentially a label), OR the call failed and we got the
        // unavailable() fallback. Same contract as Linux.
        assert!(!node.role.is_empty(), "role must be non-empty");
    }
}
