// crates/kestrel-agent/src/capabilities/ax/windows.rs
//
// Windows AX backend. Talks to UI Automation (the modern Win32
// accessibility API) via the `uiautomation` crate, which is a thin
// safe wrapper over the COM-based IUIAutomation*** interfaces. The
// crate handles CoInitialize per-thread, so callers don't have to.
//
// On failure (UI Automation unavailable, COM init refused, focused
// element query rejected), we return `AccessibilityNode::unavailable()`
// (`fallback: true`) and callers fall back to a screenshot.
//
// Current implementation depth: depth-1 (focused element only).
// Walking children via UIAutomation requires constructing a
// `TreeWalker` and iterating its `get_first_child` chain — feasible
// and well-supported by the crate, but each step is a COM round-trip
// and we want to test the basic wiring works first before adding the
// depth-5 walk. A follow-up PR can add full recursion.

use kestrel_proto::AccessibilityNode;

pub fn describe() -> AccessibilityNode {
    let automation = match uiautomation::UIAutomation::new() {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!("ax(windows): UIAutomation::new failed: {}", e);
            return AccessibilityNode::unavailable();
        }
    };

    let focused = match automation.get_focused_element() {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("ax(windows): get_focused_element failed: {}", e);
            return AccessibilityNode::unavailable();
        }
    };

    let role = focused
        .get_classname()
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into());
    let label = focused
        .get_name()
        .ok()
        .filter(|s| !s.is_empty());
    let enabled = focused.is_enabled().unwrap_or(true);

    // Children walk not yet implemented — see module-level comment.
    // Returning fallback: false because the platform call succeeded
    // and the data we DO have is real.
    //
    // Follow-up TODO: construct a TreeWalker and recurse with
    // depth=5 to match macOS.
    AccessibilityNode {
        role,
        label,
        value: None,
        focused: true,
        enabled,
        bounds: None,
        children: vec![],
        fallback: false,
    }
}
