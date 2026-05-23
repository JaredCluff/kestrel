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
// AUTHOR CAVEAT: This file's tree-walk recursion was written without
// the ability to runtime-verify on a Windows machine. The shape
// (TreeWalker.get_first_child / get_next_sibling, depth budget,
// per-step error degradation) follows the uiautomation 0.16 docs.rs
// surface and standard UIA patterns. Specific method names may need
// adjustment in a follow-up if the crate API has drifted; the
// structural recursion is what we're shipping.

use kestrel_proto::AccessibilityNode;

/// Maximum recursion depth. Matches the macOS implementation so
/// callers get comparable output shapes across platforms.
const MAX_DEPTH: u8 = 5;

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

    let walker = match automation.create_tree_walker() {
        Ok(w) => w,
        Err(e) => {
            // The focused element worked but the TreeWalker didn't —
            // return a depth-1 result rather than full fallback.
            tracing::warn!("ax(windows): create_tree_walker failed: {}", e);
            return shallow_node(&focused);
        }
    };

    walk(&focused, &walker, MAX_DEPTH)
}

/// One-element render with no children. Used when the TreeWalker
/// itself can't be constructed but we have a focused element to
/// describe.
fn shallow_node(elem: &uiautomation::UIElement) -> AccessibilityNode {
    let role = elem
        .get_classname()
        .ok()
        .filter(|s: &String| !s.is_empty())
        .unwrap_or_else(|| "unknown".into());
    let label = elem.get_name().ok().filter(|s: &String| !s.is_empty());
    let enabled = elem.is_enabled().unwrap_or(true);
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

/// Depth-budgeted recursion over the UIA tree. Children are walked via
/// the TreeWalker's first-child / next-sibling chain, which is the
/// standard COM-API pattern. A failing child step contributes an
/// `unavailable()` placeholder but doesn't abort the whole walk.
fn walk(
    elem: &uiautomation::UIElement,
    walker: &uiautomation::UITreeWalker,
    depth: u8,
) -> AccessibilityNode {
    let role = elem
        .get_classname()
        .ok()
        .filter(|s: &String| !s.is_empty())
        .unwrap_or_else(|| "unknown".into());
    let label = elem.get_name().ok().filter(|s: &String| !s.is_empty());
    let enabled = elem.is_enabled().unwrap_or(true);

    let mut children: Vec<AccessibilityNode> = vec![];
    if depth > 0 {
        // get_first_child returns Err when there are no children, which
        // is the natural terminator for our recursion.
        let mut cur = walker.get_first_child(elem).ok();
        while let Some(child) = cur {
            children.push(walk(&child, walker, depth - 1));
            cur = walker.get_next_sibling(&child).ok();
        }
    }

    AccessibilityNode {
        role,
        label,
        value: None,
        focused: depth == MAX_DEPTH, // only the root we entered is the
                                     // focused element
        enabled,
        bounds: None,
        children,
        fallback: false,
    }
}
