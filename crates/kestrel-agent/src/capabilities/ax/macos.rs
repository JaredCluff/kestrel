// crates/kestrel-agent/src/capabilities/ax/macos.rs
//
// macOS AX implementation. Uses the `accessibility` crate to walk the
// frontmost application's tree up to 5 levels deep. Requires the user
// to have granted Accessibility permission to the kestrel-agent binary
// in System Settings → Privacy & Security → Accessibility; without
// that, the children list comes back empty and `fallback: false` —
// callers should fall back to a screenshot.

use accessibility::{AXUIElement, AXUIElementAttributes};
use kestrel_proto::AccessibilityNode;

pub fn describe() -> AccessibilityNode {
    // Get the focused application via NSWorkspace frontmost app PID.
    // This avoids needing to downcast CFType to AXUIElement.
    match frontmost_app() {
        Some(app) => walk(&app, 5),
        None => AccessibilityNode::unavailable(),
    }
}

/// Returns an AXUIElement for the frontmost (focused) application,
/// or None if AX permission is denied or no frontmost app exists.
fn frontmost_app() -> Option<AXUIElement> {
    use objc::{class, msg_send, sel, sel_impl};
    use objc::rc::autoreleasepool;
    use accessibility_sys::pid_t;

    unsafe {
        autoreleasepool(|| {
            let workspace: *mut objc::runtime::Object =
                msg_send![class!(NSWorkspace), sharedWorkspace];
            if workspace.is_null() {
                return None;
            }
            let app: *mut objc::runtime::Object = msg_send![workspace, frontmostApplication];
            if app.is_null() {
                return None;
            }
            let pid: pid_t = msg_send![app, processIdentifier];
            if pid <= 0 {
                return None;
            }
            Some(AXUIElement::application(pid))
        })
    }
}

fn walk(elem: &AXUIElement, depth: u8) -> AccessibilityNode {
    let role = elem
        .role()
        .map(|s| s.to_string())
        .unwrap_or_else(|_| "unknown".into());

    let label = elem
        .title()
        .ok()
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            elem.description()
                .ok()
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
        });

    let focused = elem.focused().map(|b| bool::from(b)).unwrap_or(false);
    let enabled = elem.enabled().map(|b| bool::from(b)).unwrap_or(true);

    let children: Vec<AccessibilityNode> = if depth == 0 {
        vec![]
    } else {
        match elem.children() {
            Ok(arr) => arr.into_iter().map(|c| walk(&c, depth - 1)).collect(),
            Err(_) => vec![],
        }
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
