// crates/kestrel-agent/src/capabilities/world/windows.rs
//
// Windows world observation. Like the Linux module, we ship structural
// plumbing now and fill in real implementations as follow-up — author
// can't runtime-verify from macOS.

use kestrel_proto::{FocusedApp, MousePosition};

pub fn current_focused_app() -> Option<FocusedApp> {
    // TODO: Use the uiautomation crate (already a Windows-only dep
    // from PR #43) to call IUIAutomation::GetFocusedElement and pull
    // the owning process id + name. The shape is similar to what
    // ax/windows.rs already does for the focused tree walk; we'd
    // duplicate the focused-element fetch here for the cheap path
    // (no tree, just root).
    None
}

pub fn current_mouse_position() -> Option<MousePosition> {
    // TODO: Win32 GetCursorPos(&POINT). Simple FFI call; deferred
    // because verifying it requires Windows hardware.
    None
}
