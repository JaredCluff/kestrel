// crates/kestrel-agent/src/capabilities/world/windows.rs
//
// Windows world observation. Focused app via UIAutomation
// GetFocusedElement; mouse position via Win32 GetCursorPos.
//
// AUTHOR CAVEAT: written without runtime verification on Windows.
// Compilation is checked via cfg gating; downstream Windows
// installs fill in any drift.

use kestrel_proto::{FocusedApp, MousePosition};

pub fn current_focused_app() -> Option<FocusedApp> {
    // Reuse the uiautomation dep that's already pulled in for the
    // AX module on Windows. UIAutomation::get_focused_element
    // returns the element with keyboard focus; we surface its
    // classname (best proxy for "app" without a separate enumeration
    // of processes) and the owning pid via get_process_id.
    let automation = uiautomation::UIAutomation::new().ok()?;
    let elem = automation.get_focused_element().ok()?;
    let name = elem
        .get_classname()
        .ok()
        .filter(|s: &String| !s.is_empty())
        .unwrap_or_else(|| "unknown".into());
    let window_title = elem.get_name().ok().filter(|s: &String| !s.is_empty());
    let pid = elem.get_process_id().unwrap_or(0) as u32;
    Some(FocusedApp { name, pid, window_title })
}

pub fn current_mouse_position() -> Option<MousePosition> {
    // GetCursorPos(&POINT) — straight Win32. We define the FFI
    // signature inline so we don't pull in a whole windows-rs
    // dep for one call.
    #[repr(C)]
    struct Point { x: i32, y: i32 }
    #[link(name = "user32")]
    unsafe extern "system" {
        fn GetCursorPos(point: *mut Point) -> i32;
    }
    let mut pt = Point { x: 0, y: 0 };
    let ok = unsafe { GetCursorPos(&mut pt) };
    if ok == 0 {
        return None;
    }
    Some(MousePosition { x: pt.x, y: pt.y, display: 0 })
}
