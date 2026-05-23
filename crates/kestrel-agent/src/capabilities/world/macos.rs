// crates/kestrel-agent/src/capabilities/world/macos.rs
//
// macOS-specific world observation. Focused-app discovery reuses the
// NSWorkspace frontmost-app trick from ax.rs; mouse position uses
// CoreGraphics' CGEventSourceGetMouseCursorPosition. Both are cheap
// (~µs) and don't require Accessibility permission (unlike the full
// AX walk in ax.rs).

// objc 0.2 macros expand into cargo-clippy cfg checks; same allow as
// the AX module.
#![allow(unexpected_cfgs)]

use kestrel_proto::{FocusedApp, MousePosition};

pub fn current_focused_app() -> Option<FocusedApp> {
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
            // Name via NSRunningApplication.localizedName. NSString → UTF-8 via UTF8String.
            let ns_name: *mut objc::runtime::Object = msg_send![app, localizedName];
            let name = if ns_name.is_null() {
                "unknown".to_string()
            } else {
                let cstr: *const std::os::raw::c_char = msg_send![ns_name, UTF8String];
                if cstr.is_null() {
                    "unknown".to_string()
                } else {
                    std::ffi::CStr::from_ptr(cstr)
                        .to_string_lossy()
                        .into_owned()
                }
            };
            Some(FocusedApp {
                name,
                pid: pid as u32,
                // NSWorkspace doesn't directly expose the focused window's
                // title. Getting it requires the AX API (see ax.rs's
                // walker) which is much more expensive; deferred for v1.
                // The dashboard "focused" column will show the app
                // name; AI calls describe() if it wants the window title.
                window_title: None,
            })
        })
    }
}

pub fn current_mouse_position() -> Option<MousePosition> {
    // NSEvent.mouseLocation returns the cursor in Cocoa global
    // coordinates (origin at bottom-left of the main screen, Y
    // increasing upward). For dashboard display we report the raw
    // integer values — the AI can cross-reference against
    // `displays[]` to know which monitor.
    use objc::{class, msg_send, sel, sel_impl};
    #[repr(C)]
    #[derive(Copy, Clone)]
    struct NSPoint {
        x: f64,
        y: f64,
    }
    unsafe {
        let pt: NSPoint = msg_send![class!(NSEvent), mouseLocation];
        Some(MousePosition {
            x: pt.x as i32,
            y: pt.y as i32,
            display: 0,
        })
    }
}
