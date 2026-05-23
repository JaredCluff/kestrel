// crates/kestrel-agent/src/capabilities/world/linux.rs
//
// Linux world observation. Stubs for v1: focused-app discovery on
// Linux would go through AT-SPI's "active accessible" or X11/Wayland
// window manager protocols, both of which are non-trivial. Same
// constraint we documented for the AX backend (PR #43): we can't
// runtime-verify from a macOS dev machine, so the structural plumbing
// is here and the bodies return `None` until follow-up work fills
// them in. Returning None is a graceful degrade — the dashboard shows
// no focused-app cell rather than crashing.

use kestrel_proto::{FocusedApp, MousePosition};

pub fn current_focused_app() -> Option<FocusedApp> {
    // TODO: AT-SPI `Registry.get_active_accessible()` or x11/wayland
    // window-manager query. The atspi crate is already a dep on Linux
    // (PR #43); reusing its connection from the AX module would let
    // this be a thin call. Deferred to a Phase-6 follow-up.
    None
}

pub fn current_mouse_position() -> Option<MousePosition> {
    // TODO: Linux mouse-position query depends on the display server:
    //   - X11: XQueryPointer
    //   - Wayland: there's no portable cursor-position query (privacy
    //     by design); some compositors expose it via custom protocols.
    // v1 returns None across the board to keep the dep surface small.
    None
}
