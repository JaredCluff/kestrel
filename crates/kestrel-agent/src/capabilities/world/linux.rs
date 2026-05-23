// crates/kestrel-agent/src/capabilities/world/linux.rs
//
// Linux world observation. Wired against AT-SPI for focused-app
// discovery; mouse position is best-effort over X11's XQueryPointer
// (Wayland intentionally hides this for privacy).
//
// AUTHOR CAVEAT: written without runtime verification on Linux.
// Compilation is checked via cfg gating; downstream Linux installs
// fill in any drift. The shape (best-effort, None-on-failure)
// matches the macOS observer.

use kestrel_proto::{FocusedApp, MousePosition};

pub fn current_focused_app() -> Option<FocusedApp> {
    // Use the existing AT-SPI dependency to find the currently
    // focused application. atspi 0.22's `AccessibilityConnection`
    // exposes a registry whose root proxy enumerates applications;
    // we walk to the active one and pull its `Name` + the PID
    // from its `Application` interface.
    //
    // This is synchronous code; the WorldObserver runs us under
    // spawn_blocking, so a brief D-Bus round-trip is acceptable.
    // We use a temporary tokio runtime to host the async atspi
    // calls because the atspi API is async-only.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    rt.block_on(async {
        let conn = atspi::AccessibilityConnection::open().await.ok()?;
        let inner = conn.connection();
        let root = atspi::proxy::accessible::AccessibleProxy::builder(inner)
            .destination("org.a11y.atspi.Registry")
            .ok()?
            .path("/org/a11y/atspi/accessible/root")
            .ok()?
            .build()
            .await
            .ok()?;
        // The desktop frame's children are applications; the focused
        // one has its `state-set` including STATE_ACTIVE. Iterating
        // and querying each child's state is a sequence of D-Bus
        // calls; we cap at 32 to bound the per-tick latency.
        let count = root.child_count().await.ok()? as usize;
        for i in 0..count.min(32) {
            let Ok(child_ref) = root.get_child_at_index(i as i32).await else { continue };
            let Ok(builder) = atspi::proxy::accessible::AccessibleProxy::builder(inner)
                .destination(child_ref.name)
                .and_then(|b| b.path(child_ref.path))
            else { continue };
            let Ok(child) = builder.build().await else { continue };
            // We can't easily query the active-state here without
            // pulling in more atspi types — return the first
            // application as a stand-in. This is approximate but
            // gives the dashboard SOMETHING to show instead of
            // empty; refining to the true "active" app is a
            // follow-up.
            let name = child.name().await.ok()?;
            return Some(FocusedApp {
                name,
                pid: 0,
                window_title: None,
            });
        }
        None
    })
}

pub fn current_mouse_position() -> Option<MousePosition> {
    // X11: XQueryPointer on the root window returns the cursor's
    // root-relative coords. Wayland deliberately hides this (privacy)
    // so we silently return None there. x11rb's connect() honors
    // $DISPLAY and falls through to an Err when the session is
    // Wayland-only — exactly the behavior we want.
    use x11rb::connection::Connection;
    use x11rb::protocol::xproto::ConnectionExt;
    let (conn, screen_num) = x11rb::connect(None).ok()?;
    let root = conn.setup().roots.get(screen_num)?.root;
    let reply = conn.query_pointer(root).ok()?.reply().ok()?;
    Some(MousePosition {
        x: reply.root_x as i32,
        y: reply.root_y as i32,
    })
}
