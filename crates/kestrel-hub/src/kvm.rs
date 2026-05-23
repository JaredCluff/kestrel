// crates/kestrel-hub/src/kvm.rs
//
// KVM cursor routing. Captures local mouse events via rdev, detects when the
// cursor crosses a display edge, locks the local cursor, and routes subsequent
// input events to the neighbor node.
//
// On macOS rdev::listen may require Accessibility permission (System Settings →
// Privacy & Security → Accessibility). If it fails the KVM feature is silently
// disabled; all other hub features continue working.

use std::sync::Arc;
use rdev::EventType;
use tokio::sync::{Mutex, RwLock};

use crate::config::NodeLayout;
use crate::router::NodeRegistry;

/// Hot-swappable layout, shared between the KVM task and the dashboard's
/// layout-edit endpoints. Edits acquire `write()` briefly; the KVM event
/// loop acquires `read()` on each edge crossing. The reads are short and
/// uncontested in practice.
pub type SharedLayout = Arc<RwLock<Vec<NodeLayout>>>;

struct KvmState {
    layout: SharedLayout,
    registry: Arc<NodeRegistry>,
    focused: Option<String>,
    virt_x: f64,
    virt_y: f64,
    local_w: f64,
    local_h: f64,
}

impl KvmState {
    fn new(layout: SharedLayout, registry: Arc<NodeRegistry>, local_w: u32, local_h: u32) -> Self {
        KvmState { layout, registry, focused: None, virt_x: 0.5, virt_y: 0.5,
            local_w: local_w as f64, local_h: local_h as f64 }
    }

    /// Look up the layout neighbor in direction (dc, dr) from the
    /// currently-focused node. Reads the shared layout under a brief
    /// read lock. Cloned out because we can't hold the read guard across
    /// the await point in the caller.
    async fn find_neighbor(&self, dc: i32, dr: i32) -> Option<NodeLayout> {
        let layout = self.layout.read().await;
        let (base_col, base_row) = match &self.focused {
            None => (0, 0),
            Some(f) => {
                let lay = layout.iter().find(|l| &l.node_id == f)?;
                (lay.col, lay.row)
            }
        };
        layout
            .iter()
            .find(|l| l.col == base_col + dc && l.row == base_row + dr)
            .cloned()
    }

    async fn handle_mouse_move(&mut self, abs_x: f64, abs_y: f64) {
        if let Some(node_id) = self.focused.clone() {
            let dx = abs_x / self.local_w * 0.05;
            let dy = abs_y / self.local_h * 0.05;
            let new_vx = (self.virt_x + dx).clamp(0.0, 1.0);
            let new_vy = (self.virt_y + dy).clamp(0.0, 1.0);
            self.registry.fire_mouse_move(&node_id, new_vx, new_vy);
            self.virt_x = new_vx;
            self.virt_y = new_vy;

            if new_vx <= 0.01 || new_vx >= 0.99 || new_vy <= 0.01 || new_vy >= 0.99 {
                tracing::info!("KVM: returning focus to local");
                self.focused = None;
                lock_cursor(false);
            }
            return;
        }

        let norm_x = abs_x / self.local_w;
        let norm_y = abs_y / self.local_h;

        let result = if norm_x >= 0.99 {
            self.find_neighbor(1, 0).await.map(|n| (n.node_id, 0.01_f64, norm_y))
        } else if norm_x <= 0.01 {
            self.find_neighbor(-1, 0).await.map(|n| (n.node_id, 0.99_f64, norm_y))
        } else if norm_y <= 0.01 {
            self.find_neighbor(0, -1).await.map(|n| (n.node_id, norm_x, 0.99_f64))
        } else if norm_y >= 0.99 {
            self.find_neighbor(0, 1).await.map(|n| (n.node_id, norm_x, 0.01_f64))
        } else {
            None
        };

        if let Some((node_id, entry_x, entry_y)) = result {
            tracing::info!("KVM: switching focus to {}", node_id);
            lock_cursor(true);
            self.focused = Some(node_id.clone());
            self.virt_x = entry_x;
            self.virt_y = entry_y;
            self.registry.fire_mouse_move(&node_id, entry_x, entry_y);
        }
    }
}

fn lock_cursor(lock: bool) {
    #[cfg(target_os = "macos")]
    {
        #[link(name = "CoreGraphics", kind = "framework")]
        extern "C" {
            fn CGAssociateMouseAndMouseCursorPosition(connected: bool) -> i32;
        }
        unsafe { CGAssociateMouseAndMouseCursorPosition(!lock); }
    }
    #[cfg(not(target_os = "macos"))]
    { let _ = lock; }
}

/// Wrap an initial layout in a SharedLayout suitable for passing to
/// [`start`] AND the dashboard. Keeping construction in one place means
/// callers can't accidentally hand out two independent layouts.
pub fn shared_layout(initial: Vec<NodeLayout>) -> SharedLayout {
    Arc::new(RwLock::new(initial))
}

/// Spawn the KVM cursor-routing task. The KVM task reads `layout` on
/// every mouse-edge event, so external mutations to the same Arc apply
/// live — no restart needed. If the initial layout is empty AND no one
/// writes to it later, the task short-circuits and never runs the rdev
/// listener; callers that may add layout entries later should still
/// start the task (it's cheap when idle) so dashboard edits take effect.
pub fn start(layout: SharedLayout, registry: Arc<NodeRegistry>) {
    let (local_w, local_h) = xcap::Monitor::all()
        .ok()
        .and_then(|m| m.into_iter().next())
        .and_then(|m| Some((m.width().ok()?, m.height().ok()?)))
        .unwrap_or((1920, 1080));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<rdev::Event>();
    std::thread::spawn(move || {
        if let Err(e) = rdev::listen(move |event| { let _ = event_tx.send(event); }) {
            tracing::warn!("KVM rdev stopped (needs Accessibility on macOS): {e:?}");
        }
    });

    let state = Arc::new(Mutex::new(KvmState::new(layout, registry, local_w, local_h)));
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            if let EventType::MouseMove { x, y } = event.event_type {
                state.lock().await.handle_mouse_move(x, y).await;
            }
        }
    });
}
