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
use tokio::sync::Mutex;

use crate::config::NodeLayout;
use crate::router::NodeRegistry;

struct KvmState {
    layout: Vec<NodeLayout>,
    registry: Arc<NodeRegistry>,
    focused: Option<String>,
    virt_x: f64,
    virt_y: f64,
    local_w: f64,
    local_h: f64,
}

impl KvmState {
    fn new(layout: Vec<NodeLayout>, registry: Arc<NodeRegistry>, local_w: u32, local_h: u32) -> Self {
        KvmState { layout, registry, focused: None, virt_x: 0.5, virt_y: 0.5,
            local_w: local_w as f64, local_h: local_h as f64 }
    }

    fn find_neighbor(&self, dc: i32, dr: i32) -> Option<&NodeLayout> {
        let (base_col, base_row) = match &self.focused {
            None => (0, 0),
            Some(f) => {
                let lay = self.layout.iter().find(|l| &l.node_id == f)?;
                (lay.col, lay.row)
            }
        };
        self.layout.iter().find(|l| l.col == base_col + dc && l.row == base_row + dr)
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
            self.find_neighbor(1, 0).map(|n| (n.node_id.clone(), 0.01_f64, norm_y))
        } else if norm_x <= 0.01 {
            self.find_neighbor(-1, 0).map(|n| (n.node_id.clone(), 0.99_f64, norm_y))
        } else if norm_y <= 0.01 {
            self.find_neighbor(0, -1).map(|n| (n.node_id.clone(), norm_x, 0.99_f64))
        } else if norm_y >= 0.99 {
            self.find_neighbor(0, 1).map(|n| (n.node_id.clone(), norm_x, 0.01_f64))
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

pub fn start(layout: Vec<NodeLayout>, registry: Arc<NodeRegistry>) {
    if layout.is_empty() {
        tracing::info!("KVM: no layout configured, disabled");
        return;
    }

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
