// crates/kestrel-agent/src/capabilities/world/mod.rs
//
// WorldObserver — periodically samples the agent's observable local
// state (focused app, mouse position, clipboard metadata) and pushes
// a `Payload::WorldUpdate` event back to the hub only when something
// has changed. Same channel the shell-event pump uses; same bounded
// backpressure semantics.
//
// Cadence: 2s. Slower than a human's perception of "live", fast
// enough that the AI's turn-by-turn polling sees state at most ~2s
// stale. The cost per observation is dominated by clipboard read
// (~ms), focused-app query (~ms on macOS via NSWorkspace, similar
// elsewhere), and mouse position (~µs).
//
// Per-platform observation lives in os-specific submodules. The
// non-OS-specific glue (clipboard fingerprint, change detection,
// channel send) lives here.
//
// Failure model: any error during observation degrades that field to
// `None`. We never propagate observation errors to the hub — a
// partial snapshot is more useful than no snapshot.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc::Sender;

use kestrel_proto::{
    Capabilities, ClipboardKind, ClipboardMetadata, DisplayInfo, FocusedApp, KestrelMessage,
    MousePosition, MsgKind, Payload, ShellSession, WorldState,
};
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use sha2::{Digest, Sha256};

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "windows")]
mod windows;

/// How often the observer takes a snapshot. 2s balances "AI sees fresh
/// state on every turn" against "agent doesn't burn CPU on idle
/// machines."
pub const OBSERVE_INTERVAL: Duration = Duration::from_secs(2);

/// Holds an event-channel handle + the last sent state so we only push
/// `WorldUpdate` when something actually changed (defensive
/// duplication on top of the hub's no-op-on-identical-state check).
pub struct WorldObserver {
    event_tx: Sender<KestrelMessage>,
    /// Display geometry — captured once at observer construction; we
    /// re-emit it in every WorldState so the hub doesn't have to
    /// remember it from the SystemInfo handshake. Doesn't change at
    /// runtime under normal use (monitors hot-plugged would need an
    /// observer restart; out of scope for v1).
    displays: Vec<DisplayInfo>,
    /// Last `WorldState` actually sent on the wire. Held inside the
    /// `AsyncMutex` because `run()` consumes & updates it under .await.
    last_sent: AsyncMutex<WorldState>,
    /// Phase 6 follow-up: shell-session metadata handle from the
    /// ShellManager. Polled every tick to populate
    /// WorldState.shells.
    shell_meta: Arc<StdMutex<HashMap<u32, crate::capabilities::shell::ShellMeta>>>,
}

impl WorldObserver {
    pub fn new(event_tx: Sender<KestrelMessage>, displays: Vec<DisplayInfo>) -> Arc<Self> {
        // Default constructor with no shell tracking — used by tests
        // that don't have a ShellManager. Real callers go through
        // with_shells.
        Self::with_shells(
            event_tx,
            displays,
            Arc::new(StdMutex::new(HashMap::new())),
        )
    }

    pub fn with_shells(
        event_tx: Sender<KestrelMessage>,
        displays: Vec<DisplayInfo>,
        shell_meta: Arc<StdMutex<HashMap<u32, crate::capabilities::shell::ShellMeta>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            event_tx,
            displays,
            last_sent: AsyncMutex::new(WorldState::empty()),
            shell_meta,
        })
    }

    /// Phase 8 follow-up: emit a fresh Capabilities frame when the
    /// observer detects a change worth re-broadcasting (e.g., display
    /// plugged in, docker started). Best-effort: hub ignores
    /// Capabilities frames that aren't different from the previous
    /// one (the recording de-dupes).
    pub async fn push_capabilities_now(&self) {
        let caps = Capabilities {
            os: std::env::consts::OS.into(),
            has_gpu: false,
            has_display: !self.displays.is_empty(),
            has_sudo: false,
            has_docker: docker_socket_present(),
        };
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::Capabilities { caps },
        };
        let _ = self.event_tx.try_send(msg);
    }

    /// Spawn the observation loop. Returns immediately; the loop runs
    /// until the underlying event channel closes (which happens when
    /// the connection's receiver side is dropped on disconnect), then
    /// exits cleanly. No JoinHandle.abort() needed from the caller.
    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(OBSERVE_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Re-emit capabilities periodically so dynamic changes
            // (docker started, display plugged in) are observable
            // hub-side without restarting the agent. Hub-side
            // record_capabilities() de-dupes against unchanged
            // values so a re-send when nothing changed is cheap.
            const CAP_REEMIT_EVERY: u64 = 15;
            let mut cap_tick: u64 = 0;
            loop {
                tick.tick().await;
                if !self.observe_and_maybe_send().await {
                    break;
                }
                cap_tick = cap_tick.wrapping_add(1);
                if cap_tick.is_multiple_of(CAP_REEMIT_EVERY) {
                    self.push_capabilities_now().await;
                }
            }
        })
    }

    /// Sample state and push a WorldUpdate if anything has changed.
    /// Returns `false` when the event channel's receiver has been
    /// dropped — caller breaks the spawn loop on that signal so the
    /// observer task doesn't leak past a connection drop.
    async fn observe_and_maybe_send(&self) -> bool {
        let observed = self.observe().await;
        let mut last = self.last_sent.lock().await;
        // Compare excluding `last_observed_unix` (always changes per
        // tick). Cheap struct equality on all the other fields.
        let mut probe = observed.clone();
        probe.last_observed_unix = last.last_observed_unix;
        if probe == *last {
            return true; // unchanged; keep observing
        }
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::WorldUpdate { state: observed.clone() },
        };
        match self.event_tx.try_send(msg) {
            Ok(()) => {
                *last = observed;
                true
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                // Backpressure: the WS sender is behind. Skip this
                // observation; we'll retry on the next tick. Keep
                // last_sent unchanged so the next tick re-detects
                // the same change.
                true
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                // Receiver dropped — connection's gone. Exit the loop.
                false
            }
        }
    }

    async fn observe(&self) -> WorldState {
        // Per-platform observation. Each helper returns `None` on
        // failure, which degrades that field rather than aborting
        // the whole snapshot.
        //
        // CRITICAL: focused_app and clipboard query OS APIs that
        // can briefly block (NSWorkspace lock, AppKit pasteboard
        // lock). On a current_thread tokio runtime those blocks
        // would stall every other task — including the WS reader
        // that handles ping/pong, which we saw add 1700ms RTT to
        // a Phase-8 regression. Run them on the blocking pool.
        let displays = self.displays.clone();
        let shell_meta = self.shell_meta.clone();
        tokio::task::spawn_blocking(move || {
            let focused_app = current_focused_app();
            let mouse = current_mouse_position();
            let clipboard = current_clipboard_metadata();
            let screen_fingerprint = current_screen_fingerprint();
            // Snapshot shell metadata. Sorted by pty_id so the
            // resulting Vec compares equal across ticks when nothing
            // changed (HashMap iteration order is non-deterministic).
            let shells: Vec<ShellSession> = {
                let map = shell_meta.lock().expect("shell meta poisoned");
                let mut entries: Vec<ShellSession> = map
                    .iter()
                    .map(|(pty_id, m)| ShellSession {
                        pty_id: *pty_id,
                        alive: m.alive,
                        buffered_bytes: m.bytes_written,
                        last_write_unix: m.last_write_unix,
                    })
                    .collect();
                entries.sort_by_key(|s| s.pty_id);
                entries
            };
            WorldState {
                focused_app,
                mouse,
                displays,
                clipboard,
                shells,
                screen_fingerprint,
                last_observed_unix: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            }
        })
        .await
        .unwrap_or_else(|_| WorldState::empty())
    }
}

// ---- Per-platform dispatch -------------------------------------------------

#[cfg(target_os = "macos")]
fn current_focused_app() -> Option<FocusedApp> { macos::current_focused_app() }
#[cfg(target_os = "linux")]
fn current_focused_app() -> Option<FocusedApp> { linux::current_focused_app() }
#[cfg(target_os = "windows")]
fn current_focused_app() -> Option<FocusedApp> { windows::current_focused_app() }
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
fn current_focused_app() -> Option<FocusedApp> { None }

#[cfg(target_os = "macos")]
fn current_mouse_position() -> Option<MousePosition> { macos::current_mouse_position() }
#[cfg(target_os = "linux")]
fn current_mouse_position() -> Option<MousePosition> { linux::current_mouse_position() }
#[cfg(target_os = "windows")]
fn current_mouse_position() -> Option<MousePosition> { windows::current_mouse_position() }
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
fn current_mouse_position() -> Option<MousePosition> { None }

// ---- Clipboard fingerprint (cross-platform via arboard) --------------------

/// Read the clipboard and return its metadata. NEVER returns the
/// content — the byte length and a truncated SHA-256 fingerprint are
/// enough for the AI to detect change. If the clipboard is empty,
/// inaccessible, or arboard fails, returns `None`.
///
/// Note: arboard's `get_image()` is moderately expensive (decodes the
/// platform clipboard's image representation). We do this every 2s
/// while the observer runs. If perf ever bites, this is the first
/// place to add a "skip image read; only fingerprint text" knob.
fn current_clipboard_metadata() -> Option<ClipboardMetadata> {
    let mut cb = arboard::Clipboard::new().ok()?;
    // Try text first; if present, summarize it.
    if let Ok(text) = cb.get_text() {
        if !text.is_empty() {
            return Some(ClipboardMetadata {
                kind: ClipboardKind::Text,
                byte_len: text.len() as u64,
                fingerprint_hex: fingerprint(text.as_bytes()),
            });
        }
    }
    // Otherwise try image.
    if let Ok(img) = cb.get_image() {
        // arboard returns `ImageData { width, height, bytes: Cow<[u8]> }`.
        // bytes is the raw RGBA buffer; len = w*h*4.
        return Some(ClipboardMetadata {
            kind: ClipboardKind::Image,
            byte_len: img.bytes.len() as u64,
            fingerprint_hex: fingerprint(&img.bytes),
        });
    }
    None
}

/// First 16 hex chars of SHA-256(bytes). Truncated so a leaked
/// fingerprint can't be brute-forced back to short clipboard strings.
fn fingerprint(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let hex = format!("{:x}", digest);
    hex.chars().take(16).collect()
}

/// Cheap socket-existence probe (duplicates transport.rs's helper
/// for cross-module reuse — the world observer doesn't depend on
/// transport). Windows arm returns false; see transport.rs for the
/// named-pipe TODO.
fn docker_socket_present() -> bool {
    #[cfg(unix)]
    {
        ["/var/run/docker.sock", "/run/docker.sock"]
            .iter()
            .any(|p| std::path::Path::new(p).exists())
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// Compute a screen fingerprint by sampling the primary display
/// down to an 8×8 luminance grid and hashing the resulting 64 bytes.
/// Two screens that look approximately the same hash the same. None
/// if there's no display or capture fails.
///
/// Cost: one screen capture (~10ms on macOS via xcap), one
/// downsample, one SHA-256 of 64 bytes. Cheap enough to run every
/// 2s alongside the other observation.
fn current_screen_fingerprint() -> Option<String> {
    let monitors = xcap::Monitor::all().ok()?;
    let mon = monitors.into_iter().next()?;
    let img = mon.capture_image().ok()?;
    // Downsample to 8x8 luminance. img is image::RgbaImage; iterate
    // a coarse grid and accumulate luminance per cell.
    const N: u32 = 8;
    let (w, h) = (img.width(), img.height());
    if w == 0 || h == 0 { return None; }
    let mut cells = [0u32; 64];
    let mut counts = [0u32; 64];
    for y in 0..h {
        let cy = (y * N / h).min(N - 1) as usize;
        for x in 0..w {
            let cx = (x * N / w).min(N - 1) as usize;
            let p = img.get_pixel(x, y);
            // ITU-R BT.601 luminance approximation.
            let lum = (p[0] as u32 * 299 + p[1] as u32 * 587 + p[2] as u32 * 114) / 1000;
            let idx = cy * 8 + cx;
            cells[idx] = cells[idx].saturating_add(lum);
            counts[idx] = counts[idx].saturating_add(1);
        }
    }
    let mut grid = [0u8; 64];
    for i in 0..64 {
        grid[i] = if counts[i] == 0 { 0 } else { (cells[i] / counts[i]).min(255) as u8 };
    }
    Some(fingerprint(&grid))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_16_hex_chars_and_stable() {
        let a = fingerprint(b"hello");
        let b = fingerprint(b"hello");
        let c = fingerprint(b"helloo");
        assert_eq!(a.len(), 16, "fingerprint must be 16 hex chars");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(a, b, "same input must produce same fingerprint");
        assert_ne!(a, c, "different inputs must produce different fingerprints");
    }

    #[tokio::test]
    async fn observer_send_skipped_when_only_timestamp_differs() {
        // Two observations that differ only in last_observed_unix
        // must NOT trigger a push — that's the whole point of the
        // "exclude timestamp from the comparison" logic in
        // observe_and_maybe_send. Tested at the level of the
        // private impl with a synthetic WorldState.
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let obs = WorldObserver::new(tx, vec![]);

        // Seed last_sent with a state at time t.
        {
            let mut guard = obs.last_sent.lock().await;
            *guard = WorldState {
                focused_app: Some(FocusedApp {
                    name: "Safari".into(),
                    pid: 1,
                    window_title: None,
                }),
                mouse: None,
                displays: vec![],
                clipboard: None,
                shells: vec![],
                screen_fingerprint: None,
                last_observed_unix: 100,
            };
        }
        // Manually call the path that observation would hit — same
        // focused_app, different timestamp. Real observation would
        // yield an identical state shape with a fresh timestamp.
        // We can't call observe() directly because it would query
        // real system state on the test runner; instead bypass and
        // hand-craft the "observed" state.
        let observed = WorldState {
            focused_app: Some(FocusedApp {
                name: "Safari".into(),
                pid: 1,
                window_title: None,
            }),
            mouse: None,
            displays: vec![],
            clipboard: None,
            shells: vec![],
            screen_fingerprint: None,
            last_observed_unix: 200, // newer
        };
        // Replicate observe_and_maybe_send's diff logic.
        let last = obs.last_sent.lock().await;
        let mut probe = observed.clone();
        probe.last_observed_unix = last.last_observed_unix;
        let would_send = probe != *last;
        drop(last);
        assert!(
            !would_send,
            "identical state differing only in timestamp must not trigger a send"
        );
        // And nothing arrived on the channel (defensive belt-and-braces).
        assert!(rx.try_recv().is_err(), "channel should be empty");
    }

    #[tokio::test]
    async fn observer_send_triggered_when_focused_app_changes() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let obs = WorldObserver::new(tx, vec![]);
        let observed = WorldState {
            focused_app: Some(FocusedApp {
                name: "Mail".into(),
                pid: 2,
                window_title: None,
            }),
            mouse: None,
            displays: vec![],
            clipboard: None,
            shells: vec![],
            screen_fingerprint: None,
            last_observed_unix: 300,
        };
        // last_sent starts at WorldState::empty(); any non-empty observation
        // differs from it.
        {
            let last = obs.last_sent.lock().await;
            let mut probe = observed.clone();
            probe.last_observed_unix = last.last_observed_unix;
            assert!(probe != *last);
        }
        // Drive observe_and_maybe_send via a hand-rolled equivalent so
        // we don't have to make observe() injectable for the test.
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::WorldUpdate { state: observed.clone() },
        };
        obs.event_tx.try_send(msg).unwrap();
        {
            let mut last = obs.last_sent.lock().await;
            *last = observed.clone();
        }
        // One frame should now be on the channel.
        let frame = rx.try_recv().expect("frame should be queued");
        match frame.payload {
            Payload::WorldUpdate { state } => {
                assert_eq!(state.focused_app.unwrap().name, "Mail");
            }
            other => panic!("expected WorldUpdate, got {:?}", other),
        }
    }
}
