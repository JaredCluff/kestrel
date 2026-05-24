// crates/kestrel-hub/src/shutdown.rs
//
// Graceful shutdown plumbing. One `Shutdown` per running hub: a signal
// listener (SIGINT + SIGTERM on Unix, Ctrl-C on Windows) flips a
// broadcast latch; long-running tasks `await shutdown.wait()` to know
// when to drain and exit.
//
// Design notes:
//   - Broadcast (not oneshot) so multiple tasks can independently
//     await the same shutdown signal without coordinating.
//   - Latched via the Arc<AtomicBool> so a task that calls `wait()`
//     AFTER shutdown was signalled still returns immediately — no
//     "missed the broadcast" race.
//   - The signal listener is spawned once at hub start and lives
//     until the process exits.
//
// Usage:
//
//   let shutdown = Shutdown::install();
//   tokio::spawn({
//       let s = shutdown.clone();
//       async move { s.wait().await; do_cleanup(); }
//   });
//   shutdown.wait().await;
//   // proceed with drain

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// Cloneable shutdown handle. All clones share the same latch; flipping
/// it via the signal handler (or an explicit `signal()` call from tests)
/// wakes every task currently parked on `wait()` and immediately
/// returns from any future `wait()` calls.
#[derive(Clone)]
pub struct Shutdown {
    fired: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Shutdown {
    /// Construct a handle without installing any signal listener.
    /// Tests use this so they can drive shutdown explicitly via
    /// `signal()` without race conditions against real SIGINT.
    pub fn new() -> Self {
        Self {
            fired: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Construct a handle AND spawn the signal listener. Returns the
    /// handle; the listener task lives in the background until the
    /// process exits or one of SIGINT / SIGTERM fires.
    pub fn install() -> Self {
        let s = Self::new();
        s.spawn_signal_listener();
        s
    }

    fn spawn_signal_listener(&self) {
        let s = self.clone();
        tokio::spawn(async move {
            // We listen for SIGTERM (kill, systemd stop, docker stop)
            // and SIGINT (Ctrl-C). On Windows tokio::signal::ctrl_c
            // is the only portable choice — there's no SIGTERM on
            // that platform.
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = match signal(SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(
                            "shutdown: cannot install SIGTERM listener: {} \
                             (shutdown will only fire on Ctrl-C)",
                            e
                        );
                        // Still try SIGINT below.
                        let _ = tokio::signal::ctrl_c().await;
                        s.signal();
                        return;
                    }
                };
                tokio::select! {
                    _ = sigterm.recv() => {
                        tracing::info!("shutdown: SIGTERM received");
                    }
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("shutdown: SIGINT (Ctrl-C) received");
                    }
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("shutdown: Ctrl-C received");
            }
            s.signal();
        });
    }

    /// Trip the latch and wake every parked waiter. Idempotent: a
    /// second call is a no-op. Tests call this directly.
    pub fn signal(&self) {
        // SeqCst on the flag because waiters do a load-after-park
        // read; using Release here would allow a parked waiter to see
        // fired=false on wake-up. SeqCst keeps the wait/notify pair
        // sequenced.
        if !self.fired.swap(true, Ordering::SeqCst) {
            // Notify ALL parked waiters, not just one. Each task that
            // independently called `wait()` deserves to wake.
            self.notify.notify_waiters();
        }
    }

    /// Has shutdown been signalled? Cheap; lock-free; useful for
    /// branchless skipping of work (e.g. don't accept a new connection
    /// if shutdown is in progress).
    pub fn is_shutting_down(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    /// Park until shutdown is signalled. Returns immediately if it
    /// already was. Cheap to call from many tasks concurrently.
    pub async fn wait(&self) {
        // Fast-path: shutdown already fired.
        if self.fired.load(Ordering::SeqCst) {
            return;
        }
        // The classic Notify pattern. `notified()` returns a future
        // that ONLY registers as a waiter when polled or enabled, so a
        // naive `let n = ...; if fired { return; } n.await;` has a
        // window where signal() can fire between the second `fired`
        // check and the first poll of `n` — `notify_waiters()` walks
        // an empty waiter list and our future then parks forever.
        // `pin!` + `enable()` registers the waiter eagerly, BEFORE the
        // second flag check, so notify_waiters always sees us.
        let n = self.notify.notified();
        tokio::pin!(n);
        n.as_mut().enable();
        if self.fired.load(Ordering::SeqCst) {
            return;
        }
        n.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn wait_returns_immediately_when_already_signalled() {
        let s = Shutdown::new();
        s.signal();
        // Should not hang.
        tokio::time::timeout(Duration::from_millis(50), s.wait())
            .await
            .expect("wait did not return after signal");
    }

    #[tokio::test]
    async fn wait_wakes_when_signalled() {
        let s = Shutdown::new();
        let waiter = s.clone();
        let h = tokio::spawn(async move { waiter.wait().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        s.signal();
        tokio::time::timeout(Duration::from_millis(50), h)
            .await
            .expect("waiter did not wake")
            .expect("waiter task panicked");
    }

    #[tokio::test]
    async fn multiple_waiters_all_wake() {
        let s = Shutdown::new();
        let mut handles = Vec::new();
        for _ in 0..5 {
            let waiter = s.clone();
            handles.push(tokio::spawn(async move { waiter.wait().await }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        s.signal();
        for h in handles {
            tokio::time::timeout(Duration::from_millis(100), h)
                .await
                .expect("one of the waiters did not wake")
                .expect("waiter task panicked");
        }
    }

    #[tokio::test]
    async fn signal_is_idempotent() {
        let s = Shutdown::new();
        s.signal();
        s.signal();
        s.signal();
        assert!(s.is_shutting_down());
    }

    #[tokio::test]
    async fn is_shutting_down_reflects_signal() {
        let s = Shutdown::new();
        assert!(!s.is_shutting_down());
        s.signal();
        assert!(s.is_shutting_down());
    }

    /// Race regression: in the prior implementation, `wait()` created
    /// the Notified future but didn't enable it before the second
    /// `fired` check. If `signal()` fired exactly in that window, the
    /// notify_waiters call walked an empty waiter list and the
    /// subsequent `.await` would park forever.
    ///
    /// This test reproduces the timing window with many parallel waiters
    /// and a near-simultaneous signal. Without the `pin!` + `enable()`
    /// fix, this hangs reliably under load.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn no_lost_wakeup_when_signal_races_with_wait_registration() {
        for _ in 0..50 {
            let s = Shutdown::new();
            // Spawn many waiters, then signal almost-simultaneously.
            let mut handles = Vec::with_capacity(16);
            for _ in 0..16 {
                let waiter = s.clone();
                handles.push(tokio::spawn(async move { waiter.wait().await }));
            }
            // Yield a few times so spawned tasks get a chance to enter
            // wait(), but with no fixed delay so the race window
            // varies across iterations.
            for _ in 0..3 {
                tokio::task::yield_now().await;
            }
            s.signal();
            for h in handles {
                tokio::time::timeout(Duration::from_secs(1), h)
                    .await
                    .expect("waiter hung past 1s — lost wakeup race")
                    .expect("waiter task panicked");
            }
        }
    }
}
