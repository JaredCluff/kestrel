// crates/kestrel-agent/src/capabilities/shell.rs
use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex};
use anyhow::Context;
use kestrel_proto::{KestrelMessage, MsgKind, Payload};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tokio::sync::mpsc::Sender;

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Capacity of the shell-output channel between PTY reader threads and the
/// outbound WS sender. Bounded so a stalled hub (TCP backpressure, paused
/// client) cannot OOM the agent by letting PTY output queue without limit.
/// When full, the reader thread's `blocking_send` blocks the OS thread,
/// which in turn stops draining the PTY — kernel pipe buffer fills, the
/// PTY child blocks on its own writes. That's the desired backpressure.
pub const SHELL_EVENT_CAPACITY: usize = 128;

struct PtySession {
    master: Box<dyn portable_pty::MasterPty + Send>,
    writer: Box<dyn std::io::Write + Send>,
    /// Child process handle so we can kill it on close. portable-pty's `Child`
    /// trait requires `&mut`, so we wrap it in a Mutex for shared access.
    child: Arc<Mutex<Box<dyn portable_pty::Child + Send + Sync>>>,
}

/// Phase 6 follow-up: per-PTY observable metadata for the
/// WorldObserver. Tracked separately from PtySession so the observer
/// can query a snapshot without holding the sessions Mutex while
/// async work happens.
pub struct ShellMeta {
    pub alive: bool,
    /// Bytes written into the PTY since spawn. Approximate measure of
    /// activity; not the hub-side buffer length (which the agent
    /// doesn't directly know).
    pub bytes_written: u64,
    pub last_write_unix: u64,
}

pub struct ShellManager {
    sessions: Arc<Mutex<HashMap<u32, PtySession>>>,
    next_id: Arc<Mutex<u32>>,
    event_tx: Sender<KestrelMessage>,
    /// Per-pty observable state. Updated synchronously by spawn /
    /// write / close. Read by the WorldObserver via meta_snapshot().
    meta: Arc<Mutex<HashMap<u32, ShellMeta>>>,
}

impl ShellManager {
    pub fn new(event_tx: Sender<KestrelMessage>) -> Self {
        ShellManager {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
            event_tx,
            meta: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Phase 6 follow-up: hand the meta-Arc to outside callers so
    /// the WorldObserver can poll it without going through this
    /// manager. Holding the Arc keeps the metadata alive past
    /// ShellManager drop (mostly a non-issue — ShellManager outlives
    /// the observer in practice).
    pub fn meta_handle(&self) -> Arc<Mutex<HashMap<u32, ShellMeta>>> {
        self.meta.clone()
    }

    pub fn spawn(&self, shell: Option<String>, cols: u16, rows: u16) -> anyhow::Result<u32> {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize { rows, cols, pixel_width: 0, pixel_height: 0 })
            .context("openpty failed")?;

        let shell_path = shell.unwrap_or_else(|| {
            std::env::var("SHELL").unwrap_or_else(|_| {
                if cfg!(target_os = "windows") { "cmd.exe".into() } else { "/bin/sh".into() }
            })
        });
        let cmd = CommandBuilder::new(&shell_path);
        let child = pair.slave.spawn_command(cmd).context("spawn_command failed")?;
        drop(pair.slave);

        let mut reader = pair.master.try_clone_reader().context("try_clone_reader")?;
        let writer = pair.master.take_writer().context("take_writer")?;
        let child = Arc::new(Mutex::new(child));

        let pty_id = {
            let mut next = self.next_id.lock().unwrap();
            let id = *next;
            *next = next.wrapping_add(1);
            id
        };

        self.sessions.lock().unwrap().insert(
            pty_id,
            PtySession { master: pair.master, writer, child: child.clone() },
        );
        self.meta.lock().unwrap().insert(
            pty_id,
            ShellMeta {
                alive: true,
                bytes_written: 0,
                last_write_unix: now_unix(),
            },
        );

        let event_tx = self.event_tx.clone();
        let sessions = self.sessions.clone();
        let meta_for_reader = self.meta.clone();
        let reader_child = child.clone();
        let id = pty_id;
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if event_tx
                            .blocking_send(KestrelMessage {
                                stream_id: 0,
                                kind: MsgKind::Event,
                                payload: Payload::ShellOutput { pty_id: id, data: buf[..n].to_vec() },
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
            // Reader saw EOF (child exited) or our channel closed (hub gone).
            // Reap the child explicitly so it doesn't linger as a zombie in
            // the process table — portable-pty wraps std::process::Child, and
            // std::process::Child requires wait() to be called or the OS
            // entry leaks until the agent process exits.
            if let Ok(mut c) = reader_child.lock() {
                let _ = c.wait();
            }
            sessions.lock().unwrap().remove(&id);
            if let Some(m) = meta_for_reader.lock().unwrap().get_mut(&id) {
                m.alive = false;
            }
            let _ = event_tx.blocking_send(KestrelMessage {
                stream_id: 0,
                kind: MsgKind::Event,
                payload: Payload::ShellClose { pty_id: id },
            });
        });

        Ok(pty_id)
    }

    pub fn write(&self, pty_id: u32, data: &[u8]) -> anyhow::Result<()> {
        use std::io::Write;
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions
            .get_mut(&pty_id)
            .ok_or_else(|| anyhow::anyhow!("pty_id {} not found", pty_id))?;
        session.writer.write_all(data).context("pty write_all")?;
        if let Some(m) = self.meta.lock().unwrap().get_mut(&pty_id) {
            m.bytes_written = m.bytes_written.saturating_add(data.len() as u64);
            m.last_write_unix = now_unix();
        }
        Ok(())
    }

    pub fn resize(&self, pty_id: u32, cols: u16, rows: u16) -> anyhow::Result<()> {
        let sessions = self.sessions.lock().unwrap();
        let session = sessions
            .get(&pty_id)
            .ok_or_else(|| anyhow::anyhow!("pty_id {} not found", pty_id))?;
        session
            .master
            .resize(PtySize { rows, cols, pixel_width: 0, pixel_height: 0 })
            .context("pty resize")
    }

    pub fn close(&self, pty_id: u32) {
        // Kill the child first; dropping the master FD doesn't unblock the reader's
        // blocking read() on Linux until the child exits. Best-effort — if the
        // child is already gone, kill() returns an error we ignore.
        //
        // The reader thread is responsible for reaping (wait()) the child once
        // it sees EOF — we don't wait() here so close() stays non-blocking.
        // SIGKILL then EOF on the master FD is enough to drive the reader to
        // exit, and the reader's wait() prevents zombies.
        if let Some(session) = self.sessions.lock().unwrap().remove(&pty_id) {
            if let Ok(mut child) = session.child.lock() {
                let _ = child.kill();
            }
        }
    }

    /// Close every active PTY. Called on agent shutdown / WS disconnect so that
    /// orphaned shells don't linger between sessions.
    pub fn close_all(&self) {
        let ids: Vec<u32> = self.sessions.lock().unwrap().keys().copied().collect();
        for id in ids {
            self.close(id);
        }
    }
}

impl Drop for ShellManager {
    fn drop(&mut self) {
        self.close_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn shell_manager_spawn_and_receive_output() {
        // PTY slave ttys have ECHO enabled by default, so the kernel echoes
        // input bytes back through the master before the shell touches them.
        // If we asserted on a literal substring that's also in the input, the
        // assertion would pass even if /bin/sh were missing or never executed
        // the command. Use arithmetic expansion so the assertion target
        // ("TAG_77_END") only appears AFTER the shell evaluates $((7*11)).
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(SHELL_EVENT_CAPACITY);
        let mgr = ShellManager::new(event_tx);
        let pty_id = mgr.spawn(None, 80, 24).expect("spawn shell");

        mgr.write(pty_id, b"printf 'TAG_%d_END\n' $((7*11))\nexit\n")
            .expect("write to shell");

        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let output = rt.block_on(async {
            let mut buf = Vec::new();
            let timeout = tokio::time::sleep(Duration::from_secs(5));
            tokio::pin!(timeout);
            loop {
                tokio::select! {
                    _ = &mut timeout => break,
                    msg = event_rx.recv() => {
                        match msg {
                            Some(km) => match km.payload {
                                kestrel_proto::Payload::ShellOutput { data, .. } => buf.extend(data),
                                kestrel_proto::Payload::ShellClose { .. } => break,
                                _ => {}
                            },
                            None => break,
                        }
                    }
                }
            }
            String::from_utf8_lossy(&buf).into_owned()
        });
        assert!(
            output.contains("TAG_77_END"),
            "expected arithmetic-expanded 'TAG_77_END' in shell output, got: {:?}",
            output
        );
    }
}
