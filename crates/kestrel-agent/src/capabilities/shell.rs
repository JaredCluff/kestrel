// crates/kestrel-agent/src/capabilities/shell.rs
use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex};
use anyhow::Context;
use kestrel_proto::{KestrelMessage, MsgKind, Payload};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tokio::sync::mpsc::UnboundedSender;

struct PtySession {
    master: Box<dyn portable_pty::MasterPty + Send>,
    writer: Box<dyn std::io::Write + Send>,
}

pub struct ShellManager {
    sessions: Arc<Mutex<HashMap<u32, PtySession>>>,
    next_id: Arc<Mutex<u32>>,
    event_tx: UnboundedSender<KestrelMessage>,
}

impl ShellManager {
    pub fn new(event_tx: UnboundedSender<KestrelMessage>) -> Self {
        ShellManager {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
            event_tx,
        }
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
        pair.slave.spawn_command(cmd).context("spawn_command failed")?;
        drop(pair.slave);

        let mut reader = pair.master.try_clone_reader().context("try_clone_reader")?;
        let writer = pair.master.take_writer().context("take_writer")?;

        let pty_id = {
            let mut next = self.next_id.lock().unwrap();
            let id = *next;
            *next = next.wrapping_add(1);
            id
        };

        self.sessions.lock().unwrap().insert(pty_id, PtySession { master: pair.master, writer });

        let event_tx = self.event_tx.clone();
        let sessions = self.sessions.clone();
        let id = pty_id;
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if event_tx
                            .send(KestrelMessage {
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
            sessions.lock().unwrap().remove(&id);
            let _ = event_tx.send(KestrelMessage {
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
        session.writer.write_all(data).context("pty write_all")
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
        self.sessions.lock().unwrap().remove(&pty_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn shell_manager_spawn_and_receive_output() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let mgr = ShellManager::new(event_tx);
        let pty_id = mgr.spawn(None, 80, 24).expect("spawn shell");

        mgr.write(pty_id, b"echo kestrel-shell-test\nexit\n").expect("write to shell");

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
        assert!(output.contains("kestrel-shell-test"), "expected echo output in: {:?}", output);
    }
}
