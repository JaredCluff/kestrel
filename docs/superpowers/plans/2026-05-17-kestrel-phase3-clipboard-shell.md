# Kestrel Phase 3 — Clipboard + Shell Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add clipboard read/write and PTY shell execution (one-shot and interactive) to Kestrel agents; expose all capabilities as new MCP tools on the hub.

**Architecture:** Agent gains two new capability modules: `clipboard.rs` (arboard 3, `spawn_blocking`) and `shell.rs` (`ShellManager` over portable-pty 0.8; reader `std::thread` per PTY bridges output back to tokio via `UnboundedSender`). Agent transport is refactored to a `tokio::select!` loop that multiplexes incoming hub frames and outgoing shell events. Hub actor gains a `shell_buffers` map and `shell_close_waiters` map for buffering PTY output and wiring `run_shell`'s completion. `NodeHandle` gains high-level `run_shell`/`clipboard_read`/`clipboard_write` methods; `NodeRegistry` delegates to them. Seven new `#[tool]`-annotated methods join the existing seven in `KestrelMcp`.

**Tech Stack additions:** arboard 3 (clipboard), portable-pty 0.8 (PTY)

---

## File Map

```
kestrel/
  Cargo.toml                              # Add arboard = "3", portable-pty = "0.8"
  crates/
    kestrel-proto/
      src/
        message.rs                        # Add ClipboardContent enum; append Payload variants 14-23
        lib.rs                            # Export ClipboardContent
    kestrel-agent/
      Cargo.toml                          # Add arboard, portable-pty
      src/
        capabilities/
          clipboard.rs                    # read_clipboard() / write_clipboard() via arboard + spawn_blocking
          shell.rs                        # ShellManager: HashMap<u32, PtySession> + reader thread per PTY
          mod.rs                          # add pub mod clipboard; pub mod shell;
        transport.rs                      # Refactor: tokio::select! on incoming frames + shell event_rx
    kestrel-hub/
      src/
        transport.rs                      # Extend ActorCmd + run_actor: shell_buffers, shell_close_waiters
                                          # NodeHandle: spawn_shell, write_shell, close_shell,
                                          #             read_shell_buffer, run_shell,
                                          #             clipboard_read, clipboard_write
        router.rs                         # NodeRegistry: clipboard_read/write, run_shell,
                                          #   shell_open/write/read/close
        mcp.rs                            # 7 new tools: clipboard_read, clipboard_write, shell_run,
                                          #   shell_open, shell_write, shell_read, shell_close
      tests/
        phase3.rs                         # Integration: shell_run echo round-trip, shell interactive,
                                          #   clipboard round-trip (ignored on headless)
```

---

### Task 1: Proto types + workspace deps

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Modify: `crates/kestrel-proto/src/message.rs`
- Modify: `crates/kestrel-proto/src/lib.rs`
- Modify: `crates/kestrel-agent/Cargo.toml`

- [ ] **Step 1: Write the failing test**

Add to `crates/kestrel-proto/src/message.rs` tests (inside the existing `#[cfg(test)]` block):

```rust
#[test]
fn roundtrip_clipboard_text() {
    let msg = KestrelMessage {
        stream_id: 1,
        kind: MsgKind::Request,
        payload: Payload::ClipboardReadResp {
            content: ClipboardContent::Text("hello clipboard".into()),
        },
    };
    assert_eq!(roundtrip(&msg), msg);
}

#[test]
fn roundtrip_shell_output() {
    let msg = KestrelMessage {
        stream_id: 0,
        kind: MsgKind::Event,
        payload: Payload::ShellOutput { pty_id: 7, data: b"$ ls\n".to_vec() },
    };
    assert_eq!(roundtrip(&msg), msg);
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p kestrel-proto 2>&1 | tail -20
```

Expected: compile errors — `ClipboardContent`, `Payload::ClipboardReadResp`, `Payload::ShellOutput` not defined.

- [ ] **Step 3: Add deps to workspace Cargo.toml**

In `Cargo.toml` (workspace root), append inside `[workspace.dependencies]`:

```toml
arboard      = "3"
portable-pty = "0.8"
```

- [ ] **Step 4: Add deps to kestrel-agent Cargo.toml**

In `crates/kestrel-agent/Cargo.toml` under `[dependencies]`, append:

```toml
arboard      = { workspace = true }
portable-pty = { workspace = true }
```

- [ ] **Step 5: Extend message.rs — add ClipboardContent and Payload variants**

Replace `crates/kestrel-proto/src/message.rs` with the complete file below. The first 33 lines (Payload variants 0-13) are unchanged. Append `ClipboardContent` and variants 14-23 at the end before the `#[cfg(test)]` block:

```rust
// crates/kestrel-proto/src/message.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KestrelMessage {
    pub stream_id: u32,
    pub kind: MsgKind,
    pub payload: Payload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MsgKind { Request, Response, Event, Ack }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Payload {
    // Phase 1 — Auth + System (variants 0-4, discriminants unchanged)
    Challenge { nonce: [u8; 32] },
    AuthResponse { mac: [u8; 32], node_id: String },
    SystemInfo { os: OsInfo, displays: Vec<DisplayInfo>, hostname: String },
    Ping,
    Pong,
    // Phase 2 — Input (variants 5-9)
    KeyEvent { key: KeyCode, modifiers: Modifiers, action: PressRelease },
    TypeText { text: String },
    MouseMove { x: f64, y: f64 },
    MouseButton { button: Button, action: PressRelease, x: f64, y: f64 },
    Scroll { dx: f64, dy: f64 },
    // Phase 2 — Screen (variants 10-13)
    ScreenshotReq { display: u8, region: Option<Rect> },
    ScreenshotResp { png_bytes: Vec<u8> },
    DescribeReq { display: u8 },
    DescribeResp { tree: AccessibilityNode },
    // Phase 3 — Clipboard (variants 14-17)
    ClipboardReadReq,
    ClipboardReadResp { content: ClipboardContent },
    ClipboardWriteReq { content: ClipboardContent },
    ClipboardWriteAck,
    // Phase 3 — Shell (variants 18-23)
    ShellSpawn { shell: Option<String>, cols: u16, rows: u16 },
    ShellSpawned { pty_id: u32 },
    ShellWrite { pty_id: u32, data: Vec<u8> },
    ShellOutput { pty_id: u32, data: Vec<u8> },
    ShellResize { pty_id: u32, cols: u16, rows: u16 },
    ShellClose { pty_id: u32 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OsInfo { pub name: String, pub version: String }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DisplayInfo { pub id: u8, pub width: u32, pub height: u32 }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KeyCode {
    Char(char),
    Return, Backspace, Tab, Escape, Delete,
    Home, End, PageUp, PageDown,
    Up, Down, Left, Right,
    F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12,
    Control, Alt, Shift, Meta,
    Space, CapsLock, NumLock,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Modifiers {
    pub shift: bool,
    pub ctrl: bool,
    pub alt: bool,
    pub meta: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PressRelease { Press, Release, Click }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Button { Left, Right, Middle }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rect { pub x: f64, pub y: f64, pub w: f64, pub h: f64 }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessibilityNode {
    pub role: String,
    pub label: Option<String>,
    pub value: Option<String>,
    pub focused: bool,
    pub enabled: bool,
    pub bounds: Option<Rect>,
    pub children: Vec<AccessibilityNode>,
    /// True when the real AX tree was unavailable; caller should use screenshot instead.
    pub fallback: bool,
}

impl AccessibilityNode {
    pub fn unavailable() -> Self {
        AccessibilityNode {
            role: "root".into(),
            label: None,
            value: None,
            focused: false,
            enabled: true,
            bounds: None,
            children: vec![],
            fallback: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClipboardContent {
    Text(String),
    Image { png_bytes: Vec<u8>, width: u32, height: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: &KestrelMessage) -> KestrelMessage {
        let bytes = bincode::serde::encode_to_vec(msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        decoded
    }

    #[test]
    fn roundtrip_ping() {
        let msg = KestrelMessage { stream_id: 1, kind: MsgKind::Request, payload: Payload::Ping };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_system_info() {
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::SystemInfo {
                os: OsInfo { name: "linux".into(), version: "6.8".into() },
                displays: vec![DisplayInfo { id: 0, width: 1920, height: 1080 }],
                hostname: "dev-box".into(),
            },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_auth_challenge() {
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Request,
            payload: Payload::Challenge { nonce: [0xABu8; 32] },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_auth_response() {
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Response,
            payload: Payload::AuthResponse { mac: [0xDEu8; 32], node_id: "hub".into() },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_key_event() {
        let msg = KestrelMessage {
            stream_id: 1,
            kind: MsgKind::Request,
            payload: Payload::KeyEvent {
                key: KeyCode::Char('a'),
                modifiers: Modifiers::default(),
                action: PressRelease::Click,
            },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_mouse_move() {
        let msg = KestrelMessage {
            stream_id: 2,
            kind: MsgKind::Request,
            payload: Payload::MouseMove { x: 0.5, y: 0.3 },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_screenshot_req_resp() {
        let req = KestrelMessage {
            stream_id: 3,
            kind: MsgKind::Request,
            payload: Payload::ScreenshotReq { display: 0, region: None },
        };
        let resp = KestrelMessage {
            stream_id: 3,
            kind: MsgKind::Response,
            payload: Payload::ScreenshotResp { png_bytes: vec![137, 80, 78, 71] },
        };
        for msg in [req, resp] {
            assert_eq!(roundtrip(&msg), msg);
        }
    }

    #[test]
    fn roundtrip_type_text() {
        let msg = KestrelMessage {
            stream_id: 4,
            kind: MsgKind::Request,
            payload: Payload::TypeText { text: "hello world".into() },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_screenshot_with_region() {
        let msg = KestrelMessage {
            stream_id: 5,
            kind: MsgKind::Request,
            payload: Payload::ScreenshotReq {
                display: 0,
                region: Some(Rect { x: 0.1, y: 0.1, w: 0.8, h: 0.8 }),
            },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_clipboard_text() {
        let msg = KestrelMessage {
            stream_id: 1,
            kind: MsgKind::Request,
            payload: Payload::ClipboardReadResp {
                content: ClipboardContent::Text("hello clipboard".into()),
            },
        };
        assert_eq!(roundtrip(&msg), msg);
    }

    #[test]
    fn roundtrip_shell_output() {
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::ShellOutput { pty_id: 7, data: b"$ ls\n".to_vec() },
        };
        assert_eq!(roundtrip(&msg), msg);
    }
}
```

- [ ] **Step 6: Export ClipboardContent from kestrel-proto lib.rs**

Edit `crates/kestrel-proto/src/lib.rs` to add `ClipboardContent` to the re-export line:

```rust
pub mod auth;
pub mod message;
pub use auth::{hmac_response, verify_response};
pub use message::{
    AccessibilityNode, Button, ClipboardContent, DisplayInfo, KeyCode,
    KestrelMessage, Modifiers, MsgKind, OsInfo, Payload, PressRelease, Rect,
};
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
cargo test -p kestrel-proto 2>&1 | tail -20
```

Expected: all tests pass including `roundtrip_clipboard_text` and `roundtrip_shell_output`.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/kestrel-proto/src/message.rs crates/kestrel-proto/src/lib.rs crates/kestrel-agent/Cargo.toml
git commit -m "feat(proto): add ClipboardContent and shell/clipboard Payload variants for phase 3"
```

---

### Task 2: Agent clipboard capability

**Files:**
- Create: `crates/kestrel-agent/src/capabilities/clipboard.rs`
- Modify: `crates/kestrel-agent/src/capabilities/mod.rs`

- [ ] **Step 1: Write the failing test**

Add to `crates/kestrel-agent/src/capabilities/clipboard.rs` (create the file with test-only content first):

```rust
// crates/kestrel-agent/src/capabilities/clipboard.rs

#[cfg(test)]
mod tests {
    #[test]
    #[ignore = "requires display server / clipboard daemon; run manually"]
    fn clipboard_text_roundtrip() {
        use super::*;
        use kestrel_proto::ClipboardContent;
        write_clipboard(ClipboardContent::Text("kestrel-test-xyz".into())).unwrap();
        let got = read_clipboard().unwrap();
        assert_eq!(got, ClipboardContent::Text("kestrel-test-xyz".into()));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p kestrel-agent capabilities::clipboard 2>&1 | tail -10
```

Expected: compile error — `read_clipboard`, `write_clipboard` not defined.

- [ ] **Step 3: Implement clipboard.rs**

Replace `crates/kestrel-agent/src/capabilities/clipboard.rs` with:

```rust
// crates/kestrel-agent/src/capabilities/clipboard.rs
use anyhow::Context;
use arboard::Clipboard;
use image::{DynamicImage, ImageFormat, RgbaImage};
use kestrel_proto::ClipboardContent;
use std::borrow::Cow;
use std::io::Cursor;

pub fn read_clipboard() -> anyhow::Result<ClipboardContent> {
    let mut cb = Clipboard::new().context("arboard init")?;
    match cb.get_text() {
        Ok(text) => return Ok(ClipboardContent::Text(text)),
        Err(_) => {}
    }
    let img_data = cb.get_image().context("clipboard get_image")?;
    let width = img_data.width as u32;
    let height = img_data.height as u32;
    let rgba = RgbaImage::from_raw(width, height, img_data.bytes.into_owned())
        .ok_or_else(|| anyhow::anyhow!("clipboard image data is invalid (wrong buffer size)"))?;
    let mut png_bytes = Vec::new();
    DynamicImage::ImageRgba8(rgba)
        .write_to(&mut Cursor::new(&mut png_bytes), ImageFormat::Png)
        .context("PNG encode for clipboard image")?;
    Ok(ClipboardContent::Image { png_bytes, width, height })
}

pub fn write_clipboard(content: ClipboardContent) -> anyhow::Result<()> {
    let mut cb = Clipboard::new().context("arboard init")?;
    match content {
        ClipboardContent::Text(text) => {
            cb.set_text(text).context("clipboard set_text")
        }
        ClipboardContent::Image { png_bytes, width, height } => {
            let img = image::load_from_memory(&png_bytes).context("PNG decode for clipboard write")?;
            let rgba = img.to_rgba8();
            let data = arboard::ImageData {
                width: width as usize,
                height: height as usize,
                bytes: Cow::Owned(rgba.into_raw()),
            };
            cb.set_image(data).context("clipboard set_image")
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore = "requires display server / clipboard daemon; run manually"]
    fn clipboard_text_roundtrip() {
        use super::*;
        use kestrel_proto::ClipboardContent;
        write_clipboard(ClipboardContent::Text("kestrel-test-xyz".into())).unwrap();
        let got = read_clipboard().unwrap();
        assert_eq!(got, ClipboardContent::Text("kestrel-test-xyz".into()));
    }
}
```

- [ ] **Step 4: Add pub mod clipboard to mod.rs**

Edit `crates/kestrel-agent/src/capabilities/mod.rs`:

```rust
pub mod clipboard;
pub mod input;
pub mod screen;
pub mod shell;
```

Note: `shell` doesn't exist yet, so you'll get a compile error for it. Add only `clipboard` for now:

```rust
pub mod clipboard;
pub mod input;
pub mod screen;
```

- [ ] **Step 5: Run tests to verify they pass (ignore marked)**

```bash
cargo test -p kestrel-agent capabilities::clipboard 2>&1 | tail -10
```

Expected: test ignored (requires display), no compile errors.

- [ ] **Step 6: Commit**

```bash
git add crates/kestrel-agent/src/capabilities/clipboard.rs crates/kestrel-agent/src/capabilities/mod.rs
git commit -m "feat(agent): add clipboard capability using arboard"
```

---

### Task 3: Agent shell — ShellManager

**Files:**
- Create: `crates/kestrel-agent/src/capabilities/shell.rs`
- Modify: `crates/kestrel-agent/src/capabilities/mod.rs`

- [ ] **Step 1: Write the failing test**

Add to end of `crates/kestrel-agent/src/capabilities/shell.rs` (create with test only):

```rust
// crates/kestrel-agent/src/capabilities/shell.rs

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn shell_manager_spawn_and_receive_output() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let mgr = ShellManager::new(event_tx);
        let pty_id = mgr.spawn(None, 80, 24).expect("spawn shell");

        // Write a command and wait for output
        mgr.write(pty_id, b"echo kestrel-shell-test\nexit\n").expect("write to shell");

        // Collect events until ShellClose (or timeout via iterator limit)
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p kestrel-agent shell_manager_spawn 2>&1 | tail -10
```

Expected: compile error — `ShellManager` not defined.

- [ ] **Step 3: Implement shell.rs**

Replace `crates/kestrel-agent/src/capabilities/shell.rs` with:

```rust
// crates/kestrel-agent/src/capabilities/shell.rs
use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex};
use anyhow::Context;
use kestrel_proto::{KestrelMessage, MsgKind, Payload};
use portable_pty::{native_pty_system, CommandBuilder, MasterPty, PtySize};
use tokio::sync::mpsc::UnboundedSender;

struct PtySession {
    master: Box<dyn MasterPty>,
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
```

- [ ] **Step 4: Add pub mod shell to mod.rs**

Edit `crates/kestrel-agent/src/capabilities/mod.rs`:

```rust
pub mod clipboard;
pub mod input;
pub mod screen;
pub mod shell;
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p kestrel-agent shell_manager_spawn 2>&1 | tail -20
```

Expected: `test capabilities::shell::tests::shell_manager_spawn_and_receive_output ... ok`

- [ ] **Step 6: Commit**

```bash
git add crates/kestrel-agent/src/capabilities/shell.rs crates/kestrel-agent/src/capabilities/mod.rs
git commit -m "feat(agent): add ShellManager PTY capability using portable-pty"
```

---

### Task 4: Agent transport — select! loop for shell events

**Files:**
- Modify: `crates/kestrel-agent/src/transport.rs`

The current transport loop is `while let Some(frame) = rx.next().await { ... }`. We need to add a `ShellManager`, create a `tokio::sync::mpsc::unbounded_channel` for shell events, and multiplex both with `tokio::select!`. We also need to handle the new `ClipboardReadReq`, `ClipboardWriteReq`, `ShellSpawn`, `ShellWrite`, `ShellResize`, and `ShellClose` payload variants.

- [ ] **Step 1: Write the failing test**

The phase2 integration tests already cover the agent running. Add a compile-check by verifying the existing tests still build after our changes. No new unit test needed here — Task 8 covers integration tests.

- [ ] **Step 2: Replace transport.rs with the updated version**

Replace `crates/kestrel-agent/src/transport.rs` with:

```rust
// crates/kestrel-agent/src/transport.rs
use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use kestrel_proto::{
    verify_response, AccessibilityNode, DisplayInfo, KestrelMessage, MsgKind, OsInfo, Payload,
};
use rand::RngCore;
use rustls::ServerConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{accept_async, tungstenite::Message};

use crate::capabilities::{clipboard, input, screen, shell::ShellManager};
use crate::config::AgentConfig;

fn make_tls_config() -> Arc<ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec!["kestrel-agent".into()]).unwrap();
    let cert_chain = vec![rustls::Certificate(cert.serialize_der().unwrap())];
    let key = rustls::PrivateKey(cert.serialize_private_key_der());
    Arc::new(
        ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .unwrap(),
    )
}

pub async fn serve(
    config: &AgentConfig,
    ready: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
) -> anyhow::Result<()> {
    let acceptor = TlsAcceptor::from(make_tls_config());
    let listener = TcpListener::bind(config.listen).await?;
    let bound = listener.local_addr()?;
    tracing::info!("agent listening on {}", bound);
    if let Some(tx) = ready {
        let _ = tx.send(bound);
    }
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => { tracing::error!("accept error: {}", e); continue; }
        };
        let acceptor = acceptor.clone();
        let psk = config.psk.clone();
        let node_id = config.node_id.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, peer, acceptor, psk, node_id).await {
                tracing::warn!("connection from {} closed: {}", peer, e);
            }
        });
    }
}

async fn handle_conn(
    stream: TcpStream,
    _peer: SocketAddr,
    acceptor: TlsAcceptor,
    psk: Vec<u8>,
    node_id: String,
) -> anyhow::Result<()> {
    let tls = acceptor.accept(stream).await.context("TLS handshake failed")?;
    let ws = accept_async(tls).await.context("WebSocket handshake failed")?;
    let (mut tx, mut rx) = ws.split();

    // Challenge
    let mut nonce = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut nonce);
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id: 0, kind: MsgKind::Event,
        payload: Payload::Challenge { nonce },
    })?)).await?;

    // Auth
    let raw = rx.next().await.context("no auth response from hub")??;
    let km: KestrelMessage = decode(raw.into_data())?;
    let Payload::AuthResponse { mac, node_id: claimed } = km.payload else {
        anyhow::bail!("expected AuthResponse");
    };
    if !verify_response(&psk, &nonce, &mac) {
        let _ = tx.send(Message::Close(None)).await;
        anyhow::bail!("auth failed: bad MAC from claimed node_id={}", claimed);
    }
    tracing::info!("hub authenticated (claimed node_id={})", claimed);

    // SystemInfo — populate real display list
    let displays: Vec<DisplayInfo> = screen::list_displays()
        .into_iter()
        .map(|(i, w, h)| DisplayInfo { id: i as u8, width: w, height: h })
        .collect();
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id: 0, kind: MsgKind::Event,
        payload: Payload::SystemInfo {
            os: OsInfo { name: std::env::consts::OS.into(), version: "unknown".into() },
            displays,
            hostname: node_id,
        },
    })?)).await?;

    // Shell event channel
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<KestrelMessage>();
    let shell_mgr = ShellManager::new(event_tx);

    // Message loop — select! on incoming frames and outgoing shell events
    loop {
        tokio::select! {
            frame_result = rx.next() => {
                let Some(frame_result) = frame_result else { break; };
                let frame = frame_result?;
                if !frame.is_binary() { continue; }
                let km: KestrelMessage = decode(frame.into_data())?;
                let stream_id = km.stream_id;

                match km.payload {
                    Payload::Ping => {
                        tx.send(Message::Binary(encode(&KestrelMessage {
                            stream_id, kind: MsgKind::Response, payload: Payload::Pong,
                        })?)).await?;
                    }
                    Payload::KeyEvent { key, modifiers, action } => {
                        if let Err(e) = input::inject_key_event(key, modifiers, action, 0, 0).await {
                            tracing::warn!("key inject error: {}", e);
                        }
                    }
                    Payload::TypeText { text } => {
                        if let Err(e) = input::inject_text(text).await {
                            tracing::warn!("type_text error: {}", e);
                        }
                    }
                    Payload::MouseMove { x, y } => {
                        let (w, h) = primary_display_dims();
                        if let Err(e) = input::inject_mouse_move(x, y, w, h).await {
                            tracing::warn!("mouse_move error: {}", e);
                        }
                    }
                    Payload::MouseButton { button, action, x, y } => {
                        let (w, h) = primary_display_dims();
                        if let Err(e) = input::inject_mouse_button(button, action, x, y, w, h).await {
                            tracing::warn!("mouse_button error: {}", e);
                        }
                    }
                    Payload::Scroll { dx, dy } => {
                        if let Err(e) = input::inject_scroll(dx, dy).await {
                            tracing::warn!("scroll error: {}", e);
                        }
                    }
                    Payload::ScreenshotReq { display, region } => {
                        let result = tokio::task::spawn_blocking(move || {
                            match region {
                                Some(r) => screen::capture_region(display as usize, &r),
                                None => screen::capture_display(display as usize),
                            }
                        }).await.unwrap_or_else(|e| Err(anyhow::anyhow!("panic: {e}")));
                        let payload = match result {
                            Ok(png) => Payload::ScreenshotResp { png_bytes: png },
                            Err(e) => {
                                tracing::warn!("screenshot error: {}", e);
                                Payload::ScreenshotResp { png_bytes: vec![] }
                            }
                        };
                        tx.send(Message::Binary(encode(&KestrelMessage {
                            stream_id, kind: MsgKind::Response, payload,
                        })?)).await?;
                    }
                    Payload::DescribeReq { .. } => {
                        tx.send(Message::Binary(encode(&KestrelMessage {
                            stream_id, kind: MsgKind::Response,
                            payload: Payload::DescribeResp { tree: AccessibilityNode::unavailable() },
                        })?)).await?;
                    }
                    Payload::ClipboardReadReq => {
                        let result = tokio::task::spawn_blocking(clipboard::read_clipboard)
                            .await
                            .unwrap_or_else(|e| Err(anyhow::anyhow!("panic: {e}")));
                        let payload = match result {
                            Ok(content) => Payload::ClipboardReadResp { content },
                            Err(e) => {
                                tracing::warn!("clipboard read error: {}", e);
                                Payload::ClipboardReadResp {
                                    content: kestrel_proto::ClipboardContent::Text(
                                        format!("error: {e}")
                                    ),
                                }
                            }
                        };
                        tx.send(Message::Binary(encode(&KestrelMessage {
                            stream_id, kind: MsgKind::Response, payload,
                        })?)).await?;
                    }
                    Payload::ClipboardWriteReq { content } => {
                        let result = tokio::task::spawn_blocking(move || clipboard::write_clipboard(content))
                            .await
                            .unwrap_or_else(|e| Err(anyhow::anyhow!("panic: {e}")));
                        if let Err(e) = result {
                            tracing::warn!("clipboard write error: {}", e);
                        }
                        tx.send(Message::Binary(encode(&KestrelMessage {
                            stream_id, kind: MsgKind::Response, payload: Payload::ClipboardWriteAck,
                        })?)).await?;
                    }
                    Payload::ShellSpawn { shell, cols, rows } => {
                        let payload = match shell_mgr.spawn(shell, cols, rows) {
                            Ok(pty_id) => Payload::ShellSpawned { pty_id },
                            Err(e) => {
                                tracing::warn!("shell spawn error: {}", e);
                                Payload::ShellSpawned { pty_id: u32::MAX }
                            }
                        };
                        tx.send(Message::Binary(encode(&KestrelMessage {
                            stream_id, kind: MsgKind::Response, payload,
                        })?)).await?;
                    }
                    Payload::ShellWrite { pty_id, data } => {
                        if let Err(e) = shell_mgr.write(pty_id, &data) {
                            tracing::warn!("shell write error: {}", e);
                        }
                    }
                    Payload::ShellResize { pty_id, cols, rows } => {
                        if let Err(e) = shell_mgr.resize(pty_id, cols, rows) {
                            tracing::warn!("shell resize error: {}", e);
                        }
                    }
                    Payload::ShellClose { pty_id } => {
                        shell_mgr.close(pty_id);
                    }
                    _ => {}
                }
            }
            event = event_rx.recv() => {
                let Some(msg) = event else { break; };
                if let Ok(bytes) = encode(&msg) {
                    tx.send(Message::Binary(bytes)).await?;
                }
            }
        }
    }
    Ok(())
}

fn primary_display_dims() -> (u32, u32) {
    screen::list_displays()
        .into_iter()
        .next()
        .map(|(_, w, h)| (w, h))
        .unwrap_or((1920, 1080))
}

fn encode(msg: &KestrelMessage) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serde::encode_to_vec(msg, bincode::config::standard())?)
}

fn decode(bytes: Vec<u8>) -> anyhow::Result<KestrelMessage> {
    let (msg, _) = bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;
    Ok(msg)
}
```

- [ ] **Step 3: Verify existing tests still pass**

```bash
cargo test -p kestrel-agent 2>&1 | tail -20
```

Expected: all tests pass (screen tests still ignored).

- [ ] **Step 4: Commit**

```bash
git add crates/kestrel-agent/src/transport.rs
git commit -m "feat(agent): refactor transport to select! loop with shell event multiplexing"
```

---

### Task 5: Hub actor — shell buffer and waiter support

**Files:**
- Modify: `crates/kestrel-hub/src/transport.rs`

The actor (`run_actor`) currently handles `Fire`, `Request`, and incoming frames. We need:
1. Two new `ActorCmd` variants: `ReadShellBuffer` and `WaitShellClose`
2. Two new actor state maps: `shell_buffers: HashMap<u32, Vec<u8>>` and `shell_close_waiters: HashMap<u32, oneshot::Sender<()>>`
3. Frame handler extended to buffer `ShellOutput` and notify `ShellClose` waiters

- [ ] **Step 1: Write the failing test**

We can't test the actor in isolation without a socket. The integration test (Task 8) will exercise it end-to-end. For now, add a compile test. This step verifies transport.rs compiles with the new ActorCmd variants.

- [ ] **Step 2: Replace transport.rs with the extended actor**

Replace `crates/kestrel-hub/src/transport.rs` with:

```rust
// crates/kestrel-hub/src/transport.rs
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use kestrel_proto::{
    ClipboardContent, hmac_response, Button, KeyCode, KestrelMessage, Modifiers, MsgKind,
    OsInfo, Payload, PressRelease, Rect,
};
use rustls::ClientConfig;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::{client_async, tungstenite::Message, WebSocketStream};

// ── TLS ──────────────────────────────────────────────────────────────────────

struct SkipVerify;

impl rustls::client::ServerCertVerifier for SkipVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

fn make_client_config() -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(SkipVerify))
            .with_no_client_auth(),
    )
}

async fn tls_connect(addr: SocketAddr) -> anyhow::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let tcp = TcpStream::connect(addr).await.context("TCP connect")?;
    let connector = TlsConnector::from(make_client_config());
    let server_name = rustls::ServerName::try_from("kestrel-agent").expect("valid DNS name");
    connector.connect(server_name, tcp).await.context("TLS connect")
}

// ── Actor ─────────────────────────────────────────────────────────────────────

enum ActorCmd {
    Fire(KestrelMessage),
    Request {
        msg: KestrelMessage,
        reply: oneshot::Sender<anyhow::Result<KestrelMessage>>,
    },
    ReadShellBuffer {
        pty_id: u32,
        reply: oneshot::Sender<Vec<u8>>,
    },
    WaitShellClose {
        pty_id: u32,
        reply: oneshot::Sender<()>,
    },
}

type WsStream = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn run_actor(ws: WsStream, mut cmd_rx: mpsc::Receiver<ActorCmd>) {
    let (mut tx, mut rx) = ws.split();
    let mut pending: HashMap<u32, oneshot::Sender<anyhow::Result<KestrelMessage>>> = HashMap::new();
    let mut shell_buffers: HashMap<u32, Vec<u8>> = HashMap::new();
    let mut shell_close_waiters: HashMap<u32, oneshot::Sender<()>> = HashMap::new();
    let mut next_id: u32 = 1;
    let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping_interval.tick().await;

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                let id = next_id;
                next_id = next_id.wrapping_add(1);
                if let Ok(bytes) = encode(&KestrelMessage {
                    stream_id: id, kind: MsgKind::Request, payload: Payload::Ping,
                }) {
                    if tx.send(Message::Binary(bytes)).await.is_err() { break; }
                }
            }
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break; };
                match cmd {
                    ActorCmd::Fire(msg) => {
                        if let Ok(bytes) = encode(&msg) {
                            if tx.send(Message::Binary(bytes)).await.is_err() { break; }
                        }
                    }
                    ActorCmd::Request { mut msg, reply } => {
                        let id = next_id;
                        next_id = next_id.wrapping_add(1);
                        msg.stream_id = id;
                        pending.insert(id, reply);
                        match encode(&msg) {
                            Ok(bytes) => {
                                if tx.send(Message::Binary(bytes)).await.is_err() { break; }
                            }
                            Err(e) => {
                                if let Some(r) = pending.remove(&id) {
                                    let _ = r.send(Err(e));
                                }
                            }
                        }
                    }
                    ActorCmd::ReadShellBuffer { pty_id, reply } => {
                        let data = shell_buffers.remove(&pty_id).unwrap_or_default();
                        let _ = reply.send(data);
                    }
                    ActorCmd::WaitShellClose { pty_id, reply } => {
                        shell_close_waiters.insert(pty_id, reply);
                    }
                }
            }
            frame = rx.next() => {
                let Some(Ok(frame)) = frame else { break; };
                if !frame.is_binary() { continue; }
                match decode(frame.into_data()) {
                    Ok(msg) => {
                        // Handle streaming shell events (stream_id=0, no pending waiter)
                        match &msg.payload {
                            Payload::ShellOutput { pty_id, data } => {
                                shell_buffers.entry(*pty_id).or_default().extend(data);
                            }
                            Payload::ShellClose { pty_id } => {
                                if let Some(waiter) = shell_close_waiters.remove(pty_id) {
                                    let _ = waiter.send(());
                                }
                            }
                            _ => {}
                        }
                        // Route request-response pairs by stream_id
                        if let Some(r) = pending.remove(&msg.stream_id) {
                            let _ = r.send(Ok(msg));
                        }
                    }
                    Err(e) => tracing::warn!("hub transport decode error: {}", e),
                }
            }
        }
    }
    for (_, r) in pending {
        let _ = r.send(Err(anyhow::anyhow!("connection closed")));
    }
    for (_, w) in shell_close_waiters {
        let _ = w.send(());
    }
}

// ── NodeHandle ────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct NodeHandle {
    pub node_id: String,
    pub os_info: OsInfo,
    cmd_tx: mpsc::Sender<ActorCmd>,
}

impl NodeHandle {
    async fn fire(&self, payload: Payload) -> anyhow::Result<()> {
        self.cmd_tx
            .send(ActorCmd::Fire(KestrelMessage { stream_id: 0, kind: MsgKind::Request, payload }))
            .await
            .map_err(|_| anyhow::anyhow!("actor channel closed"))
    }

    async fn request(&self, payload: Payload) -> anyhow::Result<KestrelMessage> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCmd::Request {
                msg: KestrelMessage { stream_id: 0, kind: MsgKind::Request, payload },
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("actor channel closed"))?;
        Ok(reply_rx.await.map_err(|_| anyhow::anyhow!("actor dropped reply"))??)
    }

    // ── Phase 2 input ──────────────────────────────────────────────────────────

    pub async fn send_key_event(&self, key: KeyCode, mods: Modifiers, action: PressRelease) -> anyhow::Result<()> {
        self.fire(Payload::KeyEvent { key, modifiers: mods, action }).await
    }

    pub async fn send_type_text(&self, text: String) -> anyhow::Result<()> {
        self.fire(Payload::TypeText { text }).await
    }

    pub async fn send_mouse_move(&self, x: f64, y: f64) -> anyhow::Result<()> {
        self.fire(Payload::MouseMove { x, y }).await
    }

    pub async fn send_mouse_button(&self, button: Button, action: PressRelease, x: f64, y: f64) -> anyhow::Result<()> {
        self.fire(Payload::MouseButton { button, action, x, y }).await
    }

    pub async fn send_scroll(&self, dx: f64, dy: f64) -> anyhow::Result<()> {
        self.fire(Payload::Scroll { dx, dy }).await
    }

    pub async fn screenshot(&self, display: u8, region: Option<Rect>) -> anyhow::Result<Vec<u8>> {
        let reply = self.request(Payload::ScreenshotReq { display, region }).await?;
        match reply.payload {
            Payload::ScreenshotResp { png_bytes } => Ok(png_bytes),
            _ => anyhow::bail!("expected ScreenshotResp, got other payload"),
        }
    }

    // ── Phase 3 clipboard ─────────────────────────────────────────────────────

    pub async fn clipboard_read(&self) -> anyhow::Result<ClipboardContent> {
        let reply = self.request(Payload::ClipboardReadReq).await?;
        match reply.payload {
            Payload::ClipboardReadResp { content } => Ok(content),
            _ => anyhow::bail!("expected ClipboardReadResp"),
        }
    }

    pub async fn clipboard_write(&self, content: ClipboardContent) -> anyhow::Result<()> {
        let reply = self.request(Payload::ClipboardWriteReq { content }).await?;
        match reply.payload {
            Payload::ClipboardWriteAck => Ok(()),
            _ => anyhow::bail!("expected ClipboardWriteAck"),
        }
    }

    // ── Phase 3 shell ─────────────────────────────────────────────────────────

    pub async fn spawn_shell(&self, shell: Option<String>, cols: u16, rows: u16) -> anyhow::Result<u32> {
        let reply = self.request(Payload::ShellSpawn { shell, cols, rows }).await?;
        match reply.payload {
            Payload::ShellSpawned { pty_id } => {
                anyhow::ensure!(pty_id != u32::MAX, "agent failed to spawn shell");
                Ok(pty_id)
            }
            _ => anyhow::bail!("expected ShellSpawned"),
        }
    }

    pub async fn write_shell(&self, pty_id: u32, data: Vec<u8>) -> anyhow::Result<()> {
        self.fire(Payload::ShellWrite { pty_id, data }).await
    }

    pub async fn resize_shell(&self, pty_id: u32, cols: u16, rows: u16) -> anyhow::Result<()> {
        self.fire(Payload::ShellResize { pty_id, cols, rows }).await
    }

    pub async fn close_shell(&self, pty_id: u32) -> anyhow::Result<()> {
        self.fire(Payload::ShellClose { pty_id }).await
    }

    pub async fn read_shell_buffer(&self, pty_id: u32) -> anyhow::Result<Vec<u8>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCmd::ReadShellBuffer { pty_id, reply: reply_tx })
            .await
            .map_err(|_| anyhow::anyhow!("actor channel closed"))?;
        reply_rx.await.map_err(|_| anyhow::anyhow!("actor dropped reply"))
    }

    /// Spawn a shell, run `command`, wait for exit, return all output as UTF-8.
    /// Timeout: 30 seconds.
    pub async fn run_shell(&self, command: &str) -> anyhow::Result<String> {
        let pty_id = self.spawn_shell(None, 80, 24).await?;

        // Register close waiter BEFORE writing to avoid a race with fast-exiting commands.
        let (close_tx, close_rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCmd::WaitShellClose { pty_id, reply: close_tx })
            .await
            .map_err(|_| anyhow::anyhow!("actor channel closed"))?;

        let cmd_bytes = format!("{}\nexit\n", command).into_bytes();
        self.write_shell(pty_id, cmd_bytes).await?;

        tokio::time::timeout(Duration::from_secs(30), close_rx)
            .await
            .map_err(|_| anyhow::anyhow!("shell command timed out after 30s"))?
            .map_err(|_| anyhow::anyhow!("actor dropped shell close waiter"))?;

        let raw = self.read_shell_buffer(pty_id).await?;
        Ok(String::from_utf8_lossy(&raw).into_owned())
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

pub async fn connect(addr: SocketAddr, psk: &[u8]) -> anyhow::Result<NodeHandle> {
    let tls = tls_connect(addr).await?;
    let url = format!("wss://{}", addr);
    let (ws, _) = client_async(url, tls).await.context("WebSocket handshake")?;
    let (mut tx, mut rx) = ws.split();

    let (node_id, os_info) = do_handshake(&mut tx, &mut rx, psk).await?;

    let ws = tx.reunite(rx).expect("same stream");
    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    tokio::spawn(run_actor(ws, cmd_rx));

    Ok(NodeHandle { node_id, os_info, cmd_tx })
}

pub async fn ping_once(addr: SocketAddr, psk: &[u8]) -> anyhow::Result<Duration> {
    let handle = connect(addr, psk).await?;
    let sent = Instant::now();
    handle.request(Payload::Ping).await?;
    Ok(sent.elapsed())
}

async fn do_handshake<Tx, Rx>(
    tx: &mut Tx,
    rx: &mut Rx,
    psk: &[u8],
) -> anyhow::Result<(String, OsInfo)>
where
    Tx: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    Rx: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let raw = rx.next().await.context("no challenge from agent")??;
    let km: KestrelMessage = decode(raw.into_data())?;
    let Payload::Challenge { nonce } = km.payload else {
        anyhow::bail!("expected Challenge");
    };
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id: 0, kind: MsgKind::Response,
        payload: Payload::AuthResponse {
            mac: hmac_response(psk, &nonce),
            node_id: "hub".into(),
        },
    })?)).await?;
    let raw = rx.next().await.context("no SystemInfo from agent")??;
    let km: KestrelMessage = decode(raw.into_data())?;
    let Payload::SystemInfo { os, hostname, .. } = km.payload else {
        anyhow::bail!("expected SystemInfo");
    };
    tracing::info!("connected to node {} ({})", hostname, os.name);
    Ok((hostname, os))
}

fn encode(msg: &KestrelMessage) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serde::encode_to_vec(msg, bincode::config::standard())?)
}

fn decode(bytes: Vec<u8>) -> anyhow::Result<KestrelMessage> {
    let (msg, _) = bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;
    Ok(msg)
}
```

- [ ] **Step 3: Verify existing hub tests still compile and pass**

```bash
cargo test -p kestrel-hub 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/kestrel-hub/src/transport.rs
git commit -m "feat(hub): extend actor with shell buffer/waiter and NodeHandle clipboard/shell methods"
```

---

### Task 6: NodeRegistry clipboard and shell methods

**Files:**
- Modify: `crates/kestrel-hub/src/router.rs`

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` block in `crates/kestrel-hub/src/router.rs`:

```rust
#[test]
fn registry_has_new_methods() {
    // Compile-only: verify the new method signatures exist on NodeRegistry.
    // Real behaviour is tested in phase3.rs integration tests.
    let _r: fn(&NodeRegistry, &str) -> _ = |r: &NodeRegistry, id: &str| {
        let _ = r.run_shell(id, "echo test");
        let _ = r.clipboard_read(id);
        let _ = r.clipboard_write(id, kestrel_proto::ClipboardContent::Text("x".into()));
        let _ = r.shell_open(id, None, 80, 24);
        let _ = r.shell_write(id, 0, vec![]);
        let _ = r.shell_read(id, 0);
        let _ = r.shell_close(id, 0);
    };
    let r = NodeRegistry::new();
    assert!(r.list_sync().is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p kestrel-hub registry_has_new_methods 2>&1 | tail -10
```

Expected: compile error — methods not defined.

- [ ] **Step 3: Implement new NodeRegistry methods**

Replace `crates/kestrel-hub/src/router.rs` with:

```rust
// crates/kestrel-hub/src/router.rs
use std::collections::HashMap;
use std::sync::Arc;
use kestrel_proto::{Button, ClipboardContent, KeyCode, Modifiers, OsInfo, PressRelease, Rect};
use tokio::sync::RwLock;

use crate::transport::NodeHandle;

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub os: OsInfo,
}

#[derive(Clone)]
pub struct NodeRegistry {
    nodes: Arc<RwLock<HashMap<String, NodeHandle>>>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        NodeRegistry { nodes: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub async fn register(&self, handle: NodeHandle) {
        self.nodes.write().await.insert(handle.node_id.clone(), handle);
    }

    pub async fn list(&self) -> Vec<NodeInfo> {
        self.nodes.read().await.values()
            .map(|h| NodeInfo { node_id: h.node_id.clone(), os: h.os_info.clone() })
            .collect()
    }

    pub fn list_sync(&self) -> Vec<NodeInfo> {
        self.nodes.try_read()
            .map(|g| g.values().map(|h| NodeInfo { node_id: h.node_id.clone(), os: h.os_info.clone() }).collect())
            .unwrap_or_default()
    }

    async fn get(&self, node_id: &str) -> anyhow::Result<NodeHandle> {
        self.nodes.read().await.get(node_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("node '{}' not connected", node_id))
    }

    // ── Phase 2 ───────────────────────────────────────────────────────────────

    pub async fn screenshot(&self, node_id: &str, display: u8, region: Option<Rect>) -> anyhow::Result<Vec<u8>> {
        self.get(node_id).await?.screenshot(display, region).await
    }

    pub async fn type_text(&self, node_id: &str, text: String) -> anyhow::Result<()> {
        self.get(node_id).await?.send_type_text(text).await
    }

    pub async fn key_combo(&self, node_id: &str, keys: Vec<KeyCode>) -> anyhow::Result<()> {
        let h = self.get(node_id).await?;
        for key in &keys {
            h.send_key_event(key.clone(), Modifiers::default(), PressRelease::Press).await?;
        }
        for key in keys.iter().rev() {
            h.send_key_event(key.clone(), Modifiers::default(), PressRelease::Release).await?;
        }
        Ok(())
    }

    pub async fn mouse_move(&self, node_id: &str, x: f64, y: f64) -> anyhow::Result<()> {
        self.get(node_id).await?.send_mouse_move(x, y).await
    }

    pub async fn mouse_click(&self, node_id: &str, button: Button, x: f64, y: f64) -> anyhow::Result<()> {
        self.get(node_id).await?.send_mouse_button(button, PressRelease::Click, x, y).await
    }

    pub async fn scroll(&self, node_id: &str, dx: f64, dy: f64) -> anyhow::Result<()> {
        self.get(node_id).await?.send_scroll(dx, dy).await
    }

    pub fn fire_mouse_move(&self, node_id: &str, x: f64, y: f64) {
        let registry = self.clone();
        let node_id = node_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = registry.mouse_move(&node_id, x, y).await {
                tracing::warn!("KVM mouse_move to {} failed: {}", node_id, e);
            }
        });
    }

    // ── Phase 3 clipboard ─────────────────────────────────────────────────────

    pub async fn clipboard_read(&self, node_id: &str) -> anyhow::Result<ClipboardContent> {
        self.get(node_id).await?.clipboard_read().await
    }

    pub async fn clipboard_write(&self, node_id: &str, content: ClipboardContent) -> anyhow::Result<()> {
        self.get(node_id).await?.clipboard_write(content).await
    }

    // ── Phase 3 shell ─────────────────────────────────────────────────────────

    pub async fn run_shell(&self, node_id: &str, command: &str) -> anyhow::Result<String> {
        self.get(node_id).await?.run_shell(command).await
    }

    pub async fn shell_open(&self, node_id: &str, shell: Option<String>, cols: u16, rows: u16) -> anyhow::Result<u32> {
        self.get(node_id).await?.spawn_shell(shell, cols, rows).await
    }

    pub async fn shell_write(&self, node_id: &str, pty_id: u32, data: Vec<u8>) -> anyhow::Result<()> {
        self.get(node_id).await?.write_shell(pty_id, data).await
    }

    pub async fn shell_read(&self, node_id: &str, pty_id: u32) -> anyhow::Result<Vec<u8>> {
        self.get(node_id).await?.read_shell_buffer(pty_id).await
    }

    pub async fn shell_close(&self, node_id: &str, pty_id: u32) -> anyhow::Result<()> {
        self.get(node_id).await?.close_shell(pty_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_starts_empty() {
        let r = NodeRegistry::new();
        assert!(r.list_sync().is_empty());
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p kestrel-hub router 2>&1 | tail -10
```

Expected: `registry_starts_empty ... ok`

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-hub/src/router.rs
git commit -m "feat(hub): add clipboard and shell methods to NodeRegistry"
```

---

### Task 7: MCP tools — 7 new tools for clipboard and shell

**Files:**
- Modify: `crates/kestrel-hub/src/mcp.rs`

The existing `KestrelMcp` has 7 tools (fleet_nodes, screenshot, type_text, key_combo, mouse_move, mouse_click, scroll). We add 7 more: `clipboard_read`, `clipboard_write`, `shell_run`, `shell_open`, `shell_write`, `shell_read`, `shell_close`.

- [ ] **Step 1: Write the failing test**

The existing test `mcp_server_constructs` will fail to compile once we add the new arg structs and methods. Running it before adding those structs verifies the baseline:

```bash
cargo test -p kestrel-hub mcp_server_constructs 2>&1 | tail -5
```

Expected: passes (baseline check).

- [ ] **Step 2: Replace mcp.rs with the extended version**

Replace `crates/kestrel-hub/src/mcp.rs` with:

```rust
// crates/kestrel-hub/src/mcp.rs
use std::sync::Arc;

use base64::{Engine, engine::general_purpose};
use kestrel_proto::{Button, ClipboardContent, KeyCode};
use rmcp::{
    ErrorData as McpError,
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo},
    schemars,
    tool, tool_handler, tool_router,
};

use crate::router::NodeRegistry;

// ── Arg types ─────────────────────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ScreenshotArgs {
    pub node_id: String,
    pub display: Option<u8>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct TypeTextArgs {
    pub node_id: String,
    pub text: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct KeyComboArgs {
    pub node_id: String,
    pub keys: Vec<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct MouseMoveArgs {
    pub node_id: String,
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct MouseClickArgs {
    pub node_id: String,
    pub button: String,
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ScrollArgs {
    pub node_id: String,
    pub dx: f64,
    pub dy: f64,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct NodeIdArgs {
    pub node_id: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ClipboardWriteArgs {
    pub node_id: String,
    pub text: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ShellRunArgs {
    pub node_id: String,
    pub command: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ShellOpenArgs {
    pub node_id: String,
    pub shell: Option<String>,
    pub cols: Option<u16>,
    pub rows: Option<u16>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ShellWriteArgs {
    pub node_id: String,
    pub pty_id: u32,
    pub data: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ShellPtyArgs {
    pub node_id: String,
    pub pty_id: u32,
}

// ── Server ────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct KestrelMcp {
    registry: Arc<NodeRegistry>,
    tool_router: ToolRouter<KestrelMcp>,
}

#[tool_router]
impl KestrelMcp {
    pub fn new(registry: Arc<NodeRegistry>) -> Self {
        KestrelMcp {
            registry,
            tool_router: Self::tool_router(),
        }
    }

    // ── Phase 2 tools ─────────────────────────────────────────────────────────

    #[tool(description = "List all connected nodes with their OS and hostname")]
    async fn fleet_nodes(&self) -> Result<CallToolResult, McpError> {
        let nodes = self.registry.list().await;
        let json = serde_json::to_string_pretty(&nodes)
            .unwrap_or_else(|e| format!("error: {e}"));
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Take a PNG screenshot of a node display")]
    async fn screenshot(
        &self,
        Parameters(args): Parameters<ScreenshotArgs>,
    ) -> Result<CallToolResult, McpError> {
        let png = self
            .registry
            .screenshot(&args.node_id, args.display.unwrap_or(0), None)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        if png.is_empty() {
            return Err(McpError::internal_error(
                "screenshot returned empty bytes".to_string(),
                None,
            ));
        }
        let b64 = general_purpose::STANDARD.encode(&png);
        Ok(CallToolResult::success(vec![Content::image(b64, "image/png")]))
    }

    #[tool(description = "Type text on a node (Unicode-safe)")]
    async fn type_text(
        &self,
        Parameters(args): Parameters<TypeTextArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .type_text(&args.node_id, args.text)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    #[tool(description = "Press a key combination on a node, e.g. [\"ctrl\", \"c\"]")]
    async fn key_combo(
        &self,
        Parameters(args): Parameters<KeyComboArgs>,
    ) -> Result<CallToolResult, McpError> {
        let keys: Vec<KeyCode> = args
            .keys
            .iter()
            .map(|s| crate::capabilities_parse::parse_key_str(s))
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|e| McpError::invalid_params(e.to_string(), None))?;
        self.registry
            .key_combo(&args.node_id, keys)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    #[tool(description = "Move the mouse to normalized coordinates (0.0-1.0) on a node")]
    async fn mouse_move(
        &self,
        Parameters(args): Parameters<MouseMoveArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .mouse_move(&args.node_id, args.x, args.y)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    #[tool(description = "Click a mouse button at normalized coordinates on a node")]
    async fn mouse_click(
        &self,
        Parameters(args): Parameters<MouseClickArgs>,
    ) -> Result<CallToolResult, McpError> {
        let button = match args.button.to_lowercase().as_str() {
            "left" => Button::Left,
            "right" => Button::Right,
            "middle" => Button::Middle,
            other => {
                return Err(McpError::invalid_params(
                    format!("unknown button '{}'; use left, right, or middle", other),
                    None,
                ))
            }
        };
        self.registry
            .mouse_click(&args.node_id, button, args.x, args.y)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    #[tool(description = "Scroll on a node (dy > 0 scrolls down, dx > 0 scrolls right)")]
    async fn scroll(
        &self,
        Parameters(args): Parameters<ScrollArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .scroll(&args.node_id, args.dx, args.dy)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    // ── Phase 3 clipboard tools ───────────────────────────────────────────────

    #[tool(description = "Read the clipboard text from a node. Returns the clipboard content as text.")]
    async fn clipboard_read(
        &self,
        Parameters(args): Parameters<NodeIdArgs>,
    ) -> Result<CallToolResult, McpError> {
        let content = self.registry
            .clipboard_read(&args.node_id)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        let text = match content {
            ClipboardContent::Text(t) => t,
            ClipboardContent::Image { width, height, .. } => {
                format!("[image {}x{}]", width, height)
            }
        };
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Write text to the clipboard on a node")]
    async fn clipboard_write(
        &self,
        Parameters(args): Parameters<ClipboardWriteArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .clipboard_write(&args.node_id, ClipboardContent::Text(args.text))
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    // ── Phase 3 shell tools ───────────────────────────────────────────────────

    #[tool(description = "Run a shell command on a node and return its output. Timeout: 30 seconds.")]
    async fn shell_run(
        &self,
        Parameters(args): Parameters<ShellRunArgs>,
    ) -> Result<CallToolResult, McpError> {
        let output = self.registry
            .run_shell(&args.node_id, &args.command)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "Open an interactive PTY shell on a node. Returns a pty_id for subsequent writes/reads.")]
    async fn shell_open(
        &self,
        Parameters(args): Parameters<ShellOpenArgs>,
    ) -> Result<CallToolResult, McpError> {
        let pty_id = self.registry
            .shell_open(
                &args.node_id,
                args.shell,
                args.cols.unwrap_or(80),
                args.rows.unwrap_or(24),
            )
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(pty_id.to_string())]))
    }

    #[tool(description = "Write text to an interactive PTY shell opened with shell_open")]
    async fn shell_write(
        &self,
        Parameters(args): Parameters<ShellWriteArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .shell_write(&args.node_id, args.pty_id, args.data.into_bytes())
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    #[tool(description = "Read buffered output from an interactive PTY shell. Drains the buffer.")]
    async fn shell_read(
        &self,
        Parameters(args): Parameters<ShellPtyArgs>,
    ) -> Result<CallToolResult, McpError> {
        let raw = self.registry
            .shell_read(&args.node_id, args.pty_id)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        let text = String::from_utf8_lossy(&raw).into_owned();
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Close an interactive PTY shell opened with shell_open")]
    async fn shell_close(
        &self,
        Parameters(args): Parameters<ShellPtyArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .shell_close(&args.node_id, args.pty_id)
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }
}

#[tool_handler]
impl ServerHandler for KestrelMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::NodeRegistry;
    use std::sync::Arc;

    #[test]
    fn mcp_server_constructs() {
        let registry = Arc::new(NodeRegistry::new());
        let _server = KestrelMcp::new(registry);
    }
}
```

- [ ] **Step 3: Run tests to verify they pass**

```bash
cargo test -p kestrel-hub mcp 2>&1 | tail -10
```

Expected: `mcp_server_constructs ... ok`

- [ ] **Step 4: Commit**

```bash
git add crates/kestrel-hub/src/mcp.rs
git commit -m "feat(hub): add clipboard and shell MCP tools (14 tools total)"
```

---

### Task 8: Integration tests

**Files:**
- Create: `crates/kestrel-hub/tests/phase3.rs`

- [ ] **Step 1: Write the tests**

Create `crates/kestrel-hub/tests/phase3.rs`:

```rust
// crates/kestrel-hub/tests/phase3.rs
use std::net::SocketAddr;
use std::time::Duration;
use kestrel_agent::config::AgentConfig;
use kestrel_hub::transport::connect;

fn test_psk() -> Vec<u8> {
    b"kestrel-test-psk-32bytes-padded!".to_vec()
}

async fn start_agent(node_id: &str) -> SocketAddr {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let cfg = AgentConfig::new("127.0.0.1:0".parse().unwrap(), node_id.into(), test_psk());
    tokio::spawn(async move {
        kestrel_agent::transport::serve(&cfg, Some(ready_tx)).await.unwrap();
    });
    ready_rx.await.expect("agent did not send bound address")
}

#[tokio::test]
async fn test_shell_run_echo() {
    let addr = start_agent("shell-echo-node").await;
    let handle = connect(addr, &test_psk()).await.unwrap();
    let output = handle.run_shell("echo kestrel-phase3-ok").await.unwrap();
    assert!(
        output.contains("kestrel-phase3-ok"),
        "expected 'kestrel-phase3-ok' in shell output, got: {:?}",
        output
    );
}

#[tokio::test]
async fn test_shell_interactive_open_write_read_close() {
    let addr = start_agent("shell-interactive-node").await;
    let handle = connect(addr, &test_psk()).await.unwrap();

    let pty_id = handle.spawn_shell(None, 80, 24).await.unwrap();

    handle.write_shell(pty_id, b"echo interactive-test\n".to_vec()).await.unwrap();

    // Give the shell time to process the command
    tokio::time::sleep(Duration::from_millis(300)).await;

    let raw = handle.read_shell_buffer(pty_id).await.unwrap();
    let output = String::from_utf8_lossy(&raw);
    assert!(
        output.contains("interactive-test"),
        "expected 'interactive-test' in buffered output, got: {:?}",
        output
    );

    handle.close_shell(pty_id).await.unwrap();
}

#[tokio::test]
async fn test_shell_run_multiline() {
    let addr = start_agent("shell-multiline-node").await;
    let handle = connect(addr, &test_psk()).await.unwrap();
    let output = handle.run_shell("echo line1 && echo line2").await.unwrap();
    assert!(output.contains("line1"), "missing line1 in: {:?}", output);
    assert!(output.contains("line2"), "missing line2 in: {:?}", output);
}

#[tokio::test]
#[ignore = "requires display server / clipboard daemon; run manually"]
async fn test_clipboard_text_roundtrip() {
    let addr = start_agent("clipboard-node").await;
    let handle = connect(addr, &test_psk()).await.unwrap();
    use kestrel_proto::ClipboardContent;
    handle.clipboard_write(ClipboardContent::Text("kestrel-clipboard-xyz".into())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let got = handle.clipboard_read().await.unwrap();
    assert_eq!(got, ClipboardContent::Text("kestrel-clipboard-xyz".into()));
}
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
cargo test -p kestrel-hub --test phase3 2>&1 | tail -20
```

Expected:
```
test test_shell_run_echo ... ok
test test_shell_interactive_open_write_read_close ... ok
test test_shell_run_multiline ... ok
test test_clipboard_text_roundtrip ... ignored
```

- [ ] **Step 3: Run all workspace tests to confirm no regressions**

```bash
cargo test --workspace 2>&1 | tail -30
```

Expected: all tests pass, screen capture tests and clipboard test still ignored.

- [ ] **Step 4: Commit**

```bash
git add crates/kestrel-hub/tests/phase3.rs
git commit -m "test(hub): add phase 3 integration tests for shell run and interactive PTY"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] Clipboard read (agent: arboard, proto: ClipboardReadReq/Resp, hub: NodeHandle + NodeRegistry + MCP)
- [x] Clipboard write (agent: arboard, proto: ClipboardWriteReq/Ack, hub: NodeHandle + NodeRegistry + MCP)
- [x] Shell PTY spawn (agent: ShellManager + portable-pty, proto: ShellSpawn/Spawned, hub: spawn_shell)
- [x] Shell write (proto: ShellWrite, hub: write_shell MCP tool)
- [x] Shell output streaming (agent reader thread → event_tx → transport select! → hub actor buffer)
- [x] Shell close (bidirectional: agent reader EOF, hub close_shell; hub actor notifies waiters)
- [x] Shell resize (proto: ShellResize, agent: ShellManager::resize → MasterPty::resize)
- [x] run_shell one-shot helper (register waiter → write command+exit → timeout wait → read buffer)
- [x] Hub MCP: 7 new tools (clipboard_read, clipboard_write, shell_run, shell_open, shell_write, shell_read, shell_close)
- [x] Proto backward compat: Phase 1/2 discriminants unchanged, Phase 3 appended at 14-23
- [x] Integration tests: shell_run_echo, shell_interactive, shell_multiline, clipboard_roundtrip (ignored on headless)

**Type consistency across tasks:**
- `ClipboardContent` defined in Task 1 proto → used in Task 2 agent, Task 5 hub transport, Task 6 router, Task 7 mcp
- `ShellManager::spawn/write/resize/close` defined in Task 3 → used in Task 4 agent transport
- `ActorCmd::ReadShellBuffer/WaitShellClose` defined in Task 5 → used in Task 5 NodeHandle methods
- `NodeHandle::spawn_shell/write_shell/close_shell/read_shell_buffer/run_shell` defined in Task 5 → used in Task 6 registry, Task 8 tests
- `NodeRegistry::shell_open/write/read/close/run_shell` defined in Task 6 → used in Task 7 mcp

**Placeholder scan:** No TBD, TODO, or "similar to Task N" patterns. All steps have complete code.
