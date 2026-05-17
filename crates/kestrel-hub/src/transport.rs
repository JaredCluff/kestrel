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
