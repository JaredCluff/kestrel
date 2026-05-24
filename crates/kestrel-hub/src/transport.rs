// crates/kestrel-hub/src/transport.rs
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use kestrel_proto::{
    AccessibilityNode, ClipboardContent, hmac_response, Button, KeyCode, KestrelMessage, Modifiers, MsgKind,
    OsInfo, Payload, PressRelease, Rect, AUTH_EXPORTER_LABEL,
};
use rustls::ClientConfig;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::{
    client_async_with_config,
    tungstenite::Message,
    tungstenite::protocol::WebSocketConfig,
    WebSocketStream,
};

/// Per-PTY buffer cap. A misbehaving (or malicious) agent that pipes
/// /dev/urandom into a PTY would otherwise grow hub RAM without bound;
/// callers normally drain via shell_read but the buffer fills between drains.
/// 1 MiB lets a reasonable terminal session backlog while keeping worst-case
/// memory at `1 MiB × open PTYs` per actor.
const SHELL_BUFFER_CAP: usize = 1024 * 1024;

/// Marker appended when the per-PTY buffer truncates so the operator notices.
const SHELL_TRUNCATED_MARKER: &[u8] = b"\n[kestrel: shell output truncated to cap]\n";

/// Matches the agent's WebSocket message-size cap. Both sides enforce so a
/// malicious peer can't force allocation of a giant frame on either end.
fn ws_config() -> WebSocketConfig {
    // `..Default::default()` rather than field-reassign-after-Default so
    // future fields added by tungstenite upgrades inherit their library
    // defaults instead of being silently dropped.
    WebSocketConfig {
        max_message_size: Some(8 * 1024 * 1024),
        max_frame_size: Some(8 * 1024 * 1024),
        ..Default::default()
    }
}

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

/// Phase 13b: agent-originated WebRTC signalling messages routed back
/// through `run_actor`'s `webrtc_tx` side channel so the hub can fold
/// them into its SessionRegistry. Variant per inbound payload that
/// belongs to the SDP/ICE exchange.
#[derive(Debug, Clone)]
pub enum WebRtcEvent {
    Answer { session_id: String, sdp: String },
    Ice { session_id: String, candidate: String },
}

/// Phase 6: callers of `run_actor` can opt-in to side channels that
/// receive every `Payload::WorldUpdate` and `Payload::Capabilities`
/// the agent pushes post-handshake. Supervisor wires these to the
/// NodeRegistry's `observe_world_update` / `record_capabilities`.
/// Tests that don't care pass `None`.
async fn run_actor(
    ws: WsStream,
    mut cmd_rx: mpsc::Receiver<ActorCmd>,
    world_tx: Option<mpsc::UnboundedSender<kestrel_proto::WorldState>>,
    caps_tx: Option<mpsc::UnboundedSender<kestrel_proto::Capabilities>>,
    webrtc_tx: Option<mpsc::UnboundedSender<WebRtcEvent>>,
) {
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
                                let buf = shell_buffers.entry(*pty_id).or_default();
                                append_with_cap(buf, data);
                            }
                            Payload::ShellClose { pty_id } => {
                                if let Some(waiter) = shell_close_waiters.remove(pty_id) {
                                    let _ = waiter.send(());
                                }
                            }
                            Payload::WorldUpdate { state } => {
                                // Forward to the registry-side sink if
                                // the caller wired one up. Best-effort:
                                // a closed channel just drops the
                                // update (registry was torn down).
                                if let Some(tx) = world_tx.as_ref() {
                                    let _ = tx.send(state.clone());
                                }
                            }
                            Payload::Capabilities { caps } => {
                                // Phase 8 follow-up: live capability
                                // updates received post-handshake.
                                if let Some(tx) = caps_tx.as_ref() {
                                    let _ = tx.send(caps.clone());
                                }
                            }
                            Payload::WebRtcAnswer { session_id, sdp } => {
                                // Phase 13b signalling relay: agent's
                                // SDP answer flows back through the
                                // hub-side WebRtcEvent channel for the
                                // SessionRegistry to record.
                                if let Some(tx) = webrtc_tx.as_ref() {
                                    let _ = tx.send(WebRtcEvent::Answer {
                                        session_id: session_id.clone(),
                                        sdp: sdp.clone(),
                                    });
                                }
                            }
                            Payload::WebRtcIce { session_id, candidate } => {
                                if let Some(tx) = webrtc_tx.as_ref() {
                                    let _ = tx.send(WebRtcEvent::Ice {
                                        session_id: session_id.clone(),
                                        candidate: candidate.clone(),
                                    });
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
    // Drain any commands still queued. Without this, a `ReadShellBuffer` or
    // `Request` racing the disconnect would see its oneshot dropped (caller
    // gets "actor dropped reply") instead of the buffered data / a clear
    // "connection closed" error. Closing the channel and re-receiving lets us
    // hand back what we still have in `shell_buffers`.
    cmd_rx.close();
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            ActorCmd::Fire(_) => {}
            ActorCmd::Request { reply, .. } => {
                let _ = reply.send(Err(anyhow::anyhow!("connection closed")));
            }
            ActorCmd::ReadShellBuffer { pty_id, reply } => {
                let data = shell_buffers.remove(&pty_id).unwrap_or_default();
                let _ = reply.send(data);
            }
            ActorCmd::WaitShellClose { reply, .. } => {
                let _ = reply.send(());
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
    /// Displays the agent reported at connection time. Used for pre-flight
    /// validation of `screenshot` requests so an out-of-range `display` index
    /// returns a clear error instead of a misleading empty PNG.
    pub displays: Vec<kestrel_proto::DisplayInfo>,
    /// Phase 8: capabilities the agent advertised after SystemInfo.
    /// `None` when the agent didn't send a Capabilities frame (older
    /// agents, or those that timed out the optional capability
    /// exchange during handshake).
    pub capabilities: Option<kestrel_proto::Capabilities>,
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
        reply_rx.await.map_err(|_| anyhow::anyhow!("actor dropped reply"))?
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

    // ── Phase 4 accessibility ─────────────────────────────────────────────────

    pub async fn describe(&self, display: u8) -> anyhow::Result<AccessibilityNode> {
        let reply = self.request(Payload::DescribeReq { display }).await?;
        match reply.payload {
            Payload::DescribeResp { tree } => Ok(tree),
            _ => anyhow::bail!("expected DescribeResp"),
        }
    }

    // ── Phase 12b plugin proxy ────────────────────────────────────────────────

    pub async fn plugin_list(&self) -> anyhow::Result<Vec<kestrel_proto::PluginInfoWire>> {
        let reply = self.request(Payload::PluginListReq).await?;
        match reply.payload {
            Payload::PluginListResp { plugins } => Ok(plugins),
            Payload::Error { message, .. } => anyhow::bail!("plugin_list: {}", message),
            _ => anyhow::bail!("expected PluginListResp"),
        }
    }

    pub async fn plugin_invoke(
        &self,
        plugin: String,
        tool: String,
        args_json: String,
    ) -> anyhow::Result<String> {
        let reply = self
            .request(Payload::PluginCallReq { plugin, tool, args_json })
            .await?;
        match reply.payload {
            Payload::PluginCallResp { result_json } => Ok(result_json),
            Payload::Error { message, .. } => anyhow::bail!("plugin_invoke: {}", message),
            _ => anyhow::bail!("expected PluginCallResp"),
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

    // ── Phase 13b WebRTC signalling relay ─────────────────────────────────────

    /// Forward a browser-originated SDP offer to the agent. The agent
    /// replies with `Payload::WebRtcAnswer`, routed through the actor's
    /// caps_tx-style side channel (see `caps_tx` for the analogous
    /// pattern) — in this PR the answer arrives back via the
    /// `webrtc_tx` channel returned from `connect_with_world_sink`.
    pub async fn send_webrtc_offer(&self, session_id: String, sdp: String) -> anyhow::Result<()> {
        self.fire(Payload::WebRtcOffer { session_id, sdp }).await
    }

    /// Forward a browser-originated ICE candidate to the agent.
    pub async fn send_webrtc_ice(&self, session_id: String, candidate: String) -> anyhow::Result<()> {
        self.fire(Payload::WebRtcIce { session_id, candidate }).await
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

pub async fn connect(
    addr: SocketAddr,
    psk: &[u8],
) -> anyhow::Result<(NodeHandle, tokio::task::JoinHandle<()>)> {
    let (handle, actor_join, _world_rx, _caps_rx, _webrtc_rx) =
        connect_with_world_sink(addr, psk).await?;
    // Caller didn't want the side channels — drain to /dev/null so the
    // actor's send doesn't pile up unsent items.
    drop(_world_rx);
    drop(_caps_rx);
    drop(_webrtc_rx);
    Ok((handle, actor_join))
}

/// Phase 6 variant of `connect`. Additionally returns side channels for
/// `Payload::WorldUpdate` and `Payload::Capabilities`. Supervisor wires
/// these to the NodeRegistry's `observe_world_update` and
/// `record_capabilities`; tests that don't care can use the bare
/// `connect()` wrapper above.
pub async fn connect_with_world_sink(
    addr: SocketAddr,
    psk: &[u8],
) -> anyhow::Result<(
    NodeHandle,
    tokio::task::JoinHandle<()>,
    mpsc::UnboundedReceiver<kestrel_proto::WorldState>,
    mpsc::UnboundedReceiver<kestrel_proto::Capabilities>,
    mpsc::UnboundedReceiver<WebRtcEvent>,
)> {
    let tls = tls_connect(addr).await?;

    // Extract TLS exporter material BEFORE the WebSocket wraps the stream;
    // bound into the auth MAC to defeat MITM with self-signed certs. See
    // kestrel_proto::auth.
    let mut tls_exporter = [0u8; 32];
    {
        let (_io, conn) = tls.get_ref();
        conn.export_keying_material(&mut tls_exporter, AUTH_EXPORTER_LABEL, None)
            .map_err(|e| anyhow::anyhow!("TLS export_keying_material failed: {}", e))?;
    }

    let url = format!("wss://{}", addr);
    let (ws, _) = client_async_with_config(url, tls, Some(ws_config()))
        .await
        .context("WebSocket handshake")?;
    let (mut tx, mut rx) = ws.split();

    let (node_id, os_info, displays, capabilities) =
        do_handshake(&mut tx, &mut rx, psk, &tls_exporter).await?;

    let ws = tx.reunite(rx).expect("same stream");
    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    let (world_tx, world_rx) = mpsc::unbounded_channel();
    let (caps_tx, caps_rx) = mpsc::unbounded_channel();
    let (webrtc_tx, webrtc_rx) = mpsc::unbounded_channel();
    let actor_join = tokio::spawn(run_actor(
        ws,
        cmd_rx,
        Some(world_tx),
        Some(caps_tx),
        Some(webrtc_tx),
    ));

    Ok((
        NodeHandle { node_id, os_info, displays, capabilities, cmd_tx },
        actor_join,
        world_rx,
        caps_rx,
        webrtc_rx,
    ))
}

pub async fn ping_once(addr: SocketAddr, psk: &[u8]) -> anyhow::Result<Duration> {
    let (handle, _actor) = connect(addr, psk).await?;
    let sent = Instant::now();
    handle.request(Payload::Ping).await?;
    Ok(sent.elapsed())
}

async fn do_handshake<Tx, Rx>(
    tx: &mut Tx,
    rx: &mut Rx,
    psk: &[u8],
    tls_exporter: &[u8],
) -> anyhow::Result<(
    String,
    OsInfo,
    Vec<kestrel_proto::DisplayInfo>,
    Option<kestrel_proto::Capabilities>,
)>
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
            mac: hmac_response(psk, &nonce, tls_exporter),
            node_id: "hub".into(),
        },
    })?)).await?;
    let raw = rx.next().await.context("no SystemInfo from agent")??;
    let km: KestrelMessage = decode(raw.into_data())?;
    let Payload::SystemInfo { os, hostname, displays } = km.payload else {
        anyhow::bail!("expected SystemInfo");
    };
    tracing::info!(
        "connected to node {} ({}, {} display(s))",
        hostname,
        os.name,
        displays.len()
    );
    // Phase 8: optional Capabilities follows SystemInfo. Old agents
    // that don't send it just yield None here; the hub still
    // accepts the connection. We peek-or-skip rather than block
    // forever on the next frame — if the first post-SystemInfo
    // frame isn't Capabilities, we treat it as part of the normal
    // message loop and pass via the actor instead. For simplicity
    // we just wait briefly and accept None on timeout.
    let caps = match tokio::time::timeout(
        std::time::Duration::from_millis(500),
        rx.next(),
    ).await {
        Ok(Some(Ok(raw))) => {
            if let Ok(km) = decode(raw.into_data()) {
                if let Payload::Capabilities { caps } = km.payload {
                    Some(caps)
                } else {
                    tracing::debug!("post-SystemInfo frame wasn't Capabilities; running without");
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    };
    Ok((hostname, os, displays, caps))
}

fn encode(msg: &KestrelMessage) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serde::encode_to_vec(msg, bincode::config::standard())?)
}

fn decode(bytes: Vec<u8>) -> anyhow::Result<KestrelMessage> {
    let (msg, _) = bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;
    Ok(msg)
}

/// Append `data` to a per-PTY shell buffer, dropping the oldest bytes if total
/// length would exceed `SHELL_BUFFER_CAP`. A one-shot truncation marker is
/// appended on the first overflow so the operator notices output was lost.
fn append_with_cap(buf: &mut Vec<u8>, data: &[u8]) {
    let already_truncated = buf.ends_with(SHELL_TRUNCATED_MARKER);
    buf.extend_from_slice(data);
    if buf.len() > SHELL_BUFFER_CAP {
        let excess = buf.len() - SHELL_BUFFER_CAP;
        buf.drain(..excess);
        if !already_truncated {
            buf.extend_from_slice(SHELL_TRUNCATED_MARKER);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_with_cap_passes_through_small_writes() {
        let mut buf = Vec::new();
        append_with_cap(&mut buf, b"hello");
        append_with_cap(&mut buf, b" world");
        assert_eq!(buf, b"hello world");
    }

    #[test]
    fn append_with_cap_truncates_when_exceeded_and_marks() {
        let mut buf = Vec::new();
        // First write fills under the cap.
        append_with_cap(&mut buf, &vec![b'A'; SHELL_BUFFER_CAP - 10]);
        assert_eq!(buf.len(), SHELL_BUFFER_CAP - 10);
        assert!(!buf.ends_with(SHELL_TRUNCATED_MARKER));
        // Second write pushes over; buffer drops oldest bytes and appends marker.
        append_with_cap(&mut buf, &vec![b'B'; 1000]);
        assert!(buf.len() <= SHELL_BUFFER_CAP + SHELL_TRUNCATED_MARKER.len());
        assert!(buf.ends_with(SHELL_TRUNCATED_MARKER));
    }

    #[test]
    fn append_with_cap_stays_bounded_across_many_overflows() {
        // Buffer must never grow unbounded, even after many overflowing writes.
        let mut buf = Vec::new();
        for _ in 0..20 {
            append_with_cap(&mut buf, &vec![b'Z'; 200_000]);
        }
        assert!(
            buf.len() <= SHELL_BUFFER_CAP + 2 * SHELL_TRUNCATED_MARKER.len(),
            "buffer grew unbounded: {} bytes",
            buf.len()
        );
        // At least one truncation marker should be present so the operator knows.
        assert!(
            buf.windows(SHELL_TRUNCATED_MARKER.len())
                .any(|w| w == SHELL_TRUNCATED_MARKER),
            "expected at least one truncation marker"
        );
    }

    #[test]
    fn append_with_cap_post_drain_overflow_remarks() {
        // The actor's ReadShellBuffer drain calls `HashMap::remove`, which
        // discards the entry entirely. The next ShellOutput frame for that
        // pty_id starts with a fresh empty Vec — verified here by REPLACING
        // `buf` to mimic the actor's drain → new entry behavior, then
        // confirming a fresh overflow gets its own marker.
        let mut buf = Vec::new();
        append_with_cap(&mut buf, &vec![b'A'; SHELL_BUFFER_CAP + 100]);
        assert!(buf.ends_with(SHELL_TRUNCATED_MARKER));

        // Drain — actor's HashMap::remove drops `buf` entirely. Simulate.
        buf = Vec::new();
        append_with_cap(&mut buf, b"some shell output");
        assert!(!buf.ends_with(SHELL_TRUNCATED_MARKER));
        append_with_cap(&mut buf, &vec![b'B'; SHELL_BUFFER_CAP]);
        assert!(
            buf.ends_with(SHELL_TRUNCATED_MARKER),
            "fresh post-drain overflow should re-mark"
        );
    }

    /// Helper: count non-overlapping occurrences of `needle` in `hay`. Used
    /// in the marker-stacking tests so a regression that appends two markers
    /// back-to-back is visible (vs. just "at least one is present").
    fn count_markers(hay: &[u8]) -> usize {
        if SHELL_TRUNCATED_MARKER.is_empty() {
            return 0;
        }
        let mut n = 0;
        let mut i = 0;
        while i + SHELL_TRUNCATED_MARKER.len() <= hay.len() {
            if &hay[i..i + SHELL_TRUNCATED_MARKER.len()] == SHELL_TRUNCATED_MARKER {
                n += 1;
                i += SHELL_TRUNCATED_MARKER.len();
            } else {
                i += 1;
            }
        }
        n
    }

    #[test]
    fn append_with_cap_two_overflows_no_drain_holds_exactly_one_marker() {
        // If the operator doesn't drain between two overflows on the same
        // buffer, the dedup at `let already_truncated = buf.ends_with(MARKER)`
        // should prevent a SECOND marker from being appended. The first
        // overflow leaves the buffer ending with the marker; the second
        // overflow sees `already_truncated == true` and skips the append. The
        // single marker may end up mid-buffer after drain, but there should
        // only ever be one — not two stacked.
        let mut buf = Vec::new();
        append_with_cap(&mut buf, &vec![b'A'; SHELL_BUFFER_CAP + 100]);
        assert_eq!(count_markers(&buf), 1, "first overflow appends one marker");
        assert!(buf.ends_with(SHELL_TRUNCATED_MARKER));

        // Immediately overflow again, no drain. With dedup working, the
        // marker count must STAY AT ONE — not grow to two. This is the
        // specific regression the test guards against.
        append_with_cap(&mut buf, &vec![b'B'; SHELL_BUFFER_CAP / 2]);
        assert_eq!(
            count_markers(&buf),
            1,
            "second overflow without drain must NOT double-mark"
        );
        assert!(buf.len() <= SHELL_BUFFER_CAP + SHELL_TRUNCATED_MARKER.len());
    }

    #[test]
    fn append_with_cap_data_containing_marker_bytes_stays_bounded() {
        // Pathological case: the shell writes bytes that happen to equal the
        // truncation marker. The dedup logic at `ends_with(MARKER)` will
        // misfire — it treats real shell data as if it were a prior
        // truncation, so a subsequent overflow won't append a fresh marker.
        // That's a documented loss of marker fidelity, NOT a memory-safety
        // problem: the cap is enforced unconditionally. This test pins the
        // bounded-memory contract (the property that actually matters) and
        // explicitly does NOT assert on marker count, since marker semantics
        // are intentionally degraded for this input.
        let mut buf = Vec::new();
        let payload = SHELL_TRUNCATED_MARKER.to_vec();
        append_with_cap(&mut buf, &payload);
        assert_eq!(buf.len(), SHELL_TRUNCATED_MARKER.len());
        // Overflow with arbitrary bytes — buffer must stay capped regardless
        // of whether dedup decides to add a marker.
        append_with_cap(&mut buf, &vec![b'X'; SHELL_BUFFER_CAP + 10]);
        assert!(
            buf.len() <= SHELL_BUFFER_CAP + SHELL_TRUNCATED_MARKER.len(),
            "buffer must stay bounded even when payload contains marker bytes"
        );
    }
}
