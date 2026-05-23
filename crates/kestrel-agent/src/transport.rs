// crates/kestrel-agent/src/transport.rs
use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use kestrel_proto::{
    verify_response, AccessibilityNode, DisplayInfo, KestrelMessage, MsgKind, OsInfo, Payload,
    AUTH_EXPORTER_LABEL,
};
use rand::RngCore;
use rustls::ServerConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{accept_async_with_config, tungstenite::Message, tungstenite::protocol::WebSocketConfig};

use crate::capabilities::{clipboard, input, screen, shell, shell::ShellManager};
use crate::config::AgentConfig;

/// Cap WebSocket message size to bound memory per frame. Screenshots are the
/// largest legitimate payload (PNGs of 4K displays); 8 MiB gives generous
/// headroom while still preventing a malicious peer from forcing the agent
/// to allocate hundreds of MB on a single bincode-decoded frame.
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
    psk: zeroize::Zeroizing<Vec<u8>>,
    node_id: String,
) -> anyhow::Result<()> {
    let tls = acceptor.accept(stream).await.context("TLS handshake failed")?;

    // Extract TLS exporter material BEFORE wrapping the stream in a WebSocket;
    // once tokio-tungstenite owns the TlsStream we can no longer reach the
    // rustls Connection. The exporter binds the auth MAC to this exact TLS
    // session — a MITM that terminates TLS on each leg sees a different
    // exporter than the legitimate endpoint, so the proxied MAC won't verify.
    let mut tls_exporter = [0u8; 32];
    {
        let (_io, conn) = tls.get_ref();
        conn.export_keying_material(&mut tls_exporter, AUTH_EXPORTER_LABEL, None)
            .map_err(|e| anyhow::anyhow!("TLS export_keying_material failed: {}", e))?;
    }

    let ws = accept_async_with_config(tls, Some(ws_config()))
        .await
        .context("WebSocket handshake failed")?;
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
    if !verify_response(&psk, &nonce, &tls_exporter, &mac) {
        let _ = tx.send(Message::Close(None)).await;
        anyhow::bail!(
            "auth failed: bad MAC from claimed node_id={} (PSK mismatch or MITM detected)",
            claimed
        );
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

    // Shell event channel — bounded so a stalled hub (TCP backpressure) can't
    // let PTY output queue without limit and OOM the agent. The reader threads
    // call `blocking_send`, which applies real backpressure to the PTY producer
    // via the kernel pipe buffer.
    let (event_tx, mut event_rx) =
        tokio::sync::mpsc::channel::<KestrelMessage>(shell::SHELL_EVENT_CAPACITY);
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
                        if let Err(e) = input::inject_key_event(key, modifiers, action).await {
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
                        let tree = tokio::task::spawn_blocking(crate::capabilities::ax::describe)
                            .await
                            .unwrap_or_else(|_| AccessibilityNode::unavailable());
                        tx.send(Message::Binary(encode(&KestrelMessage {
                            stream_id, kind: MsgKind::Response,
                            payload: Payload::DescribeResp { tree },
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
