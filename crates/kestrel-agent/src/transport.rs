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

use crate::capabilities::{
    clipboard, input, screen, shell, shell::ShellManager, webrtc_session,
};
use std::collections::HashMap;

/// Coarse runtime check for "are we running as root on a Unix-ish OS?"
/// Used by capability advertisement (Phase 8). Cross-platform safe —
/// returns false on Windows where the concept doesn't apply.
fn is_running_as_root() -> bool {
    #[cfg(unix)]
    {
        // SAFETY: getuid() is a pure FFI call with no preconditions.
        unsafe { libc_getuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

#[cfg(unix)]
#[link(name = "c")]
unsafe extern "C" {
    #[link_name = "getuid"]
    fn libc_getuid() -> u32;
}

/// Best-effort docker probe. Checks the standard daemon socket
/// paths; doesn't catch remote DOCKER_HOST configs.
fn docker_socket_present() -> bool {
    let candidates = [
        "/var/run/docker.sock",
        "/run/docker.sock",
        // macOS Docker Desktop puts its socket under the user dir.
        // We don't try to resolve $HOME — Desktop also bridges
        // /var/run, so the first check covers most users.
    ];
    candidates.iter().any(|p| std::path::Path::new(p).exists())
}
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
    // Graceful shutdown: SIGTERM / SIGINT stops the accept loop and
    // returns. In-flight per-connection tasks are detached; their own
    // I/O closes when the runtime tears down, and Drop on ShellManager
    // runs close_all() so PTY children get SIGKILL'd instead of zombied.
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, peer) = match accept_result {
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
            _ = wait_for_shutdown_signal() => {
                tracing::info!("agent: shutdown signal received, stopping accept loop");
                return Ok(());
            }
        }
    }
}

/// Listen for SIGTERM / SIGINT (Unix) or Ctrl-C (Windows) and resolve
/// when either fires. Mirrors the hub's shutdown plumbing but lives
/// in-process here since the agent doesn't need the broadcast-to-many
/// pattern — there's only one consumer (the accept loop).
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let sigterm = signal(SignalKind::terminate());
        match sigterm {
            Ok(mut s) => {
                tokio::select! {
                    _ = s.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            Err(e) => {
                tracing::warn!("agent: SIGTERM listener unavailable ({}), only Ctrl-C will trigger shutdown", e);
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
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
    // Capture the displays list before it's consumed by the SystemInfo
    // payload so the WorldObserver can include it in every WorldState
    // snapshot it emits.
    let world_displays = displays.clone();
    let has_display = !world_displays.is_empty();
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id: 0, kind: MsgKind::Event,
        payload: Payload::SystemInfo {
            os: OsInfo { name: std::env::consts::OS.into(), version: "unknown".into() },
            displays,
            hostname: node_id,
        },
    })?)).await?;

    // Phase 8: capability advertisement. Best-effort detection of
    // common runtime capabilities — the AI uses these to pick a
    // node by predicate via the hub's `fleet_find` MCP tool.
    let caps = kestrel_proto::Capabilities {
        os: std::env::consts::OS.into(),
        // GPU detection is platform-specific and we don't have a
        // dependency for it. v1 reports false; future PR can wire
        // in `metal-rs` on macOS, vulkan probe on Linux, etc.
        has_gpu: false,
        has_display,
        // sudo: best-effort. On Unix, `id -u == 0` means root (so
        // sudo is trivially "yes"); otherwise we'd have to run
        // `sudo -n true` which prompts on some configs. Conservative:
        // report false unless we're literally root.
        has_sudo: is_running_as_root(),
        // Docker: probe by checking the canonical socket paths
        // without running a subprocess. Catches the common case of
        // a local docker daemon running on UNIX; doesn't catch
        // remote DOCKER_HOST configs (those would need a real
        // dial-and-API-version call).
        has_docker: docker_socket_present(),
    };
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id: 0, kind: MsgKind::Event,
        payload: Payload::Capabilities { caps },
    })?)).await?;

    // Shell event channel — bounded so a stalled hub (TCP backpressure) can't
    // let PTY output queue without limit and OOM the agent. The reader threads
    // call `blocking_send`, which applies real backpressure to the PTY producer
    // via the kernel pipe buffer. The WorldObserver shares this channel so
    // its WorldUpdate pushes ride the same backpressure-aware path.
    let (event_tx, mut event_rx) =
        tokio::sync::mpsc::channel::<KestrelMessage>(shell::SHELL_EVENT_CAPACITY);
    let shell_mgr = ShellManager::new(event_tx.clone());

    // Phase 12b: discover plugins at connection time. Spawning at
    // each (re)connect is intentional — plugins crash, get installed
    // /uninstalled, and we want a fresh list per session. Cheap if
    // there are no plugins (returns empty map immediately).
    let plugins = crate::capabilities::plugins::discover_and_spawn().await;

    // World-state observer: every ~2s, sample local state and push a
    // WorldUpdate event if anything changed. JoinHandle is dropped at
    // function-end (when the connection terminates) which aborts the
    // observer task naturally. Threading the ShellManager's meta
    // handle lets the observer surface per-PTY metadata in
    // WorldState.shells.
    let _world_observer_task = crate::capabilities::world::WorldObserver::with_shells(
        event_tx.clone(),
        world_displays,
        shell_mgr.meta_handle(),
    )
    .spawn();

    // Phase 13b: per-connection WebRTC session map keyed by hub-minted
    // session_id. Sessions outlive any single frame — ICE candidates
    // arrive in trickled bursts after the SDP exchange completes.
    let mut webrtc_sessions: HashMap<String, Arc<webrtc_session::WebRtcSession>> =
        HashMap::new();
    // Sessions report their own teardown here when the PC moves to
    // Disconnected/Failed/Closed. The select! arm below drops the
    // entry; without this the map would grow unboundedly across
    // browser tab opens/closes for the lifetime of this connection.
    let (webrtc_closed_tx, mut webrtc_closed_rx) =
        tokio::sync::mpsc::channel::<String>(8);

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
                        let (w, h) = screen::primary_display_dims();
                        if let Err(e) = input::inject_mouse_move(x, y, w, h).await {
                            tracing::warn!("mouse_move error: {}", e);
                        }
                    }
                    Payload::MouseButton { button, action, x, y } => {
                        let (w, h) = screen::primary_display_dims();
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
                    Payload::PluginListReq => {
                        let list: Vec<kestrel_proto::PluginInfoWire> = plugins
                            .values()
                            .map(|p| kestrel_proto::PluginInfoWire {
                                name: p.info.name.clone(),
                                version: p.info.version.clone(),
                                description: p.info.description.clone(),
                                tools: p.info.tools.clone(),
                            })
                            .collect();
                        tx.send(Message::Binary(encode(&KestrelMessage {
                            stream_id, kind: MsgKind::Response,
                            payload: Payload::PluginListResp { plugins: list },
                        })?)).await?;
                    }
                    Payload::WebRtcOffer { session_id, sdp } => {
                        // Boot a screen-stream + RTCPeerConnection,
                        // produce the SDP answer, and emit ICE candidates
                        // back via event_tx. Failure is logged but
                        // doesn't kill the connection — operators just
                        // won't see video for that session.
                        match webrtc_session::handle_offer(
                            session_id.clone(),
                            sdp,
                            event_tx.clone(),
                            webrtc_closed_tx.clone(),
                        ).await {
                            Ok(session) => {
                                webrtc_sessions.insert(session_id, session);
                            }
                            Err(e) => {
                                tracing::warn!("webrtc handle_offer failed: {}", e);
                                let _ = tx.send(Message::Binary(encode(&KestrelMessage {
                                    stream_id, kind: MsgKind::Response,
                                    payload: Payload::Error {
                                        code: kestrel_proto::ErrorCode::Internal,
                                        message: format!("webrtc setup failed: {}", e),
                                    },
                                })?)).await;
                            }
                        }
                    }
                    Payload::WebRtcIce { session_id, candidate } => {
                        if let Some(session) = webrtc_sessions.get(&session_id) {
                            if let Err(e) =
                                webrtc_session::add_remote_ice(session, &candidate).await
                            {
                                tracing::warn!(
                                    "webrtc add_ice for session {}: {}",
                                    session_id,
                                    e
                                );
                            }
                        } else {
                            tracing::debug!(
                                "webrtc: ICE for unknown session {}",
                                session_id
                            );
                        }
                    }
                    Payload::PluginCallReq { plugin, tool, args_json } => {
                        let response = match plugins.get(&plugin) {
                            None => Payload::Error {
                                code: kestrel_proto::ErrorCode::NotFound,
                                message: format!("plugin '{}' not loaded", plugin),
                            },
                            Some(handle) => {
                                let args: serde_json::Value =
                                    serde_json::from_str(&args_json)
                                        .unwrap_or(serde_json::Value::Null);
                                match handle.call(&tool, args).await {
                                    Ok(v) => Payload::PluginCallResp {
                                        result_json: serde_json::to_string(&v)
                                            .unwrap_or_else(|_| "null".into()),
                                    },
                                    Err(e) => Payload::Error {
                                        code: kestrel_proto::ErrorCode::Internal,
                                        message: format!("plugin call failed: {}", e),
                                    },
                                }
                            }
                        };
                        tx.send(Message::Binary(encode(&KestrelMessage {
                            stream_id, kind: MsgKind::Response,
                            payload: response,
                        })?)).await?;
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
            closed_id = webrtc_closed_rx.recv() => {
                // Channel can only close when this scope ends — receiver
                // owns one of two senders (the held webrtc_closed_tx
                // and any clones in active sessions), so recv() returning
                // None means we're tearing down. Continue rather than
                // matching exhaustively to avoid breaking out of the loop
                // on a spurious wakeup.
                if let Some(id) = closed_id {
                    webrtc_sessions.remove(&id);
                }
            }
        }
    }
    Ok(())
}

fn encode(msg: &KestrelMessage) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serde::encode_to_vec(msg, bincode::config::standard())?)
}

fn decode(bytes: Vec<u8>) -> anyhow::Result<KestrelMessage> {
    let (msg, _) = bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;
    Ok(msg)
}
