use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use kestrel_proto::{verify_response, KestrelMessage, MsgKind, OsInfo, Payload};
use rand::RngCore;
use rustls::ServerConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{accept_async, tungstenite::Message};

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
            Err(e) => {
                tracing::error!("accept error: {}", e);
                continue;
            }
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

    // Send challenge
    let mut nonce = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut nonce);
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id: 0,
        kind: MsgKind::Event,
        payload: Payload::Challenge { nonce },
    })?)).await?;

    // Receive and verify AuthResponse
    let raw = rx.next().await.context("no auth response from hub")??;
    let km: KestrelMessage = decode(raw.into_data())?;
    let Payload::AuthResponse { mac, node_id: claimed } = km.payload else {
        anyhow::bail!("expected AuthResponse, got other payload");
    };
    if !verify_response(&psk, &nonce, &mac) {
        let _ = tx.send(Message::Close(None)).await;
        anyhow::bail!("auth failed: bad MAC from claimed node_id={}", claimed);
    }
    tracing::info!("hub authenticated (claimed node_id={})", claimed);

    // Send SystemInfo (Ready signal)
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id: 0,
        kind: MsgKind::Event,
        payload: Payload::SystemInfo {
            os: OsInfo {
                name: std::env::consts::OS.into(),
                version: "unknown".into(),
            },
            displays: vec![],
            hostname: node_id,
        },
    })?)).await?;

    // Message loop
    while let Some(frame) = rx.next().await {
        let frame = frame?;
        if !frame.is_binary() {
            continue;
        }
        let km: KestrelMessage = decode(frame.into_data())?;
        if matches!(km.payload, Payload::Ping) {
            tx.send(Message::Binary(encode(&KestrelMessage {
                stream_id: km.stream_id,
                kind: MsgKind::Response,
                payload: Payload::Pong,
            })?)).await?;
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
