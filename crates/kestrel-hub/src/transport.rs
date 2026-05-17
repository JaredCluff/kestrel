use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use kestrel_proto::{hmac_response, KestrelMessage, MsgKind, OsInfo, Payload};
use rustls::ClientConfig;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::{client_async, tungstenite::Message};

pub struct NodeConn {
    pub node_id: String,
    pub os_info: OsInfo,
}

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
    let server_name = rustls::ServerName::try_from("kestrel-agent")
        .expect("valid DNS name");
    connector.connect(server_name, tcp).await.context("TLS connect")
}

/// Connect to an agent at `addr`, complete auth handshake, start background
/// ping loop. Returns node info on success.
pub async fn connect(addr: SocketAddr, psk: &[u8]) -> anyhow::Result<NodeConn> {
    let tls = tls_connect(addr).await?;
    let url = format!("wss://{}", addr);
    let (ws, _) = client_async(url, tls).await.context("WebSocket handshake")?;
    let (mut tx, mut rx) = ws.split();

    let (node_id, os_info) = do_handshake(&mut tx, &mut rx, psk).await?;

    // Background ping loop — holds tx and rx to keep connection alive
    tokio::spawn(async move {
        let mut stream_id = 1u32;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let sent = Instant::now();
            let bytes = match encode(&KestrelMessage {
                stream_id,
                kind: MsgKind::Request,
                payload: Payload::Ping,
            }) {
                Ok(b) => b,
                Err(_) => break,
            };
            if tx.send(Message::Binary(bytes)).await.is_err() {
                break;
            }
            match rx.next().await {
                Some(Ok(_)) => tracing::debug!("pong rtt={}ms", sent.elapsed().as_millis()),
                _ => break,
            }
            stream_id += 1;
        }
    });

    Ok(NodeConn { node_id, os_info })
}

/// Connect, authenticate, send one Ping, return RTT. Used by integration tests.
pub async fn ping_once(addr: SocketAddr, psk: &[u8]) -> anyhow::Result<std::time::Duration> {
    let tls = tls_connect(addr).await?;
    let url = format!("wss://{}", addr);
    let (ws, _) = client_async(url, tls).await?;
    let (mut tx, mut rx) = ws.split();

    do_handshake(&mut tx, &mut rx, psk).await?;

    let sent = Instant::now();
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id: 1,
        kind: MsgKind::Request,
        payload: Payload::Ping,
    })?))
    .await?;
    let _ = rx.next().await.context("no Pong")??;
    Ok(sent.elapsed())
}

async fn do_handshake<Tx, Rx>(
    tx: &mut Tx,
    rx: &mut Rx,
    psk: &[u8],
) -> anyhow::Result<(String, OsInfo)>
where
    Tx: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    Rx: futures_util::Stream<
            Item = Result<Message, tokio_tungstenite::tungstenite::Error>,
        > + Unpin,
{
    // Receive Challenge
    let raw = rx.next().await.context("no challenge from agent")??;
    let km: KestrelMessage = decode(raw.into_data())?;
    let Payload::Challenge { nonce } = km.payload else {
        anyhow::bail!("expected Challenge, got other payload");
    };

    // Send AuthResponse
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id: 0,
        kind: MsgKind::Response,
        payload: Payload::AuthResponse {
            mac: hmac_response(psk, &nonce),
            node_id: "hub".into(),
        },
    })?))
    .await?;

    // Receive SystemInfo
    let raw = rx.next().await.context("no SystemInfo from agent")??;
    let km: KestrelMessage = decode(raw.into_data())?;
    let Payload::SystemInfo { os, hostname, .. } = km.payload else {
        anyhow::bail!("expected SystemInfo, got other payload");
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
