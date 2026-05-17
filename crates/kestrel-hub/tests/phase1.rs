// crates/kestrel-hub/tests/phase1.rs
use std::net::SocketAddr;
use kestrel_agent::config::AgentConfig;
use kestrel_hub::transport::{connect, ping_once};

fn test_psk() -> Vec<u8> {
    // 32-byte test PSK — never used outside tests
    b"kestrel-test-psk-32bytes-padded!".to_vec()
}

async fn start_agent(node_id: &str) -> SocketAddr {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let cfg = AgentConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        node_id.into(),
        test_psk(),
    );
    tokio::spawn(async move {
        kestrel_agent::transport::serve(&cfg, Some(ready_tx))
            .await
            .unwrap();
    });
    ready_rx.await.expect("agent did not send bound address")
}

#[tokio::test]
async fn test_auth_handshake_succeeds() {
    let addr = start_agent("test-node").await;
    let conn = connect(addr, &test_psk()).await.unwrap();
    assert_eq!(conn.node_id, "test-node");
    assert!(!conn.os_info.name.is_empty());
}

#[tokio::test]
async fn test_wrong_psk_fails() {
    let addr = start_agent("auth-node").await;
    let bad_psk = b"this-is-the-wrong-psk-32bytepad!".to_vec();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        connect(addr, &bad_psk),
    )
    .await;
    match result {
        Err(_elapsed) => panic!("connect() with wrong PSK timed out — auth did not reject the connection"),
        Ok(conn_result) => assert!(conn_result.is_err(), "expected connect() to fail with wrong PSK"),
    }
}

#[tokio::test]
async fn test_ping_pong_rtt_under_100ms() {
    let addr = start_agent("ping-node").await;
    let rtt = ping_once(addr, &test_psk()).await.unwrap();
    assert!(
        rtt.as_millis() < 100,
        "loopback ping RTT was {}ms, expected < 100ms",
        rtt.as_millis()
    );
}
