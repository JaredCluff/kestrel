// crates/kestrel-hub/tests/dashboard_tls.rs
//
// End-to-end TLS test for the hub dashboard. Generates a self-signed
// cert at test time, points axum_server::bind_rustls at it, and hits
// the resulting HTTPS endpoint with reqwest. The point is to pin that
// the TLS plumbing (cert load, rustls config, bind) actually works —
// not to test rustls itself.
//
// reqwest is configured with `danger_accept_invalid_certs(true)`
// because the cert is self-signed and untrusted. That's the right
// trust model for the test (we DID just generate the cert ourselves);
// it's NOT the trust model recommended for production, where operators
// should use a cert issued by a CA the clients already trust.

use std::sync::Arc;

use axum_server::tls_rustls::RustlsConfig;
use kestrel_hub::dashboard::{router, AppState};
use kestrel_hub::router::NodeRegistry;

/// Install the rustls 0.23 process-level CryptoProvider once. Tests
/// in the same binary share state; subsequent calls return the prior
/// provider unchanged.
fn ensure_crypto_provider() {
    let _ = rustls_23::crypto::ring::default_provider().install_default();
}

fn test_master() -> Vec<u8> {
    b"kestrel-test-master-32bytes-pad!".to_vec()
}

fn starter_toml(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("kestrel.toml");
    std::fs::write(
        &path,
        r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#,
    )
    .unwrap();
    path
}

/// Generate a self-signed cert + key, write both as PEM into `dir`,
/// and return their paths. Cert is valid for "localhost".
fn write_self_signed_pem(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed cert");
    let cert_pem = cert.serialize_pem().unwrap();
    let key_pem = cert.serialize_private_key_pem();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert_pem).unwrap();
    std::fs::write(&key_path, key_pem).unwrap();
    (cert_path, key_path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dashboard_tls_serves_https_when_cert_and_key_provided() {
    ensure_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let toml = starter_toml(dir.path());
    let (cert_path, key_path) = write_self_signed_pem(dir.path());

    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, toml.to_str().unwrap().to_string(), test_master());

    let tls = RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .expect("load TLS config");
    let handle = axum_server::Handle::new();
    let server_handle = handle.clone();
    let app = router(state).into_make_service();
    // Bind to an ephemeral port. axum_server::Handle::listening() resolves
    // once the listener is bound, giving us the actual port to dial.
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server_task = tokio::spawn(async move {
        let _ = axum_server::bind_rustls(addr, tls)
            .handle(server_handle)
            .serve(app)
            .await;
    });
    let bound = handle
        .listening()
        .await
        .expect("server should bind within the test runtime");

    // reqwest with invalid-cert tolerance — we generated this cert
    // ourselves and don't have a CA.
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let url = format!("https://{}/api/nodes", bound);
    let resp = client.get(&url).send().await.expect("https GET");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    // /api/nodes returns a JSON array — at least the brackets are there.
    assert!(body.starts_with('['), "expected JSON array, got: {}", body);

    handle.shutdown();
    let _ = server_task.await;
}

#[tokio::test]
async fn rustls_config_rejects_malformed_cert_file() {
    ensure_crypto_provider();
    // Sanity check that bad input doesn't silently pass through.
    // The dashboard's start path explicitly map_errs the load failure
    // and refuses to start the hub.
    let dir = tempfile::tempdir().unwrap();
    let bogus_cert = dir.path().join("bogus.pem");
    let bogus_key = dir.path().join("bogus.key");
    std::fs::write(&bogus_cert, "not a pem").unwrap();
    std::fs::write(&bogus_key, "not a pem either").unwrap();
    let err = RustlsConfig::from_pem_file(&bogus_cert, &bogus_key)
        .await
        .expect_err("should reject garbage PEM");
    // Don't assert a specific message — rustls' error wording can drift
    // across versions. Just confirm it's an error, not silent success.
    let _ = err;
}
