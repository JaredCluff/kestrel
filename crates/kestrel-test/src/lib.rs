// crates/kestrel-test/src/lib.rs
//
// Shared test fixtures. Every helper here lived in 5+ test files
// before — `test_psk`, `test_master`, `start_agent`, `build_app`,
// `cookie_for`. Centralising them eliminates drift (the canonical
// `start_agent` was subtly different between phase1.rs and
// phase_6_13_integration.rs) and makes future test files trivial to
// write.
//
// Not a binary; no public CLI. Pulled in via `dev-dependencies` only.

use std::net::SocketAddr;
use std::sync::Arc;

use kestrel_agent::config::AgentConfig;
use kestrel_hub::dashboard::{router, AppState};
use kestrel_hub::router::NodeRegistry;

// ── Cryptographic fixtures ───────────────────────────────────────────────────

/// 32-byte test PSK. Used by tests that pre-date the per-node-PSK
/// refactor (they pin agent and supervisor to the SAME key by passing
/// this around). New tests should prefer [`test_master`] +
/// [`derive_test_psk`] which mirror the production HKDF flow.
pub fn test_psk() -> Vec<u8> {
    b"kestrel-test-psk-32bytes-padded!".to_vec()
}

/// 32-byte test master secret. The hub uses this as HKDF input;
/// every agent's PSK is `derive_test_psk(node_id)`. Pinning a fixed
/// master makes tests reproducible — same master + same node_id
/// always produces the same per-node PSK.
pub fn test_master() -> Vec<u8> {
    b"kestrel-test-master-32bytes-pad!".to_vec()
}

/// `test_master` wrapped in `Zeroizing` for code paths (supervisor,
/// AppState constructors) that take the protected wrapper directly.
pub fn test_master_zeroizing() -> zeroize::Zeroizing<Vec<u8>> {
    zeroize::Zeroizing::new(test_master())
}

/// Derive a per-node PSK from the fixed test master. Matches what
/// the production supervisor does on connect, so an agent enrolled
/// with `derive_test_psk("alpha")` authenticates against a supervisor
/// configured with master = test_master() and node_id = "alpha".
pub fn derive_test_psk(node_id: &str) -> Vec<u8> {
    kestrel_proto::derive_per_node_psk(&test_master(), node_id).to_vec()
}

// ── Agent fixture ────────────────────────────────────────────────────────────

/// Spawn an agent in the current tokio runtime on a random local
/// port (`127.0.0.1:0`). Returns the bound address.
///
/// The agent uses [`test_psk`] as its PSK — i.e. NOT the HKDF-derived
/// per-node PSK. Use [`start_agent_with_master`] for tests that want
/// the production-shaped (master + derived) flow.
///
/// Lives on the caller's runtime via `tokio::spawn`. Drops when the
/// test ends (runtime tears down). For tests that need to forcibly
/// stop the agent mid-test, use [`start_agent_with_shutdown`].
pub async fn start_agent(node_id: &str) -> SocketAddr {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let cfg = AgentConfig::new("127.0.0.1:0".parse().unwrap(), node_id.into(), test_psk());
    tokio::spawn(async move {
        let _ = kestrel_agent::transport::serve(&cfg, Some(ready_tx)).await;
    });
    ready_rx.await.expect("agent did not bind")
}

/// Like [`start_agent`] but the agent's PSK is HKDF-derived from
/// [`test_master`] + `node_id`, matching production. Tests that
/// exercise the supervisor's PSK derivation (per_node_psk_rejection,
/// phase5_reconnect, phase_6_13_integration) should use this.
pub async fn start_agent_with_master(node_id: &'static str) -> SocketAddr {
    let psk = derive_test_psk(node_id);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let cfg = AgentConfig::new("127.0.0.1:0".parse().unwrap(), node_id.into(), psk);
    tokio::spawn(async move {
        let _ = kestrel_agent::transport::serve(&cfg, Some(ready_tx)).await;
    });
    ready_rx.await.expect("agent did not bind")
}

// ── Dashboard / HTTP fixture ─────────────────────────────────────────────────

/// Hub-side test setup: temp config dir, empty NodeRegistry, default
/// AppState, axum Router. Returns the router so callers can drive it
/// via `tower::ServiceExt::oneshot`, plus the AppState for direct
/// manipulation.
///
/// The tempdir is intentionally leaked into 'static (`Box::leak`) so
/// the config file stays alive for the duration of the test — axum's
/// service may hold references into it via the AppState. The OS reaps
/// it when the test process exits.
pub fn build_app() -> (axum::Router, AppState) {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path()).to_str().unwrap().to_string();
    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, path, test_master());
    Box::leak(Box::new(dir));
    (router(state.clone()), state)
}

/// Same as [`build_app`] but with a control_token configured, which
/// enables the mutation-endpoint auth checks in `check_auth`. Tests
/// that exercise auth gating need this.
pub fn build_app_with_token(token: &str) -> (axum::Router, AppState) {
    let dir = tempfile::tempdir().unwrap();
    let path = starter_toml(dir.path()).to_str().unwrap().to_string();
    let registry = Arc::new(NodeRegistry::new());
    let state = AppState::new(registry, path, test_master()).with_control_token(token.into());
    Box::leak(Box::new(dir));
    (router(state.clone()), state)
}

/// Mint a valid signed session cookie for the given AppState's
/// signing key. Used by HTTP tests that need to look authenticated
/// without going through /login.
///
/// Returns the cookie value in the same shape a browser would send
/// it back (`kestrel_session=<signed>`).
pub fn cookie_for(state: &AppState) -> String {
    let (set_cookie, _) = kestrel_hub::dashboard::session::set_cookie_header(
        &state.session_key,
        kestrel_hub::dashboard::session::DEFAULT_SESSION_TTL_SECS,
    );
    let value = set_cookie
        .strip_prefix("kestrel_session=")
        .expect("malformed Set-Cookie header from session::set_cookie_header")
        .split(';')
        .next()
        .expect("Set-Cookie has at least the value field");
    format!("kestrel_session={}", value)
}

/// A minimal hub `kestrel.toml` suitable for tests. Writes the file
/// at `dir/kestrel.toml` and returns the path.
pub fn starter_toml(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("kestrel.toml");
    std::fs::write(
        &path,
        r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"
"#,
    )
    .expect("write starter kestrel.toml");
    path
}

#[cfg(test)]
mod self_tests {
    use super::*;

    #[test]
    fn test_psk_is_32_bytes() {
        assert_eq!(test_psk().len(), 32);
    }

    #[test]
    fn test_master_is_32_bytes() {
        assert_eq!(test_master().len(), 32);
    }

    #[test]
    fn derive_test_psk_matches_proto_function() {
        // Pinning: the helper MUST agree with kestrel_proto's
        // derivation, otherwise tests that use derive_test_psk
        // against a supervisor would silently fail to authenticate.
        let direct = kestrel_proto::derive_per_node_psk(&test_master(), "alpha");
        let via_helper = derive_test_psk("alpha");
        assert_eq!(&direct[..], &via_helper[..]);
    }

    #[test]
    fn build_app_returns_router_and_state() {
        let (_router, state) = build_app();
        // AppState should have the test_master as its master_secret.
        // (We can't easily get the master_secret back out — it's
        // Zeroizing'd — so settle for a basic non-panic smoke test.)
        let _ = state.config_path;
    }

    #[tokio::test]
    async fn cookie_for_returns_kestrel_session_header() {
        let (_router, state) = build_app();
        let c = cookie_for(&state);
        assert!(c.starts_with("kestrel_session="));
        // Should contain the dot-separated signed format.
        assert!(c.contains('.'));
    }
}
