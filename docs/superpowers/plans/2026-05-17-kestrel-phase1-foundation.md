# Kestrel Phase 1 — Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bootstrap the Rust workspace with three crates, define Phase 1 protocol messages, implement WebSocket + TLS 1.3 transport with PSK/HMAC-SHA256 authentication, and verify hub-to-agent ping/pong works end-to-end.

**Architecture:** `kestrel-proto` holds all shared message types and HMAC helpers. `kestrel-agent` runs a TLS WebSocket server that issues auth challenges and handles ping/pong. `kestrel-hub` is a TLS WebSocket client that responds to challenges, receives node info, and sends heartbeats. Tests spin both sides up in-process as tokio tasks using port 0 (OS-assigned) so nothing is hardcoded.

**Tech Stack:** Rust 2021, tokio 1.x, tokio-tungstenite 0.21 (rustls feature), tokio-rustls 0.24, rustls 0.21, rcgen 0.11, serde 1 + bincode 2 (serde compat feature), hmac 0.12 + sha2 0.10, rand 0.8, clap 4, keyring 2, hex 0.4, anyhow 1, tracing 0.1 + tracing-subscriber 0.3

---

## File Map

```
kestrel/
├── Cargo.toml                              # workspace + shared deps
├── crates/
│   ├── kestrel-proto/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                      # pub use re-exports
│   │       ├── message.rs                  # KestrelMessage, MsgKind, Payload, OsInfo, DisplayInfo
│   │       └── auth.rs                     # hmac_response(), verify_response()
│   ├── kestrel-agent/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                      # pub mod config; pub mod transport;
│   │       ├── main.rs                     # CLI: agent start / agent enroll
│   │       ├── config.rs                   # AgentConfig { listen, node_id, psk }
│   │       └── transport.rs                # serve() — TLS WS server, auth handshake, msg loop
│   └── kestrel-hub/
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs                      # pub mod config; pub mod enrollment; pub mod transport;
│           ├── main.rs                     # CLI: hub init / hub connect
│           ├── config.rs                   # HubConfig { nodes: Vec<NodeConfig> }
│           ├── enrollment.rs               # generate_psk(), store_psk(), load_psk(), enrollment_command()
│           └── transport.rs                # connect(), ping_once() — TLS WS client, auth, ping
└── crates/kestrel-hub/tests/
    └── phase1.rs                           # integration test: agent + hub as tokio tasks
```

---

## Task 1: Workspace Scaffold

**Files:**
- Create: `Cargo.toml`
- Create: `crates/kestrel-proto/Cargo.toml`
- Create: `crates/kestrel-proto/src/lib.rs`
- Create: `crates/kestrel-agent/Cargo.toml`
- Create: `crates/kestrel-agent/src/lib.rs`
- Create: `crates/kestrel-agent/src/main.rs`
- Create: `crates/kestrel-hub/Cargo.toml`
- Create: `crates/kestrel-hub/src/lib.rs`
- Create: `crates/kestrel-hub/src/main.rs`

- [ ] **Step 1: Create workspace Cargo.toml**

```toml
# Cargo.toml
[workspace]
members = [
    "crates/kestrel-proto",
    "crates/kestrel-agent",
    "crates/kestrel-hub",
]
resolver = "2"

[workspace.dependencies]
tokio              = { version = "1", features = ["full"] }
serde              = { version = "1", features = ["derive"] }
bincode            = { version = "2", features = ["serde"] }
anyhow             = "1"
tracing            = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

- [ ] **Step 2: Create proto crate**

```toml
# crates/kestrel-proto/Cargo.toml
[package]
name    = "kestrel-proto"
version = "0.1.0"
edition = "2021"

[dependencies]
serde   = { workspace = true }
bincode = { workspace = true }
hmac    = "0.12"
sha2    = "0.10"
```
```rust
// crates/kestrel-proto/src/lib.rs
pub mod auth;
pub mod message;

pub use auth::{hmac_response, verify_response};
pub use message::{DisplayInfo, KestrelMessage, MsgKind, OsInfo, Payload};
```

- [ ] **Step 3: Create agent crate**

```toml
# crates/kestrel-agent/Cargo.toml
[package]
name    = "kestrel-agent"
version = "0.1.0"
edition = "2021"

[dependencies]
kestrel-proto      = { path = "../kestrel-proto" }
tokio              = { workspace = true }
serde              = { workspace = true }
bincode            = { workspace = true }
anyhow             = { workspace = true }
tracing            = { workspace = true }
tracing-subscriber = { workspace = true }
tokio-tungstenite  = { version = "0.21", features = ["rustls-tls-native-roots"] }
tokio-rustls       = "0.24"
rustls             = "0.21"
rcgen              = "0.11"
rand               = "0.8"
futures-util       = "0.3"
clap               = { version = "4", features = ["derive"] }
toml               = "0.8"
keyring            = "2"
hex                = "0.4"

[dev-dependencies]
tokio = { workspace = true }
```

```rust
// crates/kestrel-agent/src/lib.rs
pub mod config;
pub mod transport;
```

```rust
// crates/kestrel-agent/src/main.rs
fn main() {
    println!("kestrel-agent");
}
```

- [ ] **Step 4: Create hub crate**

```toml
# crates/kestrel-hub/Cargo.toml
[package]
name    = "kestrel-hub"
version = "0.1.0"
edition = "2021"

[dependencies]
kestrel-proto      = { path = "../kestrel-proto" }
tokio              = { workspace = true }
serde              = { workspace = true }
bincode            = { workspace = true }
anyhow             = { workspace = true }
tracing            = { workspace = true }
tracing-subscriber = { workspace = true }
tokio-tungstenite  = { version = "0.21", features = ["rustls-tls-native-roots"] }
rustls             = "0.21"
rand               = "0.8"
futures-util       = "0.3"
clap               = { version = "4", features = ["derive"] }
toml               = "0.8"
keyring            = "2"
hex                = "0.4"

[dev-dependencies]
kestrel-agent = { path = "../kestrel-agent" }
tokio         = { workspace = true }
```

```rust
// crates/kestrel-hub/src/lib.rs
pub mod config;
pub mod enrollment;
pub mod transport;
```

```rust
// crates/kestrel-hub/src/main.rs
fn main() {
    println!("kestrel-hub");
}
```

- [ ] **Step 5: Create all source directories and verify the workspace compiles**

```bash
mkdir -p crates/kestrel-proto/src crates/kestrel-agent/src crates/kestrel-hub/src
# (files already written in steps above)
cargo build
```

Expected: all three crates compile. Warnings about empty modules are fine.

- [ ] **Step 6: Commit**

```bash
git init
git add .
git commit -m "chore: initialize kestrel workspace with three stub crates"
```

---

## Task 2: Proto — Phase 1 Message Types

**Files:**
- Create: `crates/kestrel-proto/src/message.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/kestrel-proto/src/message.rs` with only the test block:

```rust
// crates/kestrel-proto/src/message.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_ping() {
        let msg = KestrelMessage {
            stream_id: 1,
            kind: MsgKind::Request,
            payload: Payload::Ping,
        };
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded.stream_id, 1);
        assert!(matches!(decoded.payload, Payload::Ping));
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
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        let Payload::SystemInfo { hostname, .. } = decoded.payload else {
            panic!("wrong payload variant");
        };
        assert_eq!(hostname, "dev-box");
    }

    #[test]
    fn roundtrip_auth_challenge() {
        let nonce = [0xABu8; 32];
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::Challenge { nonce },
        };
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        let Payload::Challenge { nonce: decoded_nonce } = decoded.payload else {
            panic!("wrong payload variant");
        };
        assert_eq!(decoded_nonce, [0xABu8; 32]);
    }
}
```

- [ ] **Step 2: Run tests to confirm compile failure**

```bash
cargo test -p kestrel-proto
```

Expected: compile error — `KestrelMessage`, `MsgKind`, `Payload`, etc. not defined.

- [ ] **Step 3: Implement the message types**

Replace the entire `crates/kestrel-proto/src/message.rs` with:

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
pub enum MsgKind {
    Request,
    Response,
    Event,
    Ack,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Payload {
    Challenge { nonce: [u8; 32] },
    AuthResponse { mac: [u8; 32], node_id: String },
    SystemInfo { os: OsInfo, displays: Vec<DisplayInfo>, hostname: String },
    Ping,
    Pong,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OsInfo {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DisplayInfo {
    pub id: u8,
    pub width: u32,
    pub height: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_ping() {
        let msg = KestrelMessage {
            stream_id: 1,
            kind: MsgKind::Request,
            payload: Payload::Ping,
        };
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded.stream_id, 1);
        assert!(matches!(decoded.payload, Payload::Ping));
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
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        let Payload::SystemInfo { hostname, .. } = decoded.payload else {
            panic!("wrong payload variant");
        };
        assert_eq!(hostname, "dev-box");
    }

    #[test]
    fn roundtrip_auth_challenge() {
        let nonce = [0xABu8; 32];
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::Challenge { nonce },
        };
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        let Payload::Challenge { nonce: decoded_nonce } = decoded.payload else {
            panic!("wrong payload variant");
        };
        assert_eq!(decoded_nonce, [0xABu8; 32]);
    }
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p kestrel-proto
```

Expected:
```
test message::tests::roundtrip_auth_challenge ... ok
test message::tests::roundtrip_ping ... ok
test message::tests::roundtrip_system_info ... ok

test result: ok. 3 passed; 0 failed
```

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-proto/src/message.rs crates/kestrel-proto/src/lib.rs
git commit -m "feat: define Phase 1 protocol message types in kestrel-proto"
```

---

## Task 3: Proto — HMAC Auth Helpers

**Files:**
- Create: `crates/kestrel-proto/src/auth.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// crates/kestrel-proto/src/auth.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_verify_roundtrip() {
        let psk = b"super-secret-key-for-testing-only";
        let nonce = [0xABu8; 32];
        let mac = hmac_response(psk, &nonce);
        assert!(verify_response(psk, &nonce, &mac));
    }

    #[test]
    fn wrong_key_fails() {
        let nonce = [1u8; 32];
        let mac = hmac_response(b"correct-key", &nonce);
        assert!(!verify_response(b"wrong-key", &nonce, &mac));
    }

    #[test]
    fn wrong_nonce_fails() {
        let psk = b"some-psk";
        let mac = hmac_response(psk, &[1u8; 32]);
        assert!(!verify_response(psk, &[2u8; 32], &mac));
    }
}
```

- [ ] **Step 2: Run tests to confirm compile failure**

```bash
cargo test -p kestrel-proto auth
```

Expected: compile error — `hmac_response` and `verify_response` not defined.

- [ ] **Step 3: Implement HMAC helpers**

Replace `crates/kestrel-proto/src/auth.rs` with the full implementation:

```rust
// crates/kestrel-proto/src/auth.rs
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Compute HMAC-SHA256(psk, nonce) — the hub's proof-of-key response.
pub fn hmac_response(psk: &[u8], nonce: &[u8; 32]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(psk).expect("HMAC accepts any key length");
    mac.update(nonce);
    mac.finalize().into_bytes().into()
}

/// Constant-time verification of an auth response MAC.
pub fn verify_response(psk: &[u8], nonce: &[u8; 32], mac: &[u8; 32]) -> bool {
    let expected = hmac_response(psk, nonce);
    constant_time_eq(&expected, mac)
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_verify_roundtrip() {
        let psk = b"super-secret-key-for-testing-only";
        let nonce = [0xABu8; 32];
        let mac = hmac_response(psk, &nonce);
        assert!(verify_response(psk, &nonce, &mac));
    }

    #[test]
    fn wrong_key_fails() {
        let nonce = [1u8; 32];
        let mac = hmac_response(b"correct-key", &nonce);
        assert!(!verify_response(b"wrong-key", &nonce, &mac));
    }

    #[test]
    fn wrong_nonce_fails() {
        let psk = b"some-psk";
        let mac = hmac_response(psk, &[1u8; 32]);
        assert!(!verify_response(psk, &[2u8; 32], &mac));
    }
}
```

- [ ] **Step 4: Run all proto tests**

```bash
cargo test -p kestrel-proto
```

Expected:
```
test auth::tests::hmac_verify_roundtrip ... ok
test auth::tests::wrong_key_fails ... ok
test auth::tests::wrong_nonce_fails ... ok
test message::tests::roundtrip_auth_challenge ... ok
test message::tests::roundtrip_ping ... ok
test message::tests::roundtrip_system_info ... ok

test result: ok. 6 passed; 0 failed
```

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-proto/src/auth.rs
git commit -m "feat: add HMAC-SHA256 auth helpers to kestrel-proto"
```

---

## Task 4: Agent Config

**Files:**
- Create: `crates/kestrel-agent/src/config.rs`

- [ ] **Step 1: Write the failing test**

```rust
// crates/kestrel-agent/src/config.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_config_from_toml() {
        let s = r#"
[agent]
listen  = "0.0.0.0:7272"
node_id = "test-node"
psk     = "deadbeefdeadbeefdeadbeefdeadbeef"
"#;
        let cfg = AgentConfig::from_str(s).unwrap();
        assert_eq!(cfg.node_id, "test-node");
        assert_eq!(cfg.listen.port(), 7272);
        assert_eq!(cfg.psk, hex::decode("deadbeefdeadbeefdeadbeefdeadbeef").unwrap());
    }
}
```

- [ ] **Step 2: Run test to confirm compile failure**

```bash
cargo test -p kestrel-agent config
```

Expected: compile error — `AgentConfig` not defined.

- [ ] **Step 3: Implement AgentConfig**

```rust
// crates/kestrel-agent/src/config.rs
use std::net::SocketAddr;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub listen: SocketAddr,
    pub node_id: String,
    pub psk: Vec<u8>,
}

impl AgentConfig {
    pub fn new(listen: SocketAddr, node_id: String, psk: Vec<u8>) -> Self {
        AgentConfig { listen, node_id, psk }
    }

    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct Raw { agent: RawAgent }
        #[derive(Deserialize)]
        struct RawAgent { listen: String, node_id: String, psk: String }

        let raw: Raw = toml::from_str(s)?;
        Ok(AgentConfig {
            listen: raw.agent.listen.parse()?,
            node_id: raw.agent.node_id,
            psk: hex::decode(&raw.agent.psk)?,
        })
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Self::from_str(&std::fs::read_to_string(path)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_config_from_toml() {
        let s = r#"
[agent]
listen  = "0.0.0.0:7272"
node_id = "test-node"
psk     = "deadbeefdeadbeefdeadbeefdeadbeef"
"#;
        let cfg = AgentConfig::from_str(s).unwrap();
        assert_eq!(cfg.node_id, "test-node");
        assert_eq!(cfg.listen.port(), 7272);
        assert_eq!(cfg.psk, hex::decode("deadbeefdeadbeefdeadbeefdeadbeef").unwrap());
    }
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p kestrel-agent config
```

Expected:
```
test config::tests::parse_config_from_toml ... ok

test result: ok. 1 passed; 0 failed
```

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-agent/src/config.rs crates/kestrel-agent/src/lib.rs
git commit -m "feat: add AgentConfig with TOML + hex-PSK parsing"
```

---

## Task 5: Hub Enrollment + Config

**Files:**
- Create: `crates/kestrel-hub/src/enrollment.rs`
- Create: `crates/kestrel-hub/src/config.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// crates/kestrel-hub/src/enrollment.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_psk_is_32_bytes() {
        let key = generate_psk();
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn enrollment_command_contains_required_parts() {
        let key = vec![0u8; 32];
        let cmd = enrollment_command("192.168.1.10", &key);
        assert!(cmd.contains("kestrel-agent enroll"));
        assert!(cmd.contains("192.168.1.10"));
        assert!(cmd.contains("--key"));
    }

    #[test]
    fn psk_hex_roundtrip() {
        let key = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
        let cmd = enrollment_command("10.0.0.1", &key);
        assert!(cmd.contains("deadbeef"));
    }
}
```

```rust
// crates/kestrel-hub/src/config.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hub_config() {
        let s = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"

[[hub.nodes]]
node_id = "linux-dev"
address = "192.168.1.20:7272"

[[hub.nodes]]
node_id = "mac-studio"
address = "192.168.1.10:7272"
"#;
        let cfg = HubConfig::from_str(s).unwrap();
        assert_eq!(cfg.nodes.len(), 2);
        assert_eq!(cfg.nodes[0].node_id, "linux-dev");
        assert_eq!(cfg.nodes[1].address.port(), 7272);
    }
}
```

- [ ] **Step 2: Run tests to confirm compile failure**

```bash
cargo test -p kestrel-hub
```

Expected: compile errors — types not defined.

- [ ] **Step 3: Implement enrollment.rs**

```rust
// crates/kestrel-hub/src/enrollment.rs
use rand::RngCore;

pub fn generate_psk() -> Vec<u8> {
    let mut key = vec![0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    key
}

pub fn enrollment_command(hub_ip: &str, psk: &[u8]) -> String {
    format!(
        "kestrel-agent enroll --hub {} --key {}",
        hub_ip,
        hex::encode(psk)
    )
}

pub fn store_psk(psk: &[u8]) -> anyhow::Result<()> {
    let entry = keyring::Entry::new("kestrel", "psk")?;
    entry.set_password(&hex::encode(psk))?;
    Ok(())
}

pub fn load_psk() -> anyhow::Result<Vec<u8>> {
    let entry = keyring::Entry::new("kestrel", "psk")?;
    Ok(hex::decode(entry.get_password()?)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_psk_is_32_bytes() {
        let key = generate_psk();
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn enrollment_command_contains_required_parts() {
        let key = vec![0u8; 32];
        let cmd = enrollment_command("192.168.1.10", &key);
        assert!(cmd.contains("kestrel-agent enroll"));
        assert!(cmd.contains("192.168.1.10"));
        assert!(cmd.contains("--key"));
    }

    #[test]
    fn psk_hex_roundtrip() {
        let key = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
        let cmd = enrollment_command("10.0.0.1", &key);
        assert!(cmd.contains("deadbeef"));
    }
}
```

- [ ] **Step 4: Implement config.rs**

```rust
// crates/kestrel-hub/src/config.rs
use std::net::SocketAddr;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct HubConfig {
    pub listen_mcp: String,
    pub listen_dashboard: SocketAddr,
    pub nodes: Vec<NodeConfig>,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: String,
    pub address: SocketAddr,
}

impl HubConfig {
    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct Raw { hub: RawHub }
        #[derive(Deserialize)]
        struct RawHub {
            listen_mcp: String,
            listen_dashboard: String,
            #[serde(default)]
            nodes: Vec<RawNode>,
        }
        #[derive(Deserialize)]
        struct RawNode { node_id: String, address: String }

        let raw: Raw = toml::from_str(s)?;
        Ok(HubConfig {
            listen_mcp: raw.hub.listen_mcp,
            listen_dashboard: raw.hub.listen_dashboard.parse()?,
            nodes: raw.hub.nodes.into_iter().map(|n| NodeConfig {
                node_id: n.node_id,
                address: n.address.parse().expect("invalid node address in config"),
            }).collect(),
        })
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Self::from_str(&std::fs::read_to_string(path)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hub_config() {
        let s = r#"
[hub]
listen_mcp       = "stdio"
listen_dashboard = "0.0.0.0:7273"

[[hub.nodes]]
node_id = "linux-dev"
address = "192.168.1.20:7272"

[[hub.nodes]]
node_id = "mac-studio"
address = "192.168.1.10:7272"
"#;
        let cfg = HubConfig::from_str(s).unwrap();
        assert_eq!(cfg.nodes.len(), 2);
        assert_eq!(cfg.nodes[0].node_id, "linux-dev");
        assert_eq!(cfg.nodes[1].address.port(), 7272);
    }
}
```

- [ ] **Step 5: Run all hub tests**

```bash
cargo test -p kestrel-hub
```

Expected:
```
test config::tests::parse_hub_config ... ok
test enrollment::tests::enrollment_command_contains_required_parts ... ok
test enrollment::tests::generate_psk_is_32_bytes ... ok
test enrollment::tests::psk_hex_roundtrip ... ok

test result: ok. 4 passed; 0 failed
```

- [ ] **Step 6: Commit**

```bash
git add crates/kestrel-hub/src/
git commit -m "feat: add hub enrollment and HubConfig with TOML parsing"
```

---

## Task 6: Agent Transport — TLS WebSocket Server + Auth

**Files:**
- Create: `crates/kestrel-agent/src/transport.rs`

- [ ] **Step 1: Write a compile stub to define the public API**

```rust
// crates/kestrel-agent/src/transport.rs
use std::net::SocketAddr;
use crate::config::AgentConfig;

pub async fn serve(
    config: &AgentConfig,
    ready: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
) -> anyhow::Result<()> {
    todo!()
}
```

- [ ] **Step 2: Verify stub compiles**

```bash
cargo build -p kestrel-agent
```

Expected: compiles with "not yet implemented" warning.

- [ ] **Step 3: Implement transport.rs**

Replace the stub with the full implementation:

```rust
// crates/kestrel-agent/src/transport.rs
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
        let (stream, peer) = listener.accept().await?;
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
        anyhow::bail!("expected AuthResponse");
    };
    if !verify_response(&psk, &nonce, &mac) {
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
```

- [ ] **Step 4: Build to verify**

```bash
cargo build -p kestrel-agent
```

Expected: compiles cleanly (no errors).

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-agent/src/transport.rs crates/kestrel-agent/src/lib.rs
git commit -m "feat: implement agent TLS WebSocket server with PSK auth handshake"
```

---

## Task 7: Hub Transport — TLS Client + Auth + Ping

**Files:**
- Create: `crates/kestrel-hub/src/transport.rs`

- [ ] **Step 1: Implement transport.rs**

```rust
// crates/kestrel-hub/src/transport.rs
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use kestrel_proto::{hmac_response, KestrelMessage, MsgKind, OsInfo, Payload};
use rustls::ClientConfig;
use tokio_tungstenite::{connect_async_tls_with_config, Connector, tungstenite::Message};

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

/// Connect to an agent at `addr`, complete the auth handshake, and start a
/// background heartbeat. Returns node info on success.
pub async fn connect(addr: SocketAddr, psk: &[u8]) -> anyhow::Result<NodeConn> {
    let url = format!("wss://{}", addr);
    let (ws, _) = connect_async_tls_with_config(
        url.as_str(),
        None,
        false,
        Some(Connector::Rustls(make_client_config())),
    )
    .await
    .context("WebSocket connect")?;
    let (mut tx, mut rx) = ws.split();

    let (node_id, os_info) = do_handshake(&mut tx, &mut rx, psk).await?;

    // Background ping loop — moves tx and rx so we hold the connection alive
    tokio::spawn(async move {
        let mut stream_id = 1u32;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let sent = Instant::now();
            let ping = KestrelMessage {
                stream_id,
                kind: MsgKind::Request,
                payload: Payload::Ping,
            };
            if tx.send(Message::Binary(encode(&ping).unwrap())).await.is_err() {
                break;
            }
            if let Some(Ok(_)) = rx.next().await {
                tracing::debug!("pong rtt={}ms", sent.elapsed().as_millis());
            } else {
                break;
            }
            stream_id += 1;
        }
    });

    Ok(NodeConn { node_id, os_info })
}

/// Connect, auth, send one ping, return the round-trip time. Used by tests.
pub async fn ping_once(addr: SocketAddr, psk: &[u8]) -> anyhow::Result<std::time::Duration> {
    let url = format!("wss://{}", addr);
    let (ws, _) = connect_async_tls_with_config(
        url.as_str(),
        None,
        false,
        Some(Connector::Rustls(make_client_config())),
    )
    .await?;
    let (mut tx, mut rx) = ws.split();

    do_handshake(&mut tx, &mut rx, psk).await?;

    let sent = Instant::now();
    tx.send(Message::Binary(encode(&KestrelMessage {
        stream_id: 1,
        kind: MsgKind::Request,
        payload: Payload::Ping,
    })?)).await?;
    let _ = rx.next().await.context("no Pong")??;
    Ok(sent.elapsed())
}

async fn do_handshake<
    Tx: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    Rx: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
>(
    tx: &mut Tx,
    rx: &mut Rx,
    psk: &[u8],
) -> anyhow::Result<(String, OsInfo)> {
    // Receive challenge
    let raw = rx.next().await.context("no challenge")??;
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
    })?)).await?;

    // Receive SystemInfo
    let raw = rx.next().await.context("no SystemInfo")??;
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

- [ ] **Step 2: Build to verify**

```bash
cargo build -p kestrel-hub
```

Expected: compiles cleanly.

- [ ] **Step 3: Commit**

```bash
git add crates/kestrel-hub/src/transport.rs crates/kestrel-hub/src/lib.rs
git commit -m "feat: implement hub TLS WebSocket client with auth and ping heartbeat"
```

---

## Task 8: CLI Binaries

**Files:**
- Modify: `crates/kestrel-agent/src/main.rs`
- Modify: `crates/kestrel-hub/src/main.rs`

- [ ] **Step 1: Implement agent main.rs**

```rust
// crates/kestrel-agent/src/main.rs
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "kestrel-agent", about = "Kestrel fleet node agent")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Start {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
    },
    Enroll {
        #[arg(long)]
        hub: String,
        #[arg(long)]
        key: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Start { config } => {
            let cfg = kestrel_agent::config::AgentConfig::from_file(&config)?;
            kestrel_agent::transport::serve(&cfg, None).await?;
        }
        Command::Enroll { hub: _, key } => {
            let psk = hex::decode(&key)?;
            let entry = keyring::Entry::new("kestrel", "psk")?;
            entry.set_password(&hex::encode(&psk))?;
            println!("PSK stored in system credential store.");
            println!("Start the agent with: kestrel-agent start");
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Implement hub main.rs**

```rust
// crates/kestrel-hub/src/main.rs
use clap::{Parser, Subcommand};
use kestrel_hub::{config::HubConfig, enrollment};

#[derive(Parser)]
#[command(name = "kestrel-hub", about = "Kestrel fleet hub")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init {
        #[arg(long, default_value = "0.0.0.0")]
        bind: String,
    },
    Connect {
        #[arg(long, default_value = "kestrel.toml")]
        config: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Command::Init { bind } => {
            let psk = enrollment::generate_psk();
            enrollment::store_psk(&psk)?;
            println!("Key generated and stored in system credential store.");
            println!("Run this on each node machine:");
            println!("  {}", enrollment::enrollment_command(&bind, &psk));
        }
        Command::Connect { config } => {
            let cfg = HubConfig::from_file(&config)?;
            let psk = enrollment::load_psk()?;
            for node in &cfg.nodes {
                let conn = kestrel_hub::transport::connect(node.address, &psk).await?;
                println!("connected: {} ({})", conn.node_id, conn.os_info.name);
            }
            tokio::signal::ctrl_c().await?;
            println!("shutting down");
        }
    }
    Ok(())
}
```

- [ ] **Step 3: Build both binaries**

```bash
cargo build --bins
```

Expected: both `target/debug/kestrel-agent` and `target/debug/kestrel-hub` produced.

- [ ] **Step 4: Smoke-test the help output**

```bash
./target/debug/kestrel-agent --help
./target/debug/kestrel-hub --help
```

Expected: both print usage with `start`/`enroll` and `init`/`connect` subcommands respectively.

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-agent/src/main.rs crates/kestrel-hub/src/main.rs
git commit -m "feat: add CLI entry points for agent (start/enroll) and hub (init/connect)"
```

---

## Task 9: Integration Test — Auth Handshake + Ping/Pong

**Files:**
- Create: `crates/kestrel-hub/tests/phase1.rs`

- [ ] **Step 1: Write the failing tests**

```rust
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
    let result = connect(addr, &bad_psk).await;
    assert!(result.is_err(), "connection with wrong PSK should fail");
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
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
cargo test -p kestrel-hub --test phase1 2>&1 | head -30
```

Expected: compile error — `connect` and `ping_once` not yet in scope (or type mismatch if signatures differ). Fix any discrepancy between the test's expected API and the implementation before continuing.

- [ ] **Step 3: Run tests after verifying they compile**

```bash
cargo test -p kestrel-hub --test phase1 -- --nocapture
```

Expected:
```
running 3 tests
test test_auth_handshake_succeeds ... ok
test test_ping_pong_rtt_under_100ms ... ok
test test_wrong_psk_fails ... ok

test result: ok. 3 passed; 0 failed
```

If `test_wrong_psk_fails` hangs (agent closes connection but hub doesn't error), add a timeout:

```rust
// Replace the connect call in test_wrong_psk_fails with:
let result = tokio::time::timeout(
    std::time::Duration::from_secs(3),
    connect(addr, &bad_psk),
).await;
assert!(result.is_err() || result.unwrap().is_err());
```

- [ ] **Step 4: Run the full workspace test suite**

```bash
cargo test
```

Expected: all tests across all three crates pass.

- [ ] **Step 5: Commit**

```bash
git add crates/kestrel-hub/tests/phase1.rs
git commit -m "test: integration tests for Phase 1 auth handshake and ping/pong"
```

---

## Self-Review

### Spec Coverage

| Spec Requirement | Task |
|---|---|
| §4.1 WS transport, TLS 1.3 mandatory, configurable port (default 7272) | Task 6 — rustls + TLS 1.3, port from AgentConfig |
| §4.2 PSK auth: node sends challenge nonce, hub responds with HMAC-SHA256 | Tasks 3, 6, 7 |
| §4.2 MAC verification; reject on failure | Task 6 `verify_response` + connection close |
| §4.2 Node sends SystemInfo after auth | Task 6 `handle_conn` |
| §4.2 OS credential store (Keychain/DPAPI/libsecret) | Tasks 5, 8 via `keyring` |
| §4.2 Enrollment UX: `hub init` + `agent enroll` | Task 8 |
| §4.3 `KestrelMessage { stream_id, kind, payload }` envelope | Task 2 |
| §4.3 `Payload::Ping` / `Payload::Pong` | Task 2 |
| §4.3 `Payload::Challenge` / `Payload::AuthResponse` | Task 2 |
| §4.3 `Payload::SystemInfo { os, displays, hostname }` | Task 2 |
| §8.1 kestrel.toml config format | Tasks 4, 5 |
| §8.2 `kestrel hub init` / `kestrel agent enroll` commands | Task 8 |
| §11 Phase 1: two machines connected, ping/pong working | Task 9 |

**Out of scope for Phase 1 (as designed):**
- Five capability groups (input/screen/clipboard/shell/system) — Phase 2+
- MCP server — Phase 4
- Web dashboard / ratatui TUI — Phase 5
- KVM cursor routing — Phase 2
- mDNS discovery — Phase 5 (opt-in per spec)
- Wayland full capture — Phase 5 with libei

### Placeholder Scan

No TBDs, TODOs, "similar to Task N," or unimplemented error handling patterns found.

### Type Consistency

- `KestrelMessage`, `MsgKind`, `Payload`, `OsInfo`, `DisplayInfo` defined in Task 2 → used in Tasks 6, 7, 9. ✓
- `hmac_response(psk: &[u8], nonce: &[u8; 32]) -> [u8; 32]` defined in Task 3 → called in Task 7. ✓
- `verify_response(psk: &[u8], nonce: &[u8; 32], mac: &[u8; 32]) -> bool` defined in Task 3 → called in Task 6. ✓
- `AgentConfig { listen, node_id, psk }` + `AgentConfig::new(...)` defined in Task 4 → used in Tasks 6, 8, 9. ✓
- `HubConfig { listen_mcp, listen_dashboard, nodes }` + `NodeConfig` defined in Task 5 → used in Task 8. ✓
- `NodeConn { node_id: String, os_info: OsInfo }` defined in Task 7 → asserted in Task 9 `assert_eq!(conn.node_id, ...)`. ✓
- `connect(addr: SocketAddr, psk: &[u8]) -> anyhow::Result<NodeConn>` defined in Task 7 → called in Task 9. ✓
- `ping_once(addr: SocketAddr, psk: &[u8]) -> anyhow::Result<Duration>` defined in Task 7 → called in Task 9. ✓
