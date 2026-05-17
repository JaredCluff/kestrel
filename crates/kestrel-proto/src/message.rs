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
        let msg = KestrelMessage { stream_id: 1, kind: MsgKind::Request, payload: Payload::Ping };
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded, msg);
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
        assert_eq!(decoded, msg);
    }

    #[test]
    fn roundtrip_auth_challenge() {
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Request,
            payload: Payload::Challenge { nonce: [0xABu8; 32] },
        };
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn roundtrip_auth_response() {
        let msg = KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Response,
            payload: Payload::AuthResponse {
                mac: [0xDEu8; 32],
                node_id: "hub".into(),
            },
        };
        let bytes = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
        let (decoded, _): (KestrelMessage, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded, msg);
    }
}
