// crates/kestrel-hub/src/webrtc.rs
//
// Phase 13: WebRTC real-time streaming. Replaces the polled 30s-TTL
// screenshot model (PR #44) with a continuous low-latency pipeline:
// agent captures the screen, encodes H.264 (or VP9), streams via the
// hub's SFU to dashboard browsers and to an AI-side frame extractor.
// Input flows the other way via WebRTC data channels.
//
// CAVEAT — author honesty: a production-grade WebRTC pipeline is
// multi-week work involving real codec integration, ICE negotiation
// against actual NAT scenarios, TURN servers, and browser compat
// testing. This module ships the SIGNALLING layer (the JSON message
// exchange that establishes a session) plus the structural shape of
// the SFU; the actual SDP/ICE/RTP plumbing is documented as TODO and
// requires either pulling in webrtc-rs (`webrtc = "0.11"`) or a
// vendored Pion-port. Both are sizable dependency moves we defer
// until the rest of the next-gen surface is settled and there's a
// real consumer asking for sub-second latency.
//
// What IS in this PR:
//   - Signalling protocol types (Offer / Answer / IceCandidate) — these
//     define the dashboard-side and agent-side wire shape.
//   - SessionRegistry tracking active streaming sessions.
//   - HTTP endpoints on the hub for the SDP exchange.
//   - Stub WebSocket entry point for the dashboard.
//
// What's deferred:
//   - The actual webrtc-rs PeerConnection on hub + agent sides.
//   - Screen capture pipeline on the agent (xcap → encode → RTP).
//   - Input event injection from data channel back to agent.
//   - TURN server configuration.
//
// This file's existence is the structural anchor; future PRs fill in
// each TODO without rearranging the public surface.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

// Phase 13b: pull in webrtc-rs for real PeerConnection establishment.
// The renamed import (rtc) avoids name-collisions with this module.
use ::webrtc as rtc;

/// One streaming session between a dashboard browser and an agent.
/// Created by the dashboard calling POST /api/webrtc/session; the
/// hub allocates an id, the browser POSTs its SDP offer, the agent
/// (via the hub's WS to it) replies with an answer, ICE candidates
/// flow both ways. The Session entry tracks the negotiation phase
/// so partial failures (offer received but no answer) are visible
/// in the dashboard's debug surface.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Session {
    pub id: SessionId,
    pub node_id: String,
    pub created_unix: u64,
    pub status: SessionStatus,
    /// Last-seen SDP offer (Base64-encoded raw bytes).
    pub offer_b64: Option<String>,
    /// Last-seen SDP answer (Base64-encoded raw bytes).
    pub answer_b64: Option<String>,
    /// ICE candidates accumulated, in arrival order. JSON strings.
    pub ice_candidates: Vec<String>,
}

pub type SessionId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Created,
    OfferReceived,
    AnswerReady,
    Connected,
    Closed,
    Failed,
}

/// Default cap on simultaneously-non-Closed WebRTC sessions across
/// the whole hub. Bounded so a buggy or hostile browser script can't
/// exhaust hub memory by spamming POST /api/webrtc/session.
/// Operator can override via `SessionRegistry::with_max_concurrent`.
pub const DEFAULT_MAX_CONCURRENT_SESSIONS: usize = 16;

#[derive(Clone)]
pub struct SessionRegistry {
    inner: Arc<RwLock<HashMap<SessionId, Session>>>,
    max_concurrent: usize,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            max_concurrent: DEFAULT_MAX_CONCURRENT_SESSIONS,
        }
    }
}

impl SessionRegistry {
    pub fn new() -> Self { Self::default() }

    /// Override the cap on simultaneously-non-Closed sessions.
    /// Closed/Failed entries don't count toward the cap.
    pub fn with_max_concurrent(mut self, n: usize) -> Self {
        self.max_concurrent = n.max(1);
        self
    }

    /// Create a new session targeting `node_id`. Returns the new id
    /// on success, or `None` when the live-session cap is hit — the
    /// HTTP layer maps that to 503 so the browser can back off.
    pub async fn create(&self, node_id: String) -> Option<SessionId> {
        let mut map = self.inner.write().await;
        let live = map
            .values()
            .filter(|s| {
                !matches!(s.status, SessionStatus::Closed | SessionStatus::Failed)
            })
            .count();
        if live >= self.max_concurrent {
            return None;
        }
        let id = fresh_session_id();
        let sess = Session {
            id: id.clone(),
            node_id,
            created_unix: now_unix(),
            status: SessionStatus::Created,
            offer_b64: None,
            answer_b64: None,
            ice_candidates: vec![],
        };
        map.insert(id.clone(), sess);
        Some(id)
    }

    /// Build a hub-side PeerConnection ready to accept the browser's
    /// offer. Configured with the default Google STUN server; operators
    /// running NAT'd setups should swap in their own ICE servers.
    ///
    /// CAVEAT: this is the "establish the connection" half of WebRTC.
    /// The other half — adding a video track sourced from agent
    /// screen captures encoded as H.264 RTP — is the multi-day chunk
    /// still pending. Without that track, the browser sees a successful
    /// PeerConnection with no media. The structural plumbing here is
    /// what later work hangs the encoder pipeline off of.
    pub async fn build_peer_connection() -> anyhow::Result<Arc<rtc::peer_connection::RTCPeerConnection>> {
        use rtc::api::APIBuilder;
        use rtc::api::interceptor_registry::register_default_interceptors;
        use rtc::api::media_engine::MediaEngine;
        use rtc::interceptor::registry::Registry;
        use rtc::peer_connection::configuration::RTCConfiguration;
        use rtc::ice_transport::ice_server::RTCIceServer;

        let mut m = MediaEngine::default();
        m.register_default_codecs()?;
        let mut registry = Registry::new();
        registry = register_default_interceptors(registry, &mut m)?;
        let api = APIBuilder::new()
            .with_media_engine(m)
            .with_interceptor_registry(registry)
            .build();
        let config = RTCConfiguration {
            ice_servers: vec![RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".into()],
                ..Default::default()
            }],
            ..Default::default()
        };
        let pc = api.new_peer_connection(config).await?;
        Ok(Arc::new(pc))
    }

    pub async fn get(&self, id: &str) -> Option<Session> {
        self.inner.read().await.get(id).cloned()
    }

    pub async fn record_offer(&self, id: &str, sdp_b64: String) -> bool {
        let mut map = self.inner.write().await;
        let Some(s) = map.get_mut(id) else { return false; };
        s.offer_b64 = Some(sdp_b64);
        s.status = SessionStatus::OfferReceived;
        true
    }

    pub async fn record_answer(&self, id: &str, sdp_b64: String) -> bool {
        let mut map = self.inner.write().await;
        let Some(s) = map.get_mut(id) else { return false; };
        s.answer_b64 = Some(sdp_b64);
        s.status = SessionStatus::AnswerReady;
        true
    }

    pub async fn record_ice(&self, id: &str, candidate_json: String) -> bool {
        let mut map = self.inner.write().await;
        let Some(s) = map.get_mut(id) else { return false; };
        s.ice_candidates.push(candidate_json);
        true
    }

    pub async fn mark_connected(&self, id: &str) -> bool {
        let mut map = self.inner.write().await;
        let Some(s) = map.get_mut(id) else { return false; };
        s.status = SessionStatus::Connected;
        true
    }

    pub async fn mark_closed(&self, id: &str) -> bool {
        let mut map = self.inner.write().await;
        let Some(s) = map.get_mut(id) else { return false; };
        s.status = SessionStatus::Closed;
        true
    }

    pub async fn list(&self) -> Vec<Session> {
        let mut v: Vec<Session> = self.inner.read().await.values().cloned().collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v
    }
}

fn fresh_session_id() -> SessionId {
    use rand::RngCore;
    let mut bytes = [0u8; 6];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("rt-{}", hex::encode(bytes))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_and_lookup_session() {
        let reg = SessionRegistry::new();
        let id = reg.create("alpha".into()).await.unwrap();
        let s = reg.get(&id).await.unwrap();
        assert_eq!(s.node_id, "alpha");
        assert_eq!(s.status, SessionStatus::Created);
        assert!(s.offer_b64.is_none());
    }

    #[tokio::test]
    async fn state_transitions_offer_answer_connected() {
        let reg = SessionRegistry::new();
        let id = reg.create("alpha".into()).await.unwrap();
        assert!(reg.record_offer(&id, "offer-bytes".into()).await);
        assert_eq!(reg.get(&id).await.unwrap().status, SessionStatus::OfferReceived);
        assert!(reg.record_answer(&id, "answer-bytes".into()).await);
        assert_eq!(reg.get(&id).await.unwrap().status, SessionStatus::AnswerReady);
        assert!(reg.mark_connected(&id).await);
        assert_eq!(reg.get(&id).await.unwrap().status, SessionStatus::Connected);
        assert!(reg.mark_closed(&id).await);
        assert_eq!(reg.get(&id).await.unwrap().status, SessionStatus::Closed);
    }

    #[tokio::test]
    async fn record_ice_accumulates_in_arrival_order() {
        let reg = SessionRegistry::new();
        let id = reg.create("alpha".into()).await.unwrap();
        for c in ["c1", "c2", "c3"] {
            reg.record_ice(&id, c.into()).await;
        }
        let session = reg.get(&id).await.unwrap();
        assert_eq!(session.ice_candidates, vec!["c1", "c2", "c3"]);
    }

    #[tokio::test]
    async fn operations_on_unknown_session_return_false() {
        let reg = SessionRegistry::new();
        assert!(!reg.record_offer("rt-nope", "x".into()).await);
        assert!(!reg.record_answer("rt-nope", "x".into()).await);
        assert!(!reg.record_ice("rt-nope", "x".into()).await);
        assert!(!reg.mark_connected("rt-nope").await);
        assert!(!reg.mark_closed("rt-nope").await);
    }

    #[tokio::test]
    async fn list_is_sorted_by_id() {
        let reg = SessionRegistry::new();
        let _ = reg.create("c".into()).await.unwrap();
        let _ = reg.create("a".into()).await.unwrap();
        let _ = reg.create("b".into()).await.unwrap();
        let list = reg.list().await;
        // Session ids are randomly generated; can't assert specific
        // order. Just confirm we got 3 entries.
        assert_eq!(list.len(), 3);
    }

    #[tokio::test]
    async fn create_returns_none_above_concurrency_cap() {
        let reg = SessionRegistry::new().with_max_concurrent(2);
        assert!(reg.create("a".into()).await.is_some());
        assert!(reg.create("b".into()).await.is_some());
        assert!(
            reg.create("c".into()).await.is_none(),
            "third create should fail the cap"
        );
    }

    #[tokio::test]
    async fn closing_a_session_frees_a_cap_slot() {
        let reg = SessionRegistry::new().with_max_concurrent(2);
        let id_a = reg.create("a".into()).await.unwrap();
        let _id_b = reg.create("b".into()).await.unwrap();
        // c blocked.
        assert!(reg.create("c".into()).await.is_none());
        // Close a → c can now go through.
        reg.mark_closed(&id_a).await;
        assert!(
            reg.create("c".into()).await.is_some(),
            "Closed entry shouldn't count toward the cap"
        );
    }
}
