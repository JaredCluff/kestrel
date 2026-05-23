// crates/kestrel-hub/src/events.rs
use kestrel_proto::{OsInfo, WorldState};
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone)]
pub enum NodeEvent {
    Connected { node_id: String, os: OsInfo },
    Disconnected { node_id: String, attempt: u32, next_retry_in: Duration },
    Reconnecting { node_id: String, attempt: u32 },
    /// Phase 6: agent's WorldObserver reported a state change. The
    /// new state is included so SSE subscribers can react without
    /// re-querying the registry.
    WorldChanged { node_id: String, state: WorldState },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState { Online, Offline, Reconnecting }

#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub node_id: String,
    pub state: NodeState,
    pub os: Option<OsInfo>,
    pub latency_ms: Option<u32>,
    pub last_seen: SystemTime,
    pub next_retry_in: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_event_constructs() {
        let evt = NodeEvent::Connected {
            node_id: "a".into(),
            os: OsInfo { name: "macos".into(), version: "26".into() },
        };
        assert!(matches!(evt, NodeEvent::Connected { .. }));
    }

    #[test]
    fn node_status_constructs() {
        let s = NodeStatus {
            node_id: "a".into(),
            state: NodeState::Online,
            os: None,
            latency_ms: None,
            last_seen: SystemTime::now(),
            next_retry_in: None,
        };
        assert_eq!(s.state, NodeState::Online);
    }
}
