// crates/kestrel-hub/tests/phase_6_13_integration.rs
//
// End-to-end integration tests for Phases 6-13 against a real agent.
// Verifies the wire shapes + registry state transitions without
// relying on a connected human-operated machine.

use std::net::SocketAddr;
use std::time::Duration;

use kestrel_agent::config::AgentConfig;
use kestrel_hub::router::{CapabilityNeeds, NodeRegistry};
use kestrel_hub::transport::connect_with_world_sink;
use std::sync::Arc;

fn test_psk() -> Vec<u8> {
    b"kestrel-test-psk-32bytes-padded!".to_vec()
}

async fn start_agent(node_id: &str) -> SocketAddr {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let cfg = AgentConfig::new("127.0.0.1:0".parse().unwrap(), node_id.into(), test_psk());
    tokio::spawn(async move {
        let _ = kestrel_agent::transport::serve(&cfg, Some(ready_tx)).await;
    });
    ready_rx.await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase8_capabilities_flow_through_handshake() {
    // After a successful handshake, the agent's reported capabilities
    // are captured in the NodeHandle and visible via the registry.
    let addr = start_agent("cap-node").await;
    let (handle, _actor, _world_rx, _caps_rx, _webrtc_rx) =
        connect_with_world_sink(addr, &test_psk()).await.unwrap();
    let caps = handle.capabilities.expect("handshake must capture caps");
    assert_eq!(caps.os, std::env::consts::OS);
    // Display flag mirrors what list_displays() saw at agent start.
    // On a headless CI it might be false; just sanity-check it's a
    // bool that didn't panic.
    let _ = caps.has_display;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase8_find_nodes_with_filter() {
    // Hand-populated registry: matching predicate returns matching
    // node_ids; non-matching predicate returns empty.
    let reg = NodeRegistry::new();
    reg.record_capabilities(
        "linux-gpu",
        kestrel_proto::Capabilities {
            os: "linux".into(),
            has_gpu: true,
            has_display: true,
            has_sudo: false,
            has_docker: true,
        },
    )
    .await;
    reg.record_capabilities(
        "macbook",
        kestrel_proto::Capabilities {
            os: "macos".into(),
            has_gpu: false,
            has_display: true,
            has_sudo: false,
            has_docker: false,
        },
    )
    .await;
    let gpu_only = reg
        .find_nodes_with(&CapabilityNeeds {
            has_gpu: Some(true),
            ..Default::default()
        })
        .await;
    assert_eq!(gpu_only, vec!["linux-gpu".to_string()]);
    let docker_only = reg
        .find_nodes_with(&CapabilityNeeds {
            has_docker: Some(true),
            ..Default::default()
        })
        .await;
    assert_eq!(docker_only, vec!["linux-gpu".to_string()]);
    let none_match = reg
        .find_nodes_with(&CapabilityNeeds {
            os: Some("windows".into()),
            ..Default::default()
        })
        .await;
    assert!(none_match.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase6_world_update_flows_through_supervisor_path() {
    // Use the supervisor-style helper that consumes the world
    // channel and forwards into the registry. Verify that within
    // ~3s of agent start, the registry has SOMETHING cached (the
    // first WorldObserver tick fires at +2s).
    let addr = start_agent("world-node").await;
    let reg = Arc::new(NodeRegistry::new());
    let (handle, actor, mut world_rx, _caps_rx, _webrtc_rx) =
        connect_with_world_sink(addr, &test_psk()).await.unwrap();
    reg.register(handle).await;

    let reg_for_pump = reg.clone();
    let pump = tokio::spawn(async move {
        while let Some(state) = world_rx.recv().await {
            reg_for_pump.observe_world_update("world-node", state).await;
        }
    });

    // Wait up to 5s for the first observation to land.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut got = false;
    while std::time::Instant::now() < deadline {
        if reg.world_state_for("world-node").await.is_some() {
            got = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(got, "agent's WorldObserver should have produced an observation within 5s");

    // Cleanup.
    pump.abort();
    actor.abort();
}

#[tokio::test]
async fn phase7_job_lifecycle_end_to_end() {
    // Even without a connected agent, JobRegistry's lifecycle
    // machinery works: the spawn task fails (no agent) and the
    // job transitions to Failed.
    use kestrel_hub::jobs::{JobRegistry, JobStatus};
    let reg = Arc::new(NodeRegistry::new());
    let jobs = JobRegistry::new(reg);
    let id = jobs.start_shell("ghost".into(), "echo hi".into()).await;
    // Poll up to 2s for the job to finalize.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let s = jobs.status(&id).await.unwrap();
        if matches!(s.status, JobStatus::Failed | JobStatus::Completed) {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("job never finalized; current status: {:?}", s.status);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let final_status = jobs.status(&id).await.unwrap();
    assert!(matches!(
        final_status.status,
        JobStatus::Failed | JobStatus::Completed
    ));
    assert!(final_status.completed_unix.is_some());
}

#[tokio::test]
async fn phase11_policy_decisions_are_consistent() {
    use kestrel_hub::policy::{
        Decision, PolicyAction, PolicyConfig, PolicyRule, User,
    };
    let cfg = PolicyConfig {
        users: vec![User {
            user_id: "u1".into(),
            display_name: None,
            bearer_token: Some("tok".into()),
            policies: vec![
                PolicyRule {
                    op: "*".into(),
                    node: "*".into(),
                    action: PolicyAction::Allow,
                },
                PolicyRule {
                    op: "shell_*".into(),
                    node: "prod".into(),
                    action: PolicyAction::Deny,
                },
            ],
        }],
    };
    let user = cfg.user_for_bearer("tok").unwrap();
    assert_eq!(cfg.decide(user, "screenshot", "x").0, Decision::Allow);
    assert_eq!(cfg.decide(user, "shell_run", "prod").0, Decision::Deny);
    assert_eq!(cfg.decide(user, "shell_run", "dev").0, Decision::Allow);
}

#[tokio::test]
async fn phase13_webrtc_signalling_state_machine() {
    use kestrel_hub::webrtc::{SessionRegistry, SessionStatus};
    let reg = SessionRegistry::new();
    let id = reg.create("alpha".into()).await;
    assert_eq!(reg.get(&id).await.unwrap().status, SessionStatus::Created);
    assert!(reg.record_offer(&id, "o".into()).await);
    assert_eq!(reg.get(&id).await.unwrap().status, SessionStatus::OfferReceived);
    assert!(reg.record_answer(&id, "a".into()).await);
    assert_eq!(reg.get(&id).await.unwrap().status, SessionStatus::AnswerReady);
    assert!(reg.record_ice(&id, "ice1".into()).await);
    let s = reg.get(&id).await.unwrap();
    assert_eq!(s.ice_candidates.len(), 1);
}

/// End-to-end test of the Phase 13b SDP relay: hub forwards a real
/// SDP offer (generated by a webrtc-rs PC standing in for the
/// browser) to the agent, and the agent's WebRtcSession produces a
/// real SDP answer that arrives back through the supervisor's
/// webrtc_pump and lands in the SessionRegistry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13b_sdp_round_trip_through_hub_to_agent() {
    use kestrel_hub::webrtc::{SessionRegistry, SessionStatus};
    use webrtc::api::APIBuilder;
    use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_H264};
    use webrtc::peer_connection::configuration::RTCConfiguration;
    use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
    use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;

    let addr = start_agent("webrtc-node").await;
    let registry = Arc::new(NodeRegistry::new());
    let sessions = SessionRegistry::new();
    registry.attach_webrtc_sessions(sessions.clone()).await;

    // Use the supervisor path so the webrtc_pump is wired in.
    let (handle, _actor, _world_rx, _caps_rx, mut webrtc_rx) =
        connect_with_world_sink(addr, &test_psk()).await.unwrap();
    registry.register(handle.clone()).await;

    // Spawn the webrtc_pump manually (mirrors what supervisor does).
    let pump_registry = registry.clone();
    let pump = tokio::spawn(async move {
        while let Some(event) = webrtc_rx.recv().await {
            pump_registry.record_webrtc_event(event).await;
        }
    });

    // Create a hub-side session id and a webrtc-rs browser-side PC.
    let id = sessions.create("webrtc-node".into()).await;

    let mut m = MediaEngine::default();
    m.register_default_codecs().unwrap();
    let api = APIBuilder::new().with_media_engine(m).build();
    let browser_pc = api
        .new_peer_connection(RTCConfiguration::default())
        .await
        .unwrap();
    // Add a recv-only track so create_offer produces an m=video line.
    let dummy = std::sync::Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_H264.to_owned(),
            ..Default::default()
        },
        "v".into(),
        "browser".into(),
    ));
    browser_pc.add_track(dummy).await.unwrap();
    let offer = browser_pc.create_offer(None).await.unwrap();
    browser_pc.set_local_description(offer.clone()).await.unwrap();

    // Push the offer through the agent (the path /api/webrtc/.../offer
    // would take in production); record_offer puts it in the registry
    // first, then forward.
    assert!(sessions.record_offer(&id, offer.sdp.clone()).await);
    handle.send_webrtc_offer(id.clone(), offer.sdp).await.unwrap();

    // Wait for the SessionRegistry to transition to AnswerReady.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let answered = loop {
        let s = sessions.get(&id).await.unwrap();
        if s.status == SessionStatus::AnswerReady {
            break s;
        }
        if std::time::Instant::now() >= deadline {
            panic!("agent did not return SDP answer within 10s; status={:?}", s.status);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let answer_b64 = answered.answer_b64.expect("answer SDP recorded");
    use base64::Engine;
    let answer_sdp = String::from_utf8(
        base64::engine::general_purpose::STANDARD.decode(&answer_b64).unwrap(),
    )
    .unwrap();
    assert!(answer_sdp.contains("m=video"), "expected video m-line in answer SDP");
    assert!(
        answer_sdp.to_lowercase().contains("h264"),
        "expected H.264 codec in answer SDP: {}",
        answer_sdp
    );

    browser_pc.close().await.unwrap();
    pump.abort();
}
