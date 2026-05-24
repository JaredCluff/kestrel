// crates/kestrel-agent/src/capabilities/webrtc_session.rs
//
// Agent-side WebRTC session. Owns an RTCPeerConnection with an H.264
// video track fed by the capture stream. Combines bricks 3 and 4 of
// the WebRTC pipeline plan: a constructed PC is useless without a
// track, and a track is useless without a PC — they live together.
//
// SDP negotiation (offer/answer/ICE) is exchanged via the existing
// hub<->agent message channel (Payload::WebRtcOffer/Answer/Ice —
// added in a follow-up task). This module exposes the PC so the
// transport layer can call set_remote_description / create_answer /
// add_ice_candidate against it.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use webrtc::api::APIBuilder;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_H264};
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::media::Sample;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;

use crate::capabilities::screen_stream::EncodedFrame;

/// Construct a fresh RTCPeerConnection with default codecs + Google's
/// public STUN. Operators with NAT'd networks should replace this
/// with their own ICE config; for v1 we ship STUN-only and add TURN
/// configurability in a follow-up.
pub async fn build_peer_connection() -> anyhow::Result<Arc<RTCPeerConnection>> {
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
    Ok(Arc::new(api.new_peer_connection(config).await?))
}

/// One active streaming session. Holds the PC so the SDP negotiation
/// layer can drive it from outside; spawns a writer task that pulls
/// EncodedFrames off the channel and pushes them onto the track as
/// WebRTC media samples.
pub struct WebRtcSession {
    pub pc: Arc<RTCPeerConnection>,
}

impl WebRtcSession {
    /// Build a session bound to `frames`. The frames channel is owned
    /// by the writer task spawned here; closing it (e.g. by dropping
    /// the capture loop's sender) drains the writer cleanly.
    pub async fn new(mut frames: mpsc::Receiver<EncodedFrame>) -> anyhow::Result<Self> {
        let pc = build_peer_connection().await?;
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                ..Default::default()
            },
            "screen".to_owned(),
            "kestrel".to_owned(),
        ));
        // `add_track` returns the RTCRtpSender; we don't need to hold it
        // (the PC owns it via the transceiver), but keep it bound so a
        // reviewer can see the API contract.
        let _sender = pc.add_track(track.clone()).await?;

        let writer = track.clone();
        tokio::spawn(async move {
            let mut last_pts: u64 = 0;
            while let Some(f) = frames.recv().await {
                // Sample::duration is the inter-frame gap. For the first
                // frame we have nothing to subtract from, so seed with
                // 1ms — webrtc-rs treats zero as invalid.
                let duration = Duration::from_millis(
                    f.pts_ms.saturating_sub(last_pts).max(1),
                );
                last_pts = f.pts_ms;
                if let Err(e) = writer
                    .write_sample(&Sample {
                        data: f.bytes,
                        duration,
                        ..Default::default()
                    })
                    .await
                {
                    // A closed PC drops the writer; that's a normal
                    // wind-down, not an error worth bubbling.
                    tracing::debug!("webrtc_session: write_sample ended: {}", e);
                    break;
                }
            }
        });
        Ok(Self { pc })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use webrtc::peer_connection::signaling_state::RTCSignalingState;

    #[tokio::test]
    async fn build_peer_connection_starts_in_stable_state() {
        let pc = build_peer_connection().await.unwrap();
        assert_eq!(pc.signaling_state(), RTCSignalingState::Stable);
        pc.close().await.unwrap();
    }

    #[tokio::test]
    async fn session_attaches_one_transceiver() {
        let (_tx, rx) = mpsc::channel::<EncodedFrame>(1);
        let session = WebRtcSession::new(rx).await.unwrap();
        let transceivers = session.pc.get_transceivers().await;
        assert_eq!(transceivers.len(), 1, "expected one H.264 video transceiver");
        session.pc.close().await.unwrap();
    }

    #[tokio::test]
    async fn writer_task_drains_pending_frames_and_exits() {
        let (tx, rx) = mpsc::channel::<EncodedFrame>(4);
        let session = WebRtcSession::new(rx).await.unwrap();
        // Feed a handful of synthetic frames. The writer task may
        // log write_sample errors (no peer connected) but must not
        // panic.
        for i in 0..3 {
            tx.send(EncodedFrame {
                bytes: Bytes::from(vec![0u8; 16]),
                pts_ms: i as u64 * 33,
            })
            .await
            .unwrap();
        }
        // Drop the sender → channel closes → writer task exits.
        drop(tx);
        tokio::time::sleep(Duration::from_millis(50)).await;
        session.pc.close().await.unwrap();
    }

    #[tokio::test]
    async fn local_description_round_trip_completes() {
        // Sanity: the PC can produce an offer + set it back as local
        // description. Catches API/feature breakage early.
        let pc = build_peer_connection().await.unwrap();
        // Need at least one track or transceiver for create_offer to
        // produce a non-empty SDP.
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_owned(),
                ..Default::default()
            },
            "v".into(),
            "k".into(),
        ));
        pc.add_track(track).await.unwrap();
        let offer = pc.create_offer(None).await.unwrap();
        pc.set_local_description(offer).await.unwrap();
        assert!(pc.local_description().await.is_some());
        pc.close().await.unwrap();
    }
}
