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

use kestrel_proto::{KestrelMessage, MsgKind, Payload};
use tokio::sync::mpsc;
use webrtc::api::APIBuilder;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_H264};
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::media::Sample;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;

use crate::capabilities::{screen, screen_stream::{self, EncodedFrame}};

/// Probe the agent's primary display dimensions for normalizing
/// inbound mouse coordinates. Falls back to 1920x1080 when no
/// display is detected (headless test environments).
fn primary_display_dims() -> (u32, u32) {
    screen::list_displays()
        .into_iter()
        .next()
        .map(|(_, w, h)| (w, h))
        .unwrap_or((1920, 1080))
}

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

/// One JSON-serialized input event arriving over the WebRTC data
/// channel. The browser-side `webrtc.js` produces these for keydown /
/// keyup / mousemove / mousebutton / wheel events. Kept intentionally
/// loose (untyped `String` fields for keys) so the wire stays simple
/// and the browser doesn't need a code map. The agent maps strings
/// to its KeyCode enum and dispatches into `capabilities::input`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InputEvent {
    /// `code` is a DOM KeyboardEvent.code value (e.g. "KeyA", "Enter").
    Key { code: String, modifiers: Modifiers, action: Action },
    /// Synthetic typed text — the browser may batch quick keystrokes.
    Text { text: String },
    /// `x` and `y` are normalized 0.0..1.0 relative to the video element.
    MouseMove { x: f64, y: f64 },
    MouseButton { button: MouseButton, action: Action, x: f64, y: f64 },
    Scroll { dx: f64, dy: f64 },
}

#[derive(Debug, Clone, Copy, serde::Deserialize, Default)]
pub struct Modifiers {
    #[serde(default)]
    pub shift: bool,
    #[serde(default)]
    pub ctrl: bool,
    #[serde(default)]
    pub alt: bool,
    #[serde(default)]
    pub meta: bool,
}

#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Press,
    Release,
}

#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MouseButton {
    Left,
    Right,
    Middle,
}

/// Dispatch one decoded InputEvent into the agent's existing input
/// capability. Display dimensions are needed to denormalize mouse
/// coords; v1 uses the agent's primary display. Errors are surfaced
/// for the caller's logging.
pub async fn dispatch_input(
    event: InputEvent,
    display_w: u32,
    display_h: u32,
) -> anyhow::Result<()> {
    use kestrel_proto::{Button, KeyCode, PressRelease};

    let to_press_release = |a: Action| match a {
        Action::Press => PressRelease::Press,
        Action::Release => PressRelease::Release,
    };
    let to_button = |b: MouseButton| match b {
        MouseButton::Left => Button::Left,
        MouseButton::Right => Button::Right,
        MouseButton::Middle => Button::Middle,
    };
    let to_modifiers = |m: Modifiers| kestrel_proto::Modifiers {
        shift: m.shift,
        ctrl: m.ctrl,
        alt: m.alt,
        meta: m.meta,
    };

    match event {
        InputEvent::Key { code, modifiers, action } => {
            let key = key_from_dom_code(&code)
                .ok_or_else(|| anyhow::anyhow!("unknown DOM key code: {}", code))?;
            crate::capabilities::input::inject_key_event(
                key,
                to_modifiers(modifiers),
                to_press_release(action),
            )
            .await
        }
        InputEvent::Text { text } => crate::capabilities::input::inject_text(text).await,
        InputEvent::MouseMove { x, y } => {
            crate::capabilities::input::inject_mouse_move(x, y, display_w, display_h).await
        }
        InputEvent::MouseButton { button, action, x, y } => {
            crate::capabilities::input::inject_mouse_button(
                to_button(button),
                to_press_release(action),
                x,
                y,
                display_w,
                display_h,
            )
            .await
        }
        InputEvent::Scroll { dx, dy } => {
            crate::capabilities::input::inject_scroll(dx, dy).await
        }
    }
}

/// Translate a DOM KeyboardEvent.code string into the proto's KeyCode.
/// Covers the common keys; uncommon ones (function keys past F12,
/// non-Latin layouts) return None and the dispatch layer logs.
pub fn key_from_dom_code(code: &str) -> Option<kestrel_proto::KeyCode> {
    use kestrel_proto::KeyCode;
    // Letters: "KeyA" .. "KeyZ"
    if let Some(rest) = code.strip_prefix("Key") {
        if rest.len() == 1 {
            let ch = rest.chars().next().unwrap();
            return Some(KeyCode::Char(ch.to_ascii_lowercase()));
        }
    }
    // Digits: "Digit0" .. "Digit9"
    if let Some(rest) = code.strip_prefix("Digit") {
        if let Some(d) = rest.chars().next() {
            return Some(KeyCode::Char(d));
        }
    }
    Some(match code {
        "Enter" => KeyCode::Return,
        "Tab" => KeyCode::Tab,
        "Backspace" => KeyCode::Backspace,
        "Escape" => KeyCode::Escape,
        "Space" => KeyCode::Space,
        "ArrowUp" => KeyCode::Up,
        "ArrowDown" => KeyCode::Down,
        "ArrowLeft" => KeyCode::Left,
        "ArrowRight" => KeyCode::Right,
        "Home" => KeyCode::Home,
        "End" => KeyCode::End,
        "PageUp" => KeyCode::PageUp,
        "PageDown" => KeyCode::PageDown,
        "Delete" => KeyCode::Delete,
        _ => return None,
    })
}

/// Handle a `Payload::WebRtcOffer` arriving from the hub. Boots a
/// screen capture stream, wires it through a new `WebRtcSession`,
/// completes the SDP answer half of the negotiation, and registers an
/// ICE-candidate handler that emits `Payload::WebRtcIce` events back
/// to the hub via `event_tx`. Returns the constructed session so the
/// caller can store it in its session map and route subsequent
/// `WebRtcIce` frames into `add_ice_candidate`.
///
/// `display_index` / `target_fps` are hard-wired here for v1; making
/// them browser-selectable is a documented follow-up.
pub async fn handle_offer(
    session_id: String,
    offer_sdp: String,
    event_tx: mpsc::Sender<KestrelMessage>,
) -> anyhow::Result<Arc<WebRtcSession>> {
    let frames = screen_stream::spawn(0, 30);
    let session = Arc::new(WebRtcSession::new(frames).await?);

    // Wire input-event reception from the browser's data channel into
    // the agent's existing input capability. The browser opens an
    // "input" channel inside its offer (negotiated by SDP); this
    // handler fires on every inbound message regardless of channel
    // label so we don't have to plumb a separate channel-id config.
    let pc = session.pc.clone();
    let (input_w, input_h) = primary_display_dims();
    pc.on_data_channel(Box::new(move |dc| {
        Box::pin(async move {
            dc.on_message(Box::new(move |msg: DataChannelMessage| {
                let payload = String::from_utf8_lossy(&msg.data).to_string();
                Box::pin(async move {
                    let event: InputEvent = match serde_json::from_str(&payload) {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::debug!("webrtc input: parse failed: {} ({})", e, payload);
                            return;
                        }
                    };
                    if let Err(e) = dispatch_input(event, input_w, input_h).await {
                        tracing::warn!("webrtc input dispatch: {}", e);
                    }
                })
            }));
        })
    }));

    let pc = session.pc.clone();
    let event_tx_for_ice = event_tx.clone();
    let sid_for_ice = session_id.clone();
    pc.on_ice_candidate(Box::new(move |cand| {
        let event_tx = event_tx_for_ice.clone();
        let sid = sid_for_ice.clone();
        Box::pin(async move {
            let Some(c) = cand else { return };
            let init = match c.to_json() {
                Ok(i) => i,
                Err(e) => {
                    tracing::warn!("webrtc: candidate.to_json: {}", e);
                    return;
                }
            };
            let candidate_json = match serde_json::to_string(&init) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("webrtc: serialize ICE candidate: {}", e);
                    return;
                }
            };
            let _ = event_tx
                .send(KestrelMessage {
                    stream_id: 0,
                    kind: MsgKind::Event,
                    payload: Payload::WebRtcIce {
                        session_id: sid,
                        candidate: candidate_json,
                    },
                })
                .await;
        })
    }));

    // Complete the answer half of the SDP exchange.
    let offer = RTCSessionDescription::offer(offer_sdp)?;
    session.pc.set_remote_description(offer).await?;
    let answer = session.pc.create_answer(None).await?;
    session.pc.set_local_description(answer.clone()).await?;

    let _ = event_tx
        .send(KestrelMessage {
            stream_id: 0,
            kind: MsgKind::Event,
            payload: Payload::WebRtcAnswer {
                session_id,
                sdp: answer.sdp,
            },
        })
        .await;

    Ok(session)
}

/// Parse and feed a hub-forwarded ICE candidate into an existing
/// session. Called from the agent transport on `Payload::WebRtcIce`.
pub async fn add_remote_ice(
    session: &WebRtcSession,
    candidate_json: &str,
) -> anyhow::Result<()> {
    let init: RTCIceCandidateInit = serde_json::from_str(candidate_json)
        .map_err(|e| anyhow::anyhow!("invalid RTCIceCandidateInit JSON: {}", e))?;
    session.pc.add_ice_candidate(init).await?;
    Ok(())
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

    #[test]
    fn key_from_dom_code_handles_letters_digits_specials() {
        use kestrel_proto::KeyCode;
        assert!(matches!(key_from_dom_code("KeyA"), Some(KeyCode::Char('a'))));
        assert!(matches!(key_from_dom_code("KeyZ"), Some(KeyCode::Char('z'))));
        assert!(matches!(key_from_dom_code("Digit5"), Some(KeyCode::Char('5'))));
        assert!(matches!(key_from_dom_code("Enter"), Some(KeyCode::Return)));
        assert!(matches!(key_from_dom_code("ArrowLeft"), Some(KeyCode::Left)));
        assert!(matches!(key_from_dom_code("Space"), Some(KeyCode::Space)));
        assert!(key_from_dom_code("UnknownThing").is_none());
    }

    #[test]
    fn input_event_json_decodes_each_variant() {
        let key: InputEvent = serde_json::from_str(
            r#"{"kind":"key","code":"KeyA","modifiers":{"shift":true},"action":"press"}"#,
        )
        .unwrap();
        match key {
            InputEvent::Key { code, modifiers, action } => {
                assert_eq!(code, "KeyA");
                assert!(modifiers.shift);
                assert!(matches!(action, Action::Press));
            }
            _ => panic!("expected Key"),
        }

        let mv: InputEvent = serde_json::from_str(
            r#"{"kind":"mouse_move","x":0.5,"y":0.25}"#,
        )
        .unwrap();
        match mv {
            InputEvent::MouseMove { x, y } => {
                assert!((x - 0.5).abs() < 1e-9);
                assert!((y - 0.25).abs() < 1e-9);
            }
            _ => panic!("expected MouseMove"),
        }

        let btn: InputEvent = serde_json::from_str(
            r#"{"kind":"mouse_button","button":"left","action":"release","x":0.1,"y":0.2}"#,
        )
        .unwrap();
        assert!(matches!(btn, InputEvent::MouseButton { button: MouseButton::Left, .. }));

        let sc: InputEvent = serde_json::from_str(r#"{"kind":"scroll","dx":0.0,"dy":-3.0}"#).unwrap();
        assert!(matches!(sc, InputEvent::Scroll { .. }));

        let txt: InputEvent = serde_json::from_str(r#"{"kind":"text","text":"hi"}"#).unwrap();
        assert!(matches!(txt, InputEvent::Text { .. }));
    }

    #[test]
    fn input_event_json_rejects_unknown_kind() {
        let bad = serde_json::from_str::<InputEvent>(r#"{"kind":"sneeze"}"#);
        assert!(bad.is_err());
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
