# Agent-Side WebRTC Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the polled-screenshot model with a real continuous WebRTC video stream from agents to dashboard browsers, plus a reverse data channel for input events. End state: open the dashboard, click a node, see live screen at <1s latency and remote-control it without leaving the page.

**Architecture:** The agent runs an `openh264` H.264 encoder fed by an `xcap` capture loop; encoded frames go into a `webrtc-rs` `TrackLocalStaticSample` attached to an agent-side `RTCPeerConnection`. SDP/ICE flows through the hub (existing signalling surface in `crates/kestrel-hub/src/webrtc.rs`), which adds a new bidirectional message channel between dashboard browsers and agents. A WebRTC `DataChannel` on the same PC carries key/mouse events back to the agent's existing input capability.

**Tech Stack:** `openh264` 0.6+, `webrtc` 0.11 (already in workspace), `xcap` 0.9 (already in workspace), `bytes`, `tokio`.

---

## Approach

This is intentionally split into seven independently-mergeable PRs. Each one produces a working build; later tasks layer on earlier ones without retro-fitting. The split lets us catch codec/perf surprises early without dragging the whole pipeline along.

**Risk register:**
1. **openh264 binary blob distribution.** The crate auto-downloads Cisco's pre-built binary at build time. If a CI runner is offline or sandboxed, the build fails. Mitigation: document the env override and let operators vendor the blob.
2. **BGRA → YUV420 software conversion is slow** at full-screen 60fps. We target 30fps at start and add SIMD/GPU later if needed. Acceptable for v1 — the goal is "remote control works," not "broadcast quality."
3. **NAT traversal.** webrtc-rs handles the STUN/TURN dance, but we need a public STUN server (Google's by default) and configurable TURN for restrictive networks. v1 ships STUN-only; TURN config follows.
4. **Multi-display.** The agent already knows its displays via `world_state.displays`. v1 streams display index 0; selecting a different display via the data channel is a follow-up.

---

## File Map

```
kestrel/
  Cargo.toml                                            # MODIFY: + openh264 workspace dep
  crates/kestrel-agent/
    Cargo.toml                                          # MODIFY: + openh264, webrtc, bytes
    src/
      capabilities/
        encoder.rs                                      # NEW: H.264 encoder + BGRA→YUV420 conversion
        screen_stream.rs                                # NEW: xcap capture loop → channel of encoded frames
        webrtc_session.rs                               # NEW: agent-side RTCPeerConnection + track + data channel
        mod.rs                                          # MODIFY: register the new modules
  crates/kestrel-proto/
    src/
      message.rs                                        # MODIFY: + Payload::WebRtcOffer / Answer / IceCandidate variants (hub↔agent)
  crates/kestrel-hub/
    src/
      webrtc.rs                                         # MODIFY: relay browser SDP to agent; relay agent answer back
      transport.rs                                      # MODIFY: route inbound Payload::WebRtcAnswer / IceCandidate to SessionRegistry
      mcp.rs                                            # MODIFY: no change needed (signalling already lives in dashboard/api.rs)
    assets/
      webrtc.js                                         # MODIFY: actually attach the received track to <video>; wire DataChannel for input
```

---

## Task 1 — openh264 encoder wrapper

**Files:**
- Modify: `Cargo.toml` (workspace deps)
- Modify: `crates/kestrel-agent/Cargo.toml`
- Create: `crates/kestrel-agent/src/capabilities/encoder.rs`
- Modify: `crates/kestrel-agent/src/capabilities/mod.rs`

- [ ] **Step 1: Add `openh264 = "0.6"` and `bytes = "1"` to workspace deps + agent deps.**

- [ ] **Step 2: Write the encoder skeleton with a failing unit test.**

```rust
// crates/kestrel-agent/src/capabilities/encoder.rs
use bytes::Bytes;

pub struct H264Encoder {
    inner: openh264::encoder::Encoder,
    width: u32,
    height: u32,
}

impl H264Encoder {
    pub fn new(width: u32, height: u32, target_fps: u32) -> anyhow::Result<Self> {
        let cfg = openh264::encoder::EncoderConfig::new()
            .max_frame_rate(target_fps as f32)
            .rate_control_mode(openh264::encoder::RateControlMode::Bitrate)
            .set_bitrate_bps(2_000_000);
        let inner = openh264::encoder::Encoder::with_config(cfg)?;
        Ok(Self { inner, width, height })
    }

    /// Encode one BGRA frame. Returns the H.264 NAL units as a single
    /// Bytes (concatenated, with start codes) ready for RTP packetization.
    pub fn encode_bgra(&mut self, bgra: &[u8]) -> anyhow::Result<Bytes> {
        let yuv = bgra_to_yuv420(bgra, self.width as usize, self.height as usize);
        let frame = openh264::formats::YUVBuffer::with_yuv(
            &yuv,
            self.width as usize,
            self.height as usize,
        );
        let out = self.inner.encode(&frame)?;
        let mut buf = bytes::BytesMut::new();
        for layer in 0..out.num_layers() {
            let layer = out.layer(layer).ok_or_else(|| anyhow::anyhow!("missing layer"))?;
            for nal in 0..layer.nal_count() {
                buf.extend_from_slice(layer.nal_unit(nal).ok_or_else(|| anyhow::anyhow!("nal"))?);
            }
        }
        Ok(buf.freeze())
    }
}

/// Naive BGRA → I420 (YUV 4:2:0). Software-only; SIMD acceleration is
/// a follow-up. Returns a flat buffer in Y/U/V plane order matching
/// openh264's YUVBuffer layout.
pub fn bgra_to_yuv420(bgra: &[u8], width: usize, height: usize) -> Vec<u8> {
    assert_eq!(bgra.len(), width * height * 4, "BGRA buffer size mismatch");
    let y_size = width * height;
    let uv_size = (width / 2) * (height / 2);
    let mut out = vec![0u8; y_size + 2 * uv_size];
    let (y_plane, uv_planes) = out.split_at_mut(y_size);
    let (u_plane, v_plane) = uv_planes.split_at_mut(uv_size);

    for y in 0..height {
        for x in 0..width {
            let i = (y * width + x) * 4;
            let b = bgra[i] as f32;
            let g = bgra[i + 1] as f32;
            let r = bgra[i + 2] as f32;
            // BT.601 limited-range coefficients.
            let yv = (0.257 * r + 0.504 * g + 0.098 * b + 16.0).round().clamp(0.0, 255.0);
            y_plane[y * width + x] = yv as u8;
            if x % 2 == 0 && y % 2 == 0 {
                let uv = (-0.148 * r - 0.291 * g + 0.439 * b + 128.0).round().clamp(0.0, 255.0);
                let vv = (0.439 * r - 0.368 * g - 0.071 * b + 128.0).round().clamp(0.0, 255.0);
                let ci = (y / 2) * (width / 2) + (x / 2);
                u_plane[ci] = uv as u8;
                v_plane[ci] = vv as u8;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bgra_to_yuv420_produces_expected_size() {
        let bgra = vec![128u8; 64 * 48 * 4];
        let yuv = bgra_to_yuv420(&bgra, 64, 48);
        // Y=64*48, U=32*24, V=32*24
        assert_eq!(yuv.len(), 64 * 48 + 32 * 24 * 2);
    }

    #[test]
    fn encoder_produces_nonempty_bitstream_for_first_frame() {
        let mut enc = H264Encoder::new(64, 48, 30).unwrap();
        let bgra = vec![200u8; 64 * 48 * 4];
        let frame = enc.encode_bgra(&bgra).unwrap();
        assert!(!frame.is_empty(), "first encoded frame must produce SPS/PPS + IDR");
    }
}
```

- [ ] **Step 3: Run `cargo test -p kestrel-agent --lib encoder` — expect failure (module not registered).**

- [ ] **Step 4: Register the module in `mod.rs`:**

```rust
pub mod encoder;
```

- [ ] **Step 5: Run `cargo test -p kestrel-agent --lib encoder` — both tests should now pass.**

- [ ] **Step 6: Commit.**

```bash
git add Cargo.toml crates/kestrel-agent/Cargo.toml crates/kestrel-agent/src/capabilities/encoder.rs crates/kestrel-agent/src/capabilities/mod.rs Cargo.lock
git commit -m "feat(agent): H.264 encoder wrapper + BGRA→YUV420 conversion"
```

---

## Task 2 — Screen capture stream

**Files:**
- Create: `crates/kestrel-agent/src/capabilities/screen_stream.rs`
- Modify: `crates/kestrel-agent/src/capabilities/mod.rs`

- [ ] **Step 1: Write the capture-loop skeleton with a failing test.**

```rust
// crates/kestrel-agent/src/capabilities/screen_stream.rs
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::capabilities::encoder::H264Encoder;

pub struct EncodedFrame {
    pub bytes: Bytes,
    pub pts_ms: u64,
}

/// Spawn a capture loop that produces encoded H.264 frames at
/// approximately `target_fps`. Returns the receiver end of the
/// channel; the sender lives in the spawned task and drops when the
/// task exits.
///
/// `display_index` selects which monitor to capture (0 = primary).
pub fn spawn(display_index: usize, target_fps: u32) -> mpsc::Receiver<EncodedFrame> {
    let (tx, rx) = mpsc::channel::<EncodedFrame>(8);
    let interval = Duration::from_micros(1_000_000 / target_fps as u64);
    tokio::task::spawn_blocking(move || {
        let monitors = match xcap::Monitor::all() {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("screen_stream: cannot list monitors: {}", e);
                return;
            }
        };
        let Some(mon) = monitors.into_iter().nth(display_index) else {
            tracing::warn!("screen_stream: display {} not found", display_index);
            return;
        };
        let width = mon.width();
        let height = mon.height();
        let mut encoder = match H264Encoder::new(width, height, target_fps) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("screen_stream: encoder init failed: {}", e);
                return;
            }
        };
        let started = std::time::Instant::now();
        loop {
            let frame_start = std::time::Instant::now();
            let img = match mon.capture_image() {
                Ok(i) => i,
                Err(e) => {
                    tracing::warn!("screen_stream: capture failed: {}", e);
                    std::thread::sleep(interval);
                    continue;
                }
            };
            // xcap returns an RgbaImage; we need BGRA bytes.
            let mut bgra = img.into_raw();
            for px in bgra.chunks_exact_mut(4) {
                px.swap(0, 2); // RGBA → BGRA
            }
            let bytes = match encoder.encode_bgra(&bgra) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("screen_stream: encode failed: {}", e);
                    continue;
                }
            };
            let pts_ms = started.elapsed().as_millis() as u64;
            if tx.blocking_send(EncodedFrame { bytes, pts_ms }).is_err() {
                // Receiver dropped; tear down.
                break;
            }
            let elapsed = frame_start.elapsed();
            if elapsed < interval {
                std::thread::sleep(interval - elapsed);
            }
        }
    });
    rx
}
```

- [ ] **Step 2: Add a test that verifies the loop exits cleanly when the receiver drops.**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn loop_exits_when_receiver_drops() {
        let rx = spawn(0, 30);
        drop(rx);
        // Give the task a moment to notice. If it leaks we'd see it
        // as a hung test under cargo nextest's timeout.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
```

- [ ] **Step 3: Register module + run test. Commit.**

```bash
git add crates/kestrel-agent/src/capabilities/screen_stream.rs crates/kestrel-agent/src/capabilities/mod.rs
git commit -m "feat(agent): xcap-driven H.264 capture stream"
```

---

## Task 3 — Agent-side RTCPeerConnection scaffold

**Files:**
- Modify: `crates/kestrel-agent/Cargo.toml` (+ webrtc dep)
- Create: `crates/kestrel-agent/src/capabilities/webrtc_session.rs`
- Modify: `crates/kestrel-agent/src/capabilities/mod.rs`

- [ ] **Step 1: Add `webrtc = { workspace = true }` to agent's Cargo.toml.**

- [ ] **Step 2: Mirror the hub's `build_peer_connection`:**

```rust
// crates/kestrel-agent/src/capabilities/webrtc_session.rs
use std::sync::Arc;
use webrtc::peer_connection::RTCPeerConnection;

pub async fn build_peer_connection() -> anyhow::Result<Arc<RTCPeerConnection>> {
    use webrtc::api::APIBuilder;
    use webrtc::api::interceptor_registry::register_default_interceptors;
    use webrtc::api::media_engine::MediaEngine;
    use webrtc::interceptor::registry::Registry;
    use webrtc::peer_connection::configuration::RTCConfiguration;
    use webrtc::ice_transport::ice_server::RTCIceServer;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn construct_peer_connection() {
        let pc = build_peer_connection().await.unwrap();
        // Sanity: signalling state is "Stable" right after construction.
        assert_eq!(
            pc.signaling_state(),
            webrtc::peer_connection::signaling_state::RTCSignalingState::Stable
        );
    }
}
```

- [ ] **Step 3: Register module + test + commit.**

---

## Task 4 — Attach an H.264 track + sample writer

**Files:**
- Modify: `crates/kestrel-agent/src/capabilities/webrtc_session.rs`

- [ ] **Step 1: Add a `WebRtcSession` struct that owns the PC and a `TrackLocalStaticSample`. Spawn a task that drains a `mpsc::Receiver<EncodedFrame>` and writes samples.**

```rust
use std::sync::Arc;
use tokio::sync::mpsc;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc::media::Sample;

use crate::capabilities::screen_stream::EncodedFrame;

pub struct WebRtcSession {
    pub pc: Arc<RTCPeerConnection>,
}

impl WebRtcSession {
    pub async fn new(mut frames: mpsc::Receiver<EncodedFrame>) -> anyhow::Result<Self> {
        let pc = build_peer_connection().await?;
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: webrtc::api::media_engine::MIME_TYPE_H264.to_owned(),
                ..Default::default()
            },
            "screen".to_owned(),
            "kestrel".to_owned(),
        ));
        let _sender = pc.add_track(track.clone()).await?;
        let writer = track.clone();
        tokio::spawn(async move {
            let mut last_pts: u64 = 0;
            while let Some(f) = frames.recv().await {
                let duration = std::time::Duration::from_millis(
                    f.pts_ms.saturating_sub(last_pts).max(1),
                );
                last_pts = f.pts_ms;
                let _ = writer
                    .write_sample(&Sample { data: f.bytes, duration, ..Default::default() })
                    .await;
            }
        });
        Ok(Self { pc })
    }
}
```

- [ ] **Step 2: Add a test that feeds synthetic frames and asserts the PC has one transceiver.**

- [ ] **Step 3: Commit.**

---

## Task 5 — Signalling: relay browser SDP through hub to agent

**Files:**
- Modify: `crates/kestrel-proto/src/message.rs` (+ Payload variants 31-33)
- Modify: `crates/kestrel-hub/src/webrtc.rs` (relay logic)
- Modify: `crates/kestrel-hub/src/transport.rs` (route agent answer/ICE)
- Modify: `crates/kestrel-agent/src/transport.rs` (handle inbound offer)

- [ ] **Step 1: Add new Payload variants:**

```rust
WebRtcOffer { session_id: String, sdp: String } = 31,
WebRtcAnswer { session_id: String, sdp: String } = 32,
WebRtcIce { session_id: String, candidate: String } = 33,
```

- [ ] **Step 2: Add roundtrip tests for all three.**

- [ ] **Step 3: On the hub side, when a browser POSTs `/api/webrtc/session/{id}/offer`, send `WebRtcOffer` to the target agent via its `NodeHandle`. When the agent's actor receives `WebRtcAnswer`, push it into `SessionRegistry::record_answer`.**

- [ ] **Step 4: On the agent side, handling an inbound `WebRtcOffer` creates a `WebRtcSession`, calls `set_remote_description`, calls `create_answer`, and replies with `WebRtcAnswer`.**

- [ ] **Step 5: Add an integration test that completes the SDP exchange. Both endpoints are in-process; assert the agent's PC reaches `HaveRemoteOffer` state.**

- [ ] **Step 6: Commit.**

---

## Task 6 — DataChannel for input events back to agent

**Files:**
- Modify: `crates/kestrel-agent/src/capabilities/webrtc_session.rs`

- [ ] **Step 1: In `WebRtcSession::new`, register an `on_data_channel` handler. Inbound JSON messages of shape `{"kind":"key","code":"a"}` or `{"kind":"mouse","x":100,"y":200}` dispatch into the existing `crate::capabilities::input` module.**

- [ ] **Step 2: Test: open a loopback PC pair, send a synthetic key event over the data channel, assert the input dispatcher was called.**

- [ ] **Step 3: Commit.**

---

## Task 7 — Browser-side: actually render the track

**Files:**
- Modify: `crates/kestrel-hub/assets/webrtc.js`

- [ ] **Step 1: In the browser, after creating the `RTCPeerConnection` and POSTing the offer, attach a `pc.ontrack` handler that pipes `event.streams[0]` into a `<video>` element.**

- [ ] **Step 2: Add a `<video autoplay muted playsinline>` to the dashboard's node-detail page.**

- [ ] **Step 3: Wire a `DataChannel` for input. On `keydown` / `mousemove` inside the `<video>`, serialize and send over the channel.**

- [ ] **Step 4: Manual verification — open the dashboard, click a node, see screen, type, see characters arrive on the agent.**

---

## Verification

End-to-end manual test (requires two machines or one machine + Tart VM):

1. `cargo build --workspace --release`
2. `cargo test --workspace` — all green.
3. Start hub: `kestrel-hub start`
4. Start agent on second machine: `kestrel-agent start`
5. Open `https://hub:7273` in browser, log in, click the agent's row.
6. Expect: live video pane showing the agent's screen within 2-3 seconds.
7. Type into the video pane — expect characters to appear on the agent's host.

**Aesthetic gate:** the video pane should not be wrapped in extra chrome — the dashboard's restrained Linear-style aesthetic still applies. No play/pause controls, no fullscreen button reinvented in CSS, no overlay text. Just the `<video>` element on a flat divider background.

---

## Out of scope (deferred)

- Hardware-accelerated encoding (VideoToolbox / NVENC / VA-API)
- SIMD-accelerated BGRA→YUV420 (would land if naive impl can't hold 30fps)
- Adaptive bitrate based on RTCP receiver reports
- TURN server config (STUN-only for v1)
- Multi-display selection (display 0 always)
- Audio channel (out of scope for fleet control)
- Recording / SFU fan-out to multiple browsers per node
