// crates/kestrel-hub/assets/webrtc.js
//
// Phase 13b: browser-side WebRTC client. Dashboard pages embed this
// to receive a live stream from an agent.
//
// Flow:
//   1. POST /api/webrtc/session { node_id }   → { session_id }
//   2. Browser creates RTCPeerConnection, adds a recvonly video
//      transceiver, generates an SDP offer, POSTs it.
//   3. Browser polls GET /api/webrtc/session/:id until status is
//      AnswerReady; then setRemoteDescription(answer).
//   4. While ICE candidates trickle out of pc.onicecandidate, they
//      POST to /ice; the hub forwards them to the agent.
//   5. ontrack fires when the agent's video track lands; we attach
//      it to the <video> element on the page.
//
// CAVEAT: the hub-side SFU / agent-side encoder is the deferred
// chunk. With this JS in place, the signalling exchange is testable
// end-to-end against a hub that has a real PeerConnection wired up.

(function () {
    const KestrelWebRTC = {
        start: async function (nodeId, videoEl) {
            const csrf = (n) => document.cookie.split('; ').find(c => c.startsWith(n + '='))?.split('=')[1];
            // 1. Create session.
            const createResp = await fetch('/api/webrtc/session', {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ node_id: nodeId }),
            });
            if (!createResp.ok) throw new Error('session create failed: ' + createResp.status);
            const { session_id } = await createResp.json();

            // 2. PeerConnection.
            const pc = new RTCPeerConnection({
                iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
            });
            pc.addTransceiver('video', { direction: 'recvonly' });
            pc.ontrack = (e) => {
                if (videoEl && e.streams[0]) videoEl.srcObject = e.streams[0];
            };
            pc.onicecandidate = (e) => {
                if (e.candidate) {
                    fetch(`/api/webrtc/session/${session_id}/ice`, {
                        method: 'POST',
                        headers: { 'content-type': 'application/json' },
                        body: JSON.stringify({ candidate_json: JSON.stringify(e.candidate) }),
                    }).catch(() => { /* fire-and-forget */ });
                }
            };

            // 3. Offer.
            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
            // Base64 the SDP for round-trip safety on the wire.
            const offerB64 = btoa(offer.sdp);
            await fetch(`/api/webrtc/session/${session_id}/offer`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ sdp_b64: offerB64 }),
            });

            // 4. Poll for answer. Backs off from 100ms to 1s.
            let delay = 100;
            const deadline = Date.now() + 30_000;
            while (Date.now() < deadline) {
                const sresp = await fetch(`/api/webrtc/session/${session_id}`);
                if (sresp.ok) {
                    const s = await sresp.json();
                    if (s.status === 'answer_ready' && s.answer_b64) {
                        const answerSdp = atob(s.answer_b64);
                        await pc.setRemoteDescription({ type: 'answer', sdp: answerSdp });
                        return { pc, session_id };
                    }
                }
                await new Promise(r => setTimeout(r, delay));
                delay = Math.min(delay * 2, 1000);
            }
            throw new Error('timed out waiting for answer');
        },
    };
    window.KestrelWebRTC = KestrelWebRTC;
})();
