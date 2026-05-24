// crates/kestrel-hub/assets/webrtc.js
//
// Browser-side WebRTC client. Dashboard pages embed this to receive a
// live video stream from an agent and ship input events back over a
// data channel.
//
// Flow:
//   1. POST /api/webrtc/session { node_id }   → { session_id }
//   2. Browser creates RTCPeerConnection, adds a recvonly video
//      transceiver + a bidirectional "input" data channel, generates
//      an SDP offer, POSTs it.
//   3. Browser polls GET /api/webrtc/session/:id until status is
//      answer_ready; then setRemoteDescription(answer).
//   4. ICE candidates trickled from pc.onicecandidate POST to /ice;
//      the hub forwards them to the agent.
//   5. ontrack fires when the agent's video track lands; we attach
//      it to the <video> element on the page.
//   6. Once the data channel opens, keyboard + mouse handlers attached
//      to the <video> element serialize events as JSON and send.

(function () {
    function csrfToken() {
        const cookie = document.cookie
            .split('; ')
            .find(c => c.startsWith('csrf_token='));
        return cookie ? cookie.split('=')[1] : '';
    }

    function withCsrf(headers) {
        const h = Object.assign({ 'content-type': 'application/json' }, headers || {});
        const t = csrfToken();
        if (t) h['x-csrf-token'] = t;
        return h;
    }

    function wireInputEvents(videoEl, channel) {
        // We attach handlers to the <video> element rather than window
        // so each session's input flow is naturally scoped — closing
        // the video tag detaches the listeners.
        const send = (event) => {
            if (channel.readyState !== 'open') return;
            try { channel.send(JSON.stringify(event)); } catch (_) { /* ignore */ }
        };
        const modsFrom = (e) => ({
            shift: e.shiftKey, ctrl: e.ctrlKey, alt: e.altKey, meta: e.metaKey,
        });
        videoEl.tabIndex = 0; // allow focus so it receives keydown
        videoEl.addEventListener('keydown', (e) => {
            e.preventDefault();
            send({ kind: 'key', code: e.code, modifiers: modsFrom(e), action: 'press' });
        });
        videoEl.addEventListener('keyup', (e) => {
            e.preventDefault();
            send({ kind: 'key', code: e.code, modifiers: modsFrom(e), action: 'release' });
        });
        videoEl.addEventListener('mousemove', (e) => {
            const r = videoEl.getBoundingClientRect();
            send({ kind: 'mouse_move',
                x: (e.clientX - r.left) / r.width,
                y: (e.clientY - r.top) / r.height });
        });
        const buttonName = (b) => ({ 0: 'left', 1: 'middle', 2: 'right' }[b] || 'left');
        videoEl.addEventListener('mousedown', (e) => {
            e.preventDefault();
            const r = videoEl.getBoundingClientRect();
            send({ kind: 'mouse_button', button: buttonName(e.button),
                action: 'press',
                x: (e.clientX - r.left) / r.width,
                y: (e.clientY - r.top) / r.height });
        });
        videoEl.addEventListener('mouseup', (e) => {
            e.preventDefault();
            const r = videoEl.getBoundingClientRect();
            send({ kind: 'mouse_button', button: buttonName(e.button),
                action: 'release',
                x: (e.clientX - r.left) / r.width,
                y: (e.clientY - r.top) / r.height });
        });
        videoEl.addEventListener('wheel', (e) => {
            e.preventDefault();
            // Normalize browser wheel deltas (which vary wildly) into
            // small integer-ish steps; the agent treats these as a
            // unitless scroll amount the way enigo does.
            send({ kind: 'scroll', dx: e.deltaX / 100, dy: -e.deltaY / 100 });
        }, { passive: false });
        // Suppress the context menu so right-click reaches the agent.
        videoEl.addEventListener('contextmenu', (e) => e.preventDefault());
    }

    const KestrelWebRTC = {
        start: async function (nodeId, videoEl) {
            // 1. Create session.
            const createResp = await fetch('/api/webrtc/session', {
                method: 'POST', headers: withCsrf(),
                body: JSON.stringify({ node_id: nodeId }),
            });
            if (!createResp.ok) throw new Error('session create failed: ' + createResp.status);
            const { session_id } = await createResp.json();

            // 2. PeerConnection + transceivers + data channel.
            const pc = new RTCPeerConnection({
                iceServers: [{ urls: 'stun:stun.l.google.com:19302' }],
            });
            pc.addTransceiver('video', { direction: 'recvonly' });
            // Negotiated by SDP — the agent registers on_data_channel
            // and replies on whatever channel arrives.
            const inputChannel = pc.createDataChannel('input');
            inputChannel.onopen = () => { wireInputEvents(videoEl, inputChannel); };

            pc.ontrack = (e) => {
                if (videoEl && e.streams[0]) videoEl.srcObject = e.streams[0];
            };
            pc.onicecandidate = (e) => {
                if (e.candidate) {
                    fetch(`/api/webrtc/session/${session_id}/ice`, {
                        method: 'POST', headers: withCsrf(),
                        body: JSON.stringify({ candidate_json: JSON.stringify(e.candidate) }),
                    }).catch(() => { /* fire-and-forget */ });
                }
            };

            // 3. Offer.
            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
            const offerB64 = btoa(offer.sdp);
            await fetch(`/api/webrtc/session/${session_id}/offer`, {
                method: 'POST', headers: withCsrf(),
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
                        return { pc, session_id, inputChannel };
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
