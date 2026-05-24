// crates/kestrel-hub/assets/node-detail.js
//
// Bootstraps the live-stream PeerConnection on /node/:node_id. node_id
// comes from a data attribute on the <video> element rather than
// inline script interpolation — that keeps the page XSS-safe even if
// node_id contains a `</script>` byte sequence (maud HTML-escapes
// attributes for us).

(function () {
    const videoEl = document.getElementById('kv');
    if (!videoEl) return;
    const nodeId = videoEl.dataset.nodeId;
    if (!nodeId) return;
    window.KestrelWebRTC.start(nodeId, videoEl).catch((e) => {
        console.error('webrtc start failed:', e);
        const err = document.createElement('p');
        err.className = 'error';
        err.textContent = 'stream failed: ' + e.message;
        document.querySelector('main').appendChild(err);
    });
})();
