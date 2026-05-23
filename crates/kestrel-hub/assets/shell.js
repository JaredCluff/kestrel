// crates/kestrel-hub/assets/shell.js
//
// Minimal browser shell client. Opens a WebSocket to the hub's
// /api/shell/ws/<node_id> endpoint, streams output into a <pre>, sends
// commands from a text <input>. No external dependencies — ~50 lines
// of vanilla JS to match the rest of the dashboard's no-framework
// aesthetic.

(() => {
    const nodeId = window.__kestrelNodeId;
    if (!nodeId) {
        console.error("kestrel: missing __kestrelNodeId; shell.js can't initialize");
        return;
    }

    const output = document.getElementById("shell-output");
    const form = document.getElementById("shell-form");
    const input = document.getElementById("shell-input");

    function append(text) {
        // Append at the end and scroll to bottom. We don't strip ANSI
        // sequences — the page documentation warns operators about it.
        output.textContent += text;
        output.scrollTop = output.scrollHeight;
    }

    function status(line) {
        append("\n[hub] " + line + "\n");
    }

    // Compose ws:// or wss:// based on page protocol so the shell works
    // whether the dashboard is plain HTTP or TLS (PR #38).
    const wsProto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${wsProto}//${window.location.host}/api/shell/ws/${encodeURIComponent(nodeId)}`;
    let ws;
    try {
        ws = new WebSocket(wsUrl);
    } catch (e) {
        status("WebSocket construction failed: " + e);
        return;
    }

    ws.addEventListener("open", () => status("connected to " + nodeId));
    ws.addEventListener("close", () => status("disconnected"));
    ws.addEventListener("error", (e) => status("error: " + (e.message || "unknown")));
    ws.addEventListener("message", (evt) => {
        // Server frames are text (PTY output bytes interpreted as UTF-8
        // — the agent's shell capability already returns UTF-8 strings
        // by the time they reach us).
        append(typeof evt.data === "string" ? evt.data : "");
    });

    form.addEventListener("submit", (e) => {
        e.preventDefault();
        const line = input.value;
        // Echo locally so the user sees what they typed even if the
        // shell's local echo is off.
        append(line + "\n");
        if (ws.readyState === WebSocket.OPEN) {
            // Send the line plus newline so the shell's line discipline
            // treats it as a complete command.
            ws.send(line + "\n");
        }
        input.value = "";
    });
})();
