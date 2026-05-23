# Writing a Kestrel plugin

Plugins extend an agent's capabilities with vendor-specific tools the AI can call via `plugin_invoke`. They're long-lived executables that speak **JSON-RPC over stdio** and live in `~/.kestrel/plugins/` on the agent's machine.

## Lifecycle

When an agent (re)connects to its hub, it scans `~/.kestrel/plugins/` for executables, spawns each one, and immediately calls `info()` with a 5-second timeout. Plugins that fail to start or respond are logged and skipped — the agent keeps running.

For each plugin that responds, the agent holds the process for the connection's lifetime, serializing requests through a per-plugin mutex (no interleaving). When the agent disconnects, the plugin process is killed. The next reconnect re-discovers and re-spawns.

## Wire ABI

One request per line on stdin → one response per line on stdout. Each line is a JSON object.

### Required methods

#### `info()`

```jsonc
// agent → plugin
{ "jsonrpc": "2.0", "id": 1, "method": "info", "params": null }

// plugin → agent
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "name": "myapp",
    "version": "1.0.0",
    "description": "Tools for MyApp Inc.'s editor",
    "tools": ["select_layer", "rasterize", "export_png"]
  }
}
```

Returned ONCE at boot. Static across the plugin's lifetime; if you need to advertise different tools per-session, return the superset here.

#### `call(tool, args)`

```jsonc
// agent → plugin
{
  "jsonrpc": "2.0",
  "id": 7,
  "method": "call",
  "params": {
    "tool": "select_layer",
    "args": { "name": "Background" }
  }
}

// plugin → agent
{
  "jsonrpc": "2.0",
  "id": 7,
  "result": { "ok": true, "previous": "Layer 2" }
}
```

`args` is whatever the AI passed in `plugin_invoke({plugin, tool, args_json: "..."})`. Your plugin defines the schema; the agent doesn't parse it.

### Error responses

```jsonc
{
  "jsonrpc": "2.0",
  "id": 7,
  "error": "unknown tool 'rasterize'"
}
```

The agent surfaces this as a typed `Payload::Error { code: Internal, message }` back to the hub, which propagates as an `McpError` to the AI.

## Process model

- **stdout** is reserved for JSON-RPC. Don't `println!` anything else.
- **stderr** is forwarded to the agent's tracing layer — use it for debug logs the operator might want to see.
- **stdin** is line-oriented; flush after every response (`println!` does this implicitly in Rust; Python needs `print(..., flush=True)`).
- The process is killed (SIGKILL) when the agent disconnects. Don't rely on graceful shutdown.

## Discovery rules

The agent enumerates `~/.kestrel/plugins/` (defaulting to `$HOME/.kestrel/plugins`) and considers any file with the executable bit set on Unix (`.exe` extension on Windows). Symlinks are followed. Directories are skipped.

## Concrete example: a Python plugin

```python
#!/usr/bin/env python3
import json
import sys

def info():
    return {
        "name": "demo",
        "version": "0.1.0",
        "description": "Echoes back whatever you give it",
        "tools": ["echo"],
    }

def call(tool, args):
    if tool == "echo":
        return {"echoed": args}
    return {"error": f"unknown tool {tool}"}

for line in sys.stdin:
    try:
        req = json.loads(line)
        method = req.get("method")
        if method == "info":
            result = info()
        elif method == "call":
            params = req.get("params", {})
            result = call(params.get("tool"), params.get("args"))
        else:
            print(json.dumps({"jsonrpc": "2.0", "id": req.get("id"), "error": f"unknown method {method}"}), flush=True)
            continue
        print(json.dumps({"jsonrpc": "2.0", "id": req.get("id"), "result": result}), flush=True)
    except Exception as e:
        print(json.dumps({"jsonrpc": "2.0", "id": None, "error": str(e)}), flush=True)
```

Save as `~/.kestrel/plugins/demo`, `chmod +x demo`, restart the agent. The AI can now call:

```jsonc
plugin_list("my-mac")
// → [{ name: "demo", version: "0.1.0", description: "...", tools: ["echo"] }]

plugin_invoke("my-mac", "demo", "echo", '{"hello":"world"}')
// → '{"echoed":{"hello":"world"}}'
```

## Security considerations

Plugins inherit the agent's privilege (typically the desktop user's). Treat them like any other binary on your machine — only install plugins you trust. The agent does NOT sandbox plugins; that's a future direction worth taking via a per-plugin seatbelt profile or container.
