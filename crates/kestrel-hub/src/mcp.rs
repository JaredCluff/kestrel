// crates/kestrel-hub/src/mcp.rs
use std::sync::Arc;

use base64::{Engine, engine::general_purpose};
use kestrel_proto::{Button, ClipboardContent, KeyCode};
use rmcp::{
    ErrorData as McpError,
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo},
    schemars,
    tool, tool_handler, tool_router,
};

use crate::router::NodeRegistry;

// ── Arg types ─────────────────────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ScreenshotArgs {
    pub node_id: String,
    pub display: Option<u8>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct TypeTextArgs {
    pub node_id: String,
    pub text: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct KeyComboArgs {
    pub node_id: String,
    pub keys: Vec<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct MouseMoveArgs {
    pub node_id: String,
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct MouseClickArgs {
    pub node_id: String,
    pub button: String,
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ScrollArgs {
    pub node_id: String,
    pub dx: f64,
    pub dy: f64,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct NodeIdArgs {
    pub node_id: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ClipboardWriteArgs {
    pub node_id: String,
    pub text: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ShellRunArgs {
    pub node_id: String,
    pub command: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ShellOpenArgs {
    pub node_id: String,
    pub shell: Option<String>,
    pub cols: Option<u16>,
    pub rows: Option<u16>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ShellWriteArgs {
    pub node_id: String,
    pub pty_id: u32,
    pub data: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ShellPtyArgs {
    pub node_id: String,
    pub pty_id: u32,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DescribeArgs {
    pub node_id: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct WorldDiffArgs {
    pub node_id: String,
    /// Return the world state IFF it was observed strictly after
    /// `since_unix_secs`. Unix seconds since epoch. Pass 0 to get the
    /// current state regardless of when it was observed.
    pub since_unix_secs: u64,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct JobStartShellArgs {
    pub node_id: String,
    pub command: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct JobIdArgs {
    pub job_id: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct JobOutputArgs {
    pub job_id: String,
    /// Byte offset within the job's accumulated output to start
    /// reading from. Pass 0 on the first call; pass the returned
    /// `new_offset` on subsequent calls to stream incrementally.
    pub since_offset: usize,
}

// ── Server ────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct KestrelMcp {
    registry: Arc<NodeRegistry>,
    /// Phase 7: shared job registry. Created here so every
    /// MCP-invocation-routed handler can dispatch async jobs.
    jobs: crate::jobs::JobRegistry,
    audit: crate::audit::AuditLogger,
    tool_router: ToolRouter<KestrelMcp>,
}

#[tool_router]
impl KestrelMcp {
    pub fn new(registry: Arc<NodeRegistry>) -> Self {
        Self::with_audit(registry, crate::audit::AuditLogger::disabled())
    }

    /// Construct with a specific audit logger. `kestrel-hub start` wires
    /// a file-backed logger here; tests use [`AuditLogger::disabled`].
    pub fn with_audit(registry: Arc<NodeRegistry>, audit: crate::audit::AuditLogger) -> Self {
        let jobs = crate::jobs::JobRegistry::new(registry.clone());
        KestrelMcp {
            registry,
            jobs,
            audit,
            tool_router: Self::tool_router(),
        }
    }

    /// Helper that wraps a tool body with timing + audit logging.
    /// Captures start time, runs `work`, then emits one audit entry
    /// regardless of success or failure. The entry's status mirrors the
    /// outer `Result`. Cost when audit is disabled is just one timing
    /// instant + the function call — no allocation, no I/O.
    async fn audit_call<F, Fut, T>(
        &self,
        op: &'static str,
        node_id: &str,
        args_summary: String,
        work: F,
    ) -> Result<T, McpError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, McpError>>,
    {
        let start = std::time::Instant::now();
        let result = work().await;
        let dur_ms = start.elapsed().as_millis() as u64;
        let (status, err_msg) = match &result {
            Ok(_) => (crate::audit::CallStatus::Ok, None),
            Err(e) => (crate::audit::CallStatus::Error, Some(format!("{:?}", e))),
        };
        self.audit
            .log(op, node_id, &args_summary, status, dur_ms, err_msg.as_deref())
            .await;
        result
    }

    /// Wrap a registry-level error with operation context and a remediation hint
    /// when the failure mode is "node not connected".
    fn node_err(op: &str, node_id: &str, e: anyhow::Error) -> McpError {
        let msg = e.to_string();
        let hint = if msg.contains("not connected") {
            format!(" (hint: check that '{}' is online — see http://<hub>:dashboard or run `kestrel-hub status`)", node_id)
        } else {
            String::new()
        };
        McpError::internal_error(
            format!("{op} on '{node_id}': {msg}{hint}"),
            None,
        )
    }

    // ── Phase 2 tools ─────────────────────────────────────────────────────────

    #[tool(description = "List all connected nodes with their OS and hostname")]
    async fn fleet_nodes(&self) -> Result<CallToolResult, McpError> {
        self.audit_call("fleet_nodes", "*", String::new(), || async {
            let nodes = self.registry.list().await;
            let json = serde_json::to_string_pretty(&nodes)
                .unwrap_or_else(|e| format!("error: {e}"));
            Ok(CallToolResult::success(vec![Content::text(json)]))
        })
        .await
    }

    #[tool(description = "Take a PNG screenshot of a node display")]
    async fn screenshot(
        &self,
        Parameters(args): Parameters<ScreenshotArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        let summary = format!("display={}", args.display.unwrap_or(0));
        self.audit_call("screenshot", &node_id, summary, || async move {
            let png = self
                .registry
                .screenshot(&args.node_id, args.display.unwrap_or(0), None)
                .await
                .map_err(|e| Self::node_err("screenshot", &args.node_id, e))?;
            if png.is_empty() {
                return Err(McpError::internal_error(
                    "screenshot returned empty bytes".to_string(),
                    None,
                ));
            }
            let b64 = general_purpose::STANDARD.encode(&png);
            Ok(CallToolResult::success(vec![Content::image(b64, "image/png")]))
        })
        .await
    }

    #[tool(description = "Type text on a node (Unicode-safe)")]
    async fn type_text(
        &self,
        Parameters(args): Parameters<TypeTextArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        // Don't log the typed text itself — it might contain passwords or
        // other secrets the operator typed. Log only the length.
        let summary = format!("len={}", args.text.chars().count());
        self.audit_call("type_text", &node_id, summary, || async move {
            self.registry
                .type_text(&args.node_id, args.text)
                .await
                .map_err(|e| Self::node_err("type_text", &args.node_id, e))?;
            Ok(CallToolResult::success(vec![Content::text("ok")]))
        })
        .await
    }

    #[tool(description = "Press a key combination on a node, e.g. [\"ctrl\", \"c\"]")]
    async fn key_combo(
        &self,
        Parameters(args): Parameters<KeyComboArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        let summary = format!("keys={:?}", args.keys);
        self.audit_call("key_combo", &node_id, summary, || async move {
            let keys: Vec<KeyCode> = args
                .keys
                .iter()
                .map(|s| kestrel_proto::parse_key_str(s))
                .collect::<anyhow::Result<Vec<_>>>()
                .map_err(|e| McpError::invalid_params(e.to_string(), None))?;
            self.registry
                .key_combo(&args.node_id, keys)
                .await
                .map_err(|e| Self::node_err("key_combo", &args.node_id, e))?;
            Ok(CallToolResult::success(vec![Content::text("ok")]))
        })
        .await
    }

    #[tool(description = "Move the mouse to normalized coordinates (0.0-1.0) on a node")]
    async fn mouse_move(
        &self,
        Parameters(args): Parameters<MouseMoveArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        let summary = format!("x={},y={}", args.x, args.y);
        self.audit_call("mouse_move", &node_id, summary, || async move {
            self.registry
                .mouse_move(&args.node_id, args.x, args.y)
                .await
                .map_err(|e| Self::node_err("mouse_move", &args.node_id, e))?;
            Ok(CallToolResult::success(vec![Content::text("ok")]))
        })
        .await
    }

    #[tool(description = "Click a mouse button at normalized coordinates on a node")]
    async fn mouse_click(
        &self,
        Parameters(args): Parameters<MouseClickArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        let summary = format!("button={},x={},y={}", args.button, args.x, args.y);
        self.audit_call("mouse_click", &node_id, summary, || async move {
            let button = match args.button.to_lowercase().as_str() {
                "left" => Button::Left,
                "right" => Button::Right,
                "middle" => Button::Middle,
                other => {
                    return Err(McpError::invalid_params(
                        format!("unknown button '{}'; use left, right, or middle", other),
                        None,
                    ))
                }
            };
            self.registry
                .mouse_click(&args.node_id, button, args.x, args.y)
                .await
                .map_err(|e| Self::node_err("mouse_click", &args.node_id, e))?;
            Ok(CallToolResult::success(vec![Content::text("ok")]))
        })
        .await
    }

    #[tool(description = "Scroll on a node (dy > 0 scrolls down, dx > 0 scrolls right)")]
    async fn scroll(
        &self,
        Parameters(args): Parameters<ScrollArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        let summary = format!("dx={},dy={}", args.dx, args.dy);
        self.audit_call("scroll", &node_id, summary, || async move {
            self.registry
                .scroll(&args.node_id, args.dx, args.dy)
                .await
                .map_err(|e| Self::node_err("scroll", &args.node_id, e))?;
            Ok(CallToolResult::success(vec![Content::text("ok")]))
        })
        .await
    }

    // ── Phase 3 clipboard tools ───────────────────────────────────────────────

    #[tool(description = "Read the clipboard text from a node. Returns the clipboard content as text.")]
    async fn clipboard_read(
        &self,
        Parameters(args): Parameters<NodeIdArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        self.audit_call("clipboard_read", &node_id, String::new(), || async move {
            let content = self.registry
                .clipboard_read(&args.node_id)
                .await
                .map_err(|e| Self::node_err("clipboard_read", &args.node_id, e))?;
            let text = match content {
                ClipboardContent::Text(t) => t,
                ClipboardContent::Image { width, height, .. } => {
                    format!("[image {}x{}]", width, height)
                }
            };
            Ok(CallToolResult::success(vec![Content::text(text)]))
        })
        .await
    }

    #[tool(description = "Write text to the clipboard on a node")]
    async fn clipboard_write(
        &self,
        Parameters(args): Parameters<ClipboardWriteArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        // Length only — clipboard payloads can be secrets.
        let summary = format!("len={}", args.text.chars().count());
        self.audit_call("clipboard_write", &node_id, summary, || async move {
            self.registry
                .clipboard_write(&args.node_id, ClipboardContent::Text(args.text))
                .await
                .map_err(|e| Self::node_err("clipboard_write", &args.node_id, e))?;
            Ok(CallToolResult::success(vec![Content::text("ok")]))
        })
        .await
    }

    // ── Phase 3 shell tools ───────────────────────────────────────────────────

    #[tool(description = "Run a shell command on a node and return its output. Timeout: 30 seconds.")]
    async fn shell_run(
        &self,
        Parameters(args): Parameters<ShellRunArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        // The command IS logged — it's the highest-value audit signal
        // (which shell command did the operator run on which box?).
        // Operators who don't want commands logged should not point
        // the audit log at a world-readable file.
        let summary = format!("command={}", args.command);
        self.audit_call("shell_run", &node_id, summary, || async move {
            let output = self.registry
                .run_shell(&args.node_id, &args.command)
                .await
                .map_err(|e| Self::node_err("shell_run", &args.node_id, e))?;
            Ok(CallToolResult::success(vec![Content::text(output)]))
        })
        .await
    }

    #[tool(description = "Open an interactive PTY shell on a node. Returns a pty_id for subsequent writes/reads.")]
    async fn shell_open(
        &self,
        Parameters(args): Parameters<ShellOpenArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        let summary = format!(
            "shell={:?},cols={},rows={}",
            args.shell.as_deref().unwrap_or("$SHELL"),
            args.cols.unwrap_or(80),
            args.rows.unwrap_or(24)
        );
        self.audit_call("shell_open", &node_id, summary, || async move {
            let pty_id = self.registry
                .shell_open(
                    &args.node_id,
                    args.shell,
                    args.cols.unwrap_or(80),
                    args.rows.unwrap_or(24),
                )
                .await
                .map_err(|e| Self::node_err("shell_open", &args.node_id, e))?;
            Ok(CallToolResult::success(vec![Content::text(pty_id.to_string())]))
        })
        .await
    }

    #[tool(description = "Write text to an interactive PTY shell opened with shell_open")]
    async fn shell_write(
        &self,
        Parameters(args): Parameters<ShellWriteArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        // Same secrecy reasoning as type_text and clipboard_write —
        // log only the pty + length, never the bytes.
        let summary = format!("pty={},len={}", args.pty_id, args.data.len());
        self.audit_call("shell_write", &node_id, summary, || async move {
            self.registry
                .shell_write(&args.node_id, args.pty_id, args.data.into_bytes())
                .await
                .map_err(|e| Self::node_err("shell_write", &args.node_id, e))?;
            Ok(CallToolResult::success(vec![Content::text("ok")]))
        })
        .await
    }

    #[tool(description = "Read buffered output from an interactive PTY shell. Drains the buffer.")]
    async fn shell_read(
        &self,
        Parameters(args): Parameters<ShellPtyArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        let summary = format!("pty={}", args.pty_id);
        self.audit_call("shell_read", &node_id, summary, || async move {
            let raw = self.registry
                .shell_read(&args.node_id, args.pty_id)
                .await
                .map_err(|e| Self::node_err("shell_read", &args.node_id, e))?;
            let text = String::from_utf8_lossy(&raw).into_owned();
            Ok(CallToolResult::success(vec![Content::text(text)]))
        })
        .await
    }

    #[tool(description = "Close an interactive PTY shell opened with shell_open")]
    async fn shell_close(
        &self,
        Parameters(args): Parameters<ShellPtyArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        let summary = format!("pty={}", args.pty_id);
        self.audit_call("shell_close", &node_id, summary, || async move {
            self.registry
                .shell_close(&args.node_id, args.pty_id)
                .await
                .map_err(|e| Self::node_err("shell_close", &args.node_id, e))?;
            Ok(CallToolResult::success(vec![Content::text("ok")]))
        })
        .await
    }

    // ── Phase 4 accessibility tool ────────────────────────────────────────────

    #[tool(description = "Get the accessibility tree of the focused application on a node. Returns a JSON `AccessibilityNode` (`role`, `label`, `value`, `focused`, `enabled`, `bounds`, `children`, `fallback`) walked up to 5 levels deep on macOS (via the AX API), Linux (via AT-SPI / D-Bus), and Windows (via UI Automation). When the platform AX call fails (permission denied, AT-SPI bus down, COM init refused, unsupported OS), the response has `fallback: true` and an empty `children` array — call `screenshot` instead.")]
    async fn describe(
        &self,
        Parameters(args): Parameters<DescribeArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        self.audit_call("describe", &node_id, String::new(), || async move {
            // The proto carries a `display` field on DescribeReq for future use,
            // but the agent's AX walker always describes the focused application
            // regardless of display, so we always send 0.
            let tree = self
                .registry
                .describe(&args.node_id, 0)
                .await
                .map_err(|e| Self::node_err("describe", &args.node_id, e))?;
            let json = serde_json::to_string_pretty(&tree)
                .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e));
            Ok(CallToolResult::success(vec![Content::text(json)]))
        })
        .await
    }

    // ── Phase 6 world-state tools ─────────────────────────────────────────

    #[tool(description = "Get a structured snapshot of what's currently happening on a node: focused application name/pid, mouse position, displays, clipboard metadata (kind + length + 16-hex SHA-256 fingerprint, NEVER the content), open shell session metadata, and the unix timestamp the agent observed at. Cheaper than `screenshot` or `describe` for the common case of \"check what's going on without re-imaging.\" Returns null when the hub has no observation yet (fresh connect; agent's first WorldObserver tick hasn't completed).")]
    async fn world_state(
        &self,
        Parameters(args): Parameters<NodeIdArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        self.audit_call("world_state", &node_id, String::new(), || async move {
            match self.registry.world_state_for(&args.node_id).await {
                Some(state) => {
                    let json = serde_json::to_string_pretty(&state)
                        .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e));
                    Ok(CallToolResult::success(vec![Content::text(json)]))
                }
                None => Ok(CallToolResult::success(vec![Content::text("null")])),
            }
        })
        .await
    }

    #[tool(description = "Get the world-state snapshot for a node IFF it was observed strictly after `since_unix_secs`. Returns the text literal \"null\" when nothing has changed since that point. Lets agents poll cheaply for \"anything new?\" between turns. Pass `since_unix_secs=0` to always get the current state.")]
    async fn world_diff_since(
        &self,
        Parameters(args): Parameters<WorldDiffArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        let summary = format!("since={}", args.since_unix_secs);
        self.audit_call("world_diff_since", &node_id, summary, || async move {
            match self
                .registry
                .world_diff_since(&args.node_id, args.since_unix_secs)
                .await
            {
                Some(state) => {
                    let json = serde_json::to_string_pretty(&state)
                        .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e));
                    Ok(CallToolResult::success(vec![Content::text(json)]))
                }
                None => Ok(CallToolResult::success(vec![Content::text("null")])),
            }
        })
        .await
    }

    // ── Phase 7 async job tools ───────────────────────────────────────────

    #[tool(description = "Start a shell command as an async job. Returns a job_id immediately; poll with `job_status` and `job_output` to track. Use this instead of `shell_run` when a command might take longer than 30 seconds. The underlying agent operation still runs to completion even if the AI's turn ends — use `job_cancel` to stop one that's no longer needed.")]
    async fn job_start_shell(
        &self,
        Parameters(args): Parameters<JobStartShellArgs>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = args.node_id.clone();
        let summary = format!("command={}", args.command);
        self.audit_call("job_start_shell", &node_id, summary, || async move {
            let job_id = self
                .jobs
                .start_shell(args.node_id.clone(), args.command.clone())
                .await;
            Ok(CallToolResult::success(vec![Content::text(job_id)]))
        })
        .await
    }

    #[tool(description = "Get the current status of an async job: pending / running / completed / failed / cancelled. Includes exit_code (for completed shell jobs), error message (for failed jobs), created_unix and completed_unix timestamps, and the args_summary. Output bytes are NOT in this response — use `job_output` to stream them.")]
    async fn job_status(
        &self,
        Parameters(args): Parameters<JobIdArgs>,
    ) -> Result<CallToolResult, McpError> {
        let job_id = args.job_id.clone();
        self.audit_call("job_status", &job_id, String::new(), || async move {
            match self.jobs.status(&args.job_id).await {
                Some(job) => {
                    let json = serde_json::to_string_pretty(&job)
                        .unwrap_or_else(|e| format!("{{\"error\":\"{}\"}}", e));
                    Ok(CallToolResult::success(vec![Content::text(json)]))
                }
                None => Err(McpError::invalid_params(
                    format!("unknown job_id '{}'", args.job_id),
                    None,
                )),
            }
        })
        .await
    }

    #[tool(description = "Stream output from an async job starting at `since_offset`. Returns the bytes since that offset (UTF-8 lossily decoded) plus a `new_offset` to pass on the next call. Returns an empty string when no new bytes have arrived since `since_offset`. Pass 0 on the first call to get everything so far.")]
    async fn job_output(
        &self,
        Parameters(args): Parameters<JobOutputArgs>,
    ) -> Result<CallToolResult, McpError> {
        let job_id = args.job_id.clone();
        let summary = format!("since={}", args.since_offset);
        self.audit_call("job_output", &job_id, summary, || async move {
            match self.jobs.output_since(&args.job_id, args.since_offset).await {
                Some((bytes, new_offset)) => {
                    let text = String::from_utf8_lossy(&bytes).into_owned();
                    // Return a small JSON envelope so callers can grab the
                    // new offset programmatically without parsing the
                    // output for sentinels.
                    let json = serde_json::json!({
                        "text": text,
                        "new_offset": new_offset,
                    });
                    Ok(CallToolResult::success(vec![Content::text(json.to_string())]))
                }
                None => Err(McpError::invalid_params(
                    format!("unknown job_id '{}'", args.job_id),
                    None,
                )),
            }
        })
        .await
    }

    #[tool(description = "Mark an async job as cancelled. Does NOT abort the underlying agent operation (that's a follow-up); for shell jobs the existing 30-second timeout on the agent caps any runaway. Output captured up to the cancel point stays available via `job_output`.")]
    async fn job_cancel(
        &self,
        Parameters(args): Parameters<JobIdArgs>,
    ) -> Result<CallToolResult, McpError> {
        let job_id = args.job_id.clone();
        self.audit_call("job_cancel", &job_id, String::new(), || async move {
            let cancelled = self.jobs.cancel(&args.job_id).await;
            Ok(CallToolResult::success(vec![Content::text(
                cancelled.to_string(),
            )]))
        })
        .await
    }

    // ── Phase 8 capability-aware routing ──────────────────────────────────

    #[tool(description = "Find connected nodes matching a capability predicate. Any field in `needs` is optional; nodes match when every supplied field matches their reported capability. Example: { has_gpu: true, has_display: true } returns node_ids of nodes that have both. Empty `needs` returns all nodes with a recorded capability. Returns a JSON array of node_id strings, sorted alphabetically.")]
    async fn fleet_find(
        &self,
        Parameters(needs): Parameters<crate::router::CapabilityNeeds>,
    ) -> Result<CallToolResult, McpError> {
        let summary = format!("{:?}", needs);
        self.audit_call("fleet_find", "*", summary, || async move {
            let matches = self.registry.find_nodes_with(&needs).await;
            let json = serde_json::to_string(&matches)
                .unwrap_or_else(|e| format!("[\"error: {}\"]", e));
            Ok(CallToolResult::success(vec![Content::text(json)]))
        })
        .await
    }
}

#[tool_handler]
impl ServerHandler for KestrelMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::NodeRegistry;
    use std::sync::Arc;

    #[test]
    fn mcp_server_constructs() {
        let registry = Arc::new(NodeRegistry::new());
        let _server = KestrelMcp::new(registry);
    }

    #[test]
    fn node_err_includes_op_and_node_id() {
        let e = anyhow::anyhow!("boom");
        let mcp_err = KestrelMcp::node_err("screenshot", "macstudio", e);
        let msg = format!("{:?}", mcp_err);
        assert!(msg.contains("screenshot"), "expected op in error, got: {}", msg);
        assert!(msg.contains("macstudio"), "expected node_id in error, got: {}", msg);
    }

    #[test]
    fn node_err_appends_hint_when_not_connected() {
        let e = anyhow::anyhow!("node 'mbp' not connected");
        let mcp_err = KestrelMcp::node_err("shell_run", "mbp", e);
        let msg = format!("{:?}", mcp_err);
        assert!(msg.contains("hint:"), "expected remediation hint, got: {}", msg);
    }

    #[test]
    fn node_err_no_hint_on_other_errors() {
        let e = anyhow::anyhow!("websocket frame too large");
        let mcp_err = KestrelMcp::node_err("screenshot", "macstudio", e);
        let msg = format!("{:?}", mcp_err);
        assert!(!msg.contains("hint:"), "did not expect hint for unrelated error, got: {}", msg);
    }

    // ── Phase 6 world-state MCP tool tests ────────────────────────────────

    fn seed_world(reg: &NodeRegistry, node_id: &str, app: &str, ts: u64) {
        use kestrel_proto::{FocusedApp, WorldState};
        // Direct construction; observe_world_update is the public API
        // but it's async + broadcasts events the test doesn't need.
        // The smoke property we want is that the tool calls
        // world_state_for, which reads from the cache regardless of
        // how it got there. Using the public API would be 1:1
        // equivalent here.
        let state = WorldState {
            focused_app: Some(FocusedApp {
                name: app.into(),
                pid: 1,
                window_title: None,
            }),
            mouse: None,
            displays: vec![],
            clipboard: None,
            shells: vec![],
            last_observed_unix: ts,
        };
        // Block on the async API from this sync helper via
        // `tokio::runtime::Handle::current()` so callers inside
        // #[tokio::test] just call `seed_world` synchronously.
        let r = reg.clone();
        let n = node_id.to_string();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                r.observe_world_update(&n, state).await;
            });
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn world_state_for_known_node_returns_json() {
        let registry = Arc::new(NodeRegistry::new());
        seed_world(&registry, "alpha", "Safari", 1700000000);
        // Read back via the registry (the public API the tool uses).
        let state = registry.world_state_for("alpha").await.unwrap();
        let json = serde_json::to_string(&state).unwrap();
        assert!(json.contains("\"name\":\"Safari\""));
        assert!(json.contains("\"last_observed_unix\":1700000000"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn world_state_for_unknown_node_is_none() {
        let registry = Arc::new(NodeRegistry::new());
        assert!(registry.world_state_for("ghost").await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn world_diff_since_strict_after_only() {
        let registry = Arc::new(NodeRegistry::new());
        seed_world(&registry, "alpha", "Safari", 100);
        // since == last_observed → returns None (strict >).
        assert!(registry.world_diff_since("alpha", 100).await.is_none());
        // since < last_observed → returns the state.
        assert!(registry.world_diff_since("alpha", 50).await.is_some());
        // since > last_observed → returns None.
        assert!(registry.world_diff_since("alpha", 200).await.is_none());
    }
}
