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

// ── Server ────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct KestrelMcp {
    registry: Arc<NodeRegistry>,
    tool_router: ToolRouter<KestrelMcp>,
}

#[tool_router]
impl KestrelMcp {
    pub fn new(registry: Arc<NodeRegistry>) -> Self {
        KestrelMcp {
            registry,
            tool_router: Self::tool_router(),
        }
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
        let nodes = self.registry.list().await;
        let json = serde_json::to_string_pretty(&nodes)
            .unwrap_or_else(|e| format!("error: {e}"));
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(description = "Take a PNG screenshot of a node display")]
    async fn screenshot(
        &self,
        Parameters(args): Parameters<ScreenshotArgs>,
    ) -> Result<CallToolResult, McpError> {
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
    }

    #[tool(description = "Type text on a node (Unicode-safe)")]
    async fn type_text(
        &self,
        Parameters(args): Parameters<TypeTextArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .type_text(&args.node_id, args.text)
            .await
            .map_err(|e| Self::node_err("type_text", &args.node_id, e))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    #[tool(description = "Press a key combination on a node, e.g. [\"ctrl\", \"c\"]")]
    async fn key_combo(
        &self,
        Parameters(args): Parameters<KeyComboArgs>,
    ) -> Result<CallToolResult, McpError> {
        let keys: Vec<KeyCode> = args
            .keys
            .iter()
            .map(|s| crate::capabilities_parse::parse_key_str(s))
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|e| McpError::invalid_params(e.to_string(), None))?;
        self.registry
            .key_combo(&args.node_id, keys)
            .await
            .map_err(|e| Self::node_err("key_combo", &args.node_id, e))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    #[tool(description = "Move the mouse to normalized coordinates (0.0-1.0) on a node")]
    async fn mouse_move(
        &self,
        Parameters(args): Parameters<MouseMoveArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .mouse_move(&args.node_id, args.x, args.y)
            .await
            .map_err(|e| Self::node_err("mouse_move", &args.node_id, e))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    #[tool(description = "Click a mouse button at normalized coordinates on a node")]
    async fn mouse_click(
        &self,
        Parameters(args): Parameters<MouseClickArgs>,
    ) -> Result<CallToolResult, McpError> {
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
    }

    #[tool(description = "Scroll on a node (dy > 0 scrolls down, dx > 0 scrolls right)")]
    async fn scroll(
        &self,
        Parameters(args): Parameters<ScrollArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .scroll(&args.node_id, args.dx, args.dy)
            .await
            .map_err(|e| Self::node_err("scroll", &args.node_id, e))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    // ── Phase 3 clipboard tools ───────────────────────────────────────────────

    #[tool(description = "Read the clipboard text from a node. Returns the clipboard content as text.")]
    async fn clipboard_read(
        &self,
        Parameters(args): Parameters<NodeIdArgs>,
    ) -> Result<CallToolResult, McpError> {
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
    }

    #[tool(description = "Write text to the clipboard on a node")]
    async fn clipboard_write(
        &self,
        Parameters(args): Parameters<ClipboardWriteArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .clipboard_write(&args.node_id, ClipboardContent::Text(args.text))
            .await
            .map_err(|e| Self::node_err("clipboard_write", &args.node_id, e))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    // ── Phase 3 shell tools ───────────────────────────────────────────────────

    #[tool(description = "Run a shell command on a node and return its output. Timeout: 30 seconds.")]
    async fn shell_run(
        &self,
        Parameters(args): Parameters<ShellRunArgs>,
    ) -> Result<CallToolResult, McpError> {
        let output = self.registry
            .run_shell(&args.node_id, &args.command)
            .await
            .map_err(|e| Self::node_err("shell_run", &args.node_id, e))?;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "Open an interactive PTY shell on a node. Returns a pty_id for subsequent writes/reads.")]
    async fn shell_open(
        &self,
        Parameters(args): Parameters<ShellOpenArgs>,
    ) -> Result<CallToolResult, McpError> {
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
    }

    #[tool(description = "Write text to an interactive PTY shell opened with shell_open")]
    async fn shell_write(
        &self,
        Parameters(args): Parameters<ShellWriteArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .shell_write(&args.node_id, args.pty_id, args.data.into_bytes())
            .await
            .map_err(|e| Self::node_err("shell_write", &args.node_id, e))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    #[tool(description = "Read buffered output from an interactive PTY shell. Drains the buffer.")]
    async fn shell_read(
        &self,
        Parameters(args): Parameters<ShellPtyArgs>,
    ) -> Result<CallToolResult, McpError> {
        let raw = self.registry
            .shell_read(&args.node_id, args.pty_id)
            .await
            .map_err(|e| Self::node_err("shell_read", &args.node_id, e))?;
        let text = String::from_utf8_lossy(&raw).into_owned();
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(description = "Close an interactive PTY shell opened with shell_open")]
    async fn shell_close(
        &self,
        Parameters(args): Parameters<ShellPtyArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.registry
            .shell_close(&args.node_id, args.pty_id)
            .await
            .map_err(|e| Self::node_err("shell_close", &args.node_id, e))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
    }

    // ── Phase 4 accessibility tool ────────────────────────────────────────────

    #[tool(description = "Get the accessibility tree of the focused application on a node. Returns a JSON `AccessibilityNode` (`role`, `label`, `value`, `focused`, `enabled`, `bounds`, `children`, `fallback`) walked up to 5 levels deep. macOS-only; on non-macOS or when Accessibility permission is denied, the response has `fallback: true` and an empty `children` array — call `screenshot` instead.")]
    async fn describe(
        &self,
        Parameters(args): Parameters<DescribeArgs>,
    ) -> Result<CallToolResult, McpError> {
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
}
