// crates/kestrel-hub/src/mcp.rs
use std::sync::Arc;

use base64::{Engine, engine::general_purpose};
use kestrel_proto::{Button, KeyCode};
use rmcp::{
    ErrorData as McpError,
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo},
    schemars,
    tool, tool_handler, tool_router,
};

use crate::router::NodeRegistry;

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
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
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
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
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
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
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
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
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
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
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
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text("ok")]))
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
}
