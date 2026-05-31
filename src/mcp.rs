//! The in-process MCP server bridging OpenAI `tools` to claude (spec §4.6). `list_tools` exposes the
//! request's tool defs; `call_tool` mints a high-entropy `tool_call_id`, emits a `PendingToolCall` to
//! the orchestrator, and PARKS on a `oneshot` until the client's tool result is delivered. The
//! handler is `Clone` so rmcp's per-session `service_factory` can mint one per MCP session.
use crate::openai::ToolDef;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, ListToolsResult, PaginatedRequestParams,
    ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::ErrorData as McpError;
use std::borrow::Cow;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// A tool call the agent made. The orchestrator collects it as `AgentEvent::ToolCall` and later
/// delivers the client's result through `reply` (firing the parked `call_tool`).
pub struct PendingToolCall {
    pub id: String,
    pub name: String,
    pub args: String, // JSON-encoded arguments
    pub reply: oneshot::Sender<String>,
}

/// A globally-unique, high-entropy tool_call_id (spec §4.6: random UUID, NOT a per-turn counter).
pub fn new_tool_call_id() -> String {
    format!("call_{}", uuid::Uuid::new_v4().simple())
}

/// Build an rmcp `Tool` from an OpenAI tool definition. A missing description stays `None` (the
/// field is omitted from `tools/list`) rather than becoming an empty string.
pub fn tool_from_def(def: &ToolDef) -> Tool {
    let schema = def.function.parameters.as_object().cloned().unwrap_or_default();
    let description = def.function.description.clone().map(Cow::Owned);
    Tool::new_with_raw(def.function.name.clone(), description, Arc::new(schema))
}

#[derive(Clone)]
pub struct McpBridge {
    tools: Arc<Vec<Tool>>,
    calls_tx: mpsc::UnboundedSender<PendingToolCall>,
}

impl McpBridge {
    pub fn new(defs: Vec<ToolDef>, calls_tx: mpsc::UnboundedSender<PendingToolCall>) -> Self {
        let tools = defs.iter().map(tool_from_def).collect();
        McpBridge { tools: Arc::new(tools), calls_tx }
    }
}

impl ServerHandler for McpBridge {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        let tools = (*self.tools).clone();
        async move { Ok(ListToolsResult::with_all_items(tools)) }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        let id = new_tool_call_id();
        let name = request.name.to_string();
        let args = request
            .arguments
            .map(|a| serde_json::Value::Object(a).to_string())
            .unwrap_or_else(|| "{}".to_string());
        let calls_tx = self.calls_tx.clone();
        async move {
            let (reply, rx) = oneshot::channel();
            calls_tx
                .send(PendingToolCall { id, name, args, reply })
                .map_err(|_| McpError::internal_error("bridge orchestrator gone", None))?;
            let result = rx
                .await
                .map_err(|_| McpError::internal_error("tool result channel dropped (timed out?)", None))?;
            Ok(CallToolResult::success(vec![Content::text(result)]))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openai::{FunctionDef, ToolDef};
    use rmcp::model::CallToolRequestParams;
    use rmcp::{serve_client, serve_server};
    use tokio::sync::mpsc;

    fn def(name: &str) -> ToolDef {
        ToolDef { kind: "function".into(), function: FunctionDef {
            name: name.into(), description: Some("d".into()),
            parameters: serde_json::json!({"type":"object","properties":{"q":{"type":"string"}}}),
        }}
    }

    #[test]
    fn ids_are_high_entropy_and_unique() {
        let a = new_tool_call_id();
        let b = new_tool_call_id();
        assert!(a.starts_with("call_") && a.len() > 16);
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn list_tools_exposes_defs_and_call_tool_parks_then_returns() {
        let (calls_tx, mut calls_rx) = mpsc::unbounded_channel::<PendingToolCall>();
        let bridge = McpBridge::new(vec![def("search")], calls_tx);

        let (s_io, c_io) = tokio::io::duplex(8 * 1024);
        let server_task = tokio::spawn(async move { serve_server(bridge, s_io).await });
        let client = serve_client((), c_io).await.unwrap();
        let server = server_task.await.unwrap().unwrap();

        let deliver = tokio::spawn(async move {
            let call = calls_rx.recv().await.unwrap();
            assert_eq!(call.name, "search");
            assert!(call.args.contains("rust"));
            assert!(call.id.starts_with("call_"));
            call.reply.send(format!("RESULT:{}", call.id)).unwrap();
            call.id
        });

        let tools = client.list_tools(None).await.unwrap();
        assert_eq!(tools.tools.len(), 1);
        assert_eq!(tools.tools[0].name, "search");

        let params = CallToolRequestParams::new("search")
            .with_arguments(serde_json::json!({"q":"rust"}).as_object().unwrap().clone());
        let result = client.call_tool(params).await.unwrap();
        let text = result.content.iter().find_map(|c| c.as_text().map(|t| t.text.clone())).unwrap();
        let delivered_id = deliver.await.unwrap();
        assert_eq!(text, format!("RESULT:{delivered_id}"));

        client.cancel().await.unwrap();
        server.cancel().await.unwrap();
    }
}
