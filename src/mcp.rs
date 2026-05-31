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
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::ErrorData as McpError;
use std::borrow::Cow;
use std::path::{Path, PathBuf};
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

/// A live per-turn MCP server: the rmcp `StreamableHttpService` on an ephemeral port, the receiver
/// of the agent's tool calls, and the temp `--mcp-config` file claude is pointed at. Dropping it
/// shuts the server down (abort) and deletes the temp file — so a reaped/finished suspension that
/// owns this handle tears everything down.
pub struct McpServer {
    port: u16,
    config_path: PathBuf,
    pub calls: mpsc::UnboundedReceiver<PendingToolCall>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<()>,
}

impl McpServer {
    pub async fn start(defs: Vec<ToolDef>) -> std::io::Result<Self> {
        let (calls_tx, calls) = mpsc::unbounded_channel::<PendingToolCall>();
        let template = McpBridge::new(defs, calls_tx);
        let service = StreamableHttpService::new(
            move || Ok(template.clone()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default(),
        );
        let app = axum::Router::new().nest_service("/mcp", service);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let config_path = std::env::temp_dir()
            .join(format!("llm-bridge-mcp-{}.json", uuid::Uuid::new_v4().simple()));
        let body = serde_json::json!({
            "mcpServers": { "llm-bridge": { "type": "http", "url": format!("http://127.0.0.1:{port}/mcp") } }
        });
        std::fs::write(&config_path, serde_json::to_vec(&body)?)?;

        Ok(McpServer { port, config_path, calls, shutdown: Some(shutdown_tx), join })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }
}

impl Drop for McpServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.join.abort();
        let _ = std::fs::remove_file(&self.config_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openai::{FunctionDef, ToolDef};
    use rmcp::model::CallToolRequestParams;
    use rmcp::{serve_client, serve_server};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn server_binds_and_writes_strict_http_config() {
        let server = McpServer::start(vec![def("search")]).await.unwrap();
        assert!(server.port() > 0);
        let cfg = std::fs::read_to_string(server.config_path()).unwrap();
        let v: serde_json::Value = serde_json::from_str(&cfg).unwrap();
        assert_eq!(v["mcpServers"]["llm-bridge"]["type"], "http");
        assert_eq!(v["mcpServers"]["llm-bridge"]["url"], format!("http://127.0.0.1:{}/mcp", server.port()));
        let path = server.config_path().to_path_buf();
        drop(server); // RAII: shuts the server down and deletes the temp config
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!path.exists(), "temp mcp-config should be removed on drop");
    }

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
