# llm-bridge Phase 4b — Live MCP Bridge (rmcp server + claude `--mcp-config` + held-open suspensions) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make OpenAI `tools` actually work end-to-end for **claude**: stand up an in-process **`rmcp` MCP server** exposing the request's tools, point the spawned `claude` at it per-invocation via `--mcp-config <temp-file> --strict-mcp-config`, and when the agent calls a tool, **park** the (held-open) claude process inside its MCP call, emit the OpenAI `tool_call`s, register the suspension (the Phase-4a `SuspendedSessions` registry, now driven for real), and resume on the client's tool-result follow-up.

**Architecture:** Per tools-turn, the orchestrator starts a `StreamableHttpService` (rmcp) on an ephemeral `127.0.0.1` port serving an `McpBridge` (`ServerHandler`) whose `call_tool` generates a high-entropy `tool_call_id`, emits a `PendingToolCall`, and **awaits a `oneshot`** (so claude blocks inside the MCP call — the process is held open). The orchestrator collects the turn's `PendingToolCall`s into `AgentEvent::ToolCall`s, responds `finish_reason:"tool_calls"`, and **registers a suspension** whose continuation `EventStream` owns the live claude child + the running MCP server + the per-id `oneshot` senders. The parked suspension releases its active `max_concurrency` permit and occupies a `max_suspended_sessions` slot (`503` on overflow). The follow-up delivers results → fires the `oneshot`s → `call_tool` returns `CallToolResult`s → claude continues → the continuation streams the response; teardown drops the child + server. This is the **only** place a process outlives a single HTTP request (spec §4.6).

**Tech Stack:** **`rmcp` 1.7** (`ServerHandler`, `StreamableHttpService`, `serve_server`/`serve_client`, `Tool`/`CallToolResult`/`Content`) + axum mount + tokio `oneshot`/`mpsc`. Builds on Phase 4a (`SuspendedSessions`, `routing`, `AgentEvent::ToolCall`, tool wire types). Spec: `docs/superpowers/specs/2026-05-31-llm-bridge-design.md` (§3.1, §4.3 step 2, §4.6 MCP bridge + suspended groups, §4.7 accounting, §9 step 8, §10 risks).

---

## Proven by the Phase-4b spike (`phase4b-spike` branch, `examples/mcp_spike.rs`) — rmcp 1.7 API the tasks below rely on
Every rmcp pattern below was compiled + run in the spike. Use these EXACT shapes:

- **Dep:** `rmcp = { version = "1.7", features = ["client", "transport-streamable-http-server", "transport-streamable-http-server-session"] }` (default features add `server`, `macros`, `transport-async-rw`). `client` + `transport-async-rw` are for the hermetic in-memory tests.
- **Imports:** `use rmcp::handler::server::ServerHandler;` · `use rmcp::model::{CallToolRequestParams, CallToolResult, Content, ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool};` · `use rmcp::service::{RequestContext, RoleServer};` · `use rmcp::{serve_client, serve_server, ErrorData as McpError};`
- **`ServerHandler` (manual)** — override these three (each returns `impl Future<…> + Send + '_`, write the body as `async move {…}`):
  - `fn get_info(&self) -> ServerInfo` — `let mut i = ServerInfo::default(); i.capabilities = ServerCapabilities::builder().enable_tools().build(); i` (both are non-exhaustive → `default()`-then-mutate, NOT a struct literal).
  - `fn list_tools(&self, _: Option<PaginatedRequestParams>, _: RequestContext<RoleServer>) -> … Result<ListToolsResult, McpError>` → `Ok(ListToolsResult::with_all_items(tools))`.
  - `fn call_tool(&self, request: CallToolRequestParams, _: RequestContext<RoleServer>) -> … Result<CallToolResult, McpError>` → park, then `Ok(CallToolResult::success(vec![Content::text(result)]))`. Errors: `McpError::internal_error("msg", None)`.
- **`Tool`** is non-exhaustive → build with `Tool::new(name: impl Into<Cow<'static,str>>, description: impl Into<Cow<'static,str>>, input_schema: impl Into<Arc<JsonObject>>)` where `JsonObject = serde_json::Map<String, Value>` (pass `Arc::new(map)`). Read back: `tool.name` (a `Cow`).
- **`CallToolRequestParams`:** `.name: Cow<'static,str>`, `.arguments: Option<JsonObject>`. Build a client call with `CallToolRequestParams::new("name").with_arguments(map)`.
- **`Content::text(s)`**; read back via `c.as_text().map(|t| t.text.clone())`.
- **Server transport:** `use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;` · `use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};` then `StreamableHttpService::new(move || Ok(handler_factory()), Arc::new(LocalSessionManager::default()), StreamableHttpServerConfig::default())` and mount with `axum::Router::new().nest_service("/mcp", service)`, served by `axum::serve` on `TcpListener::bind("127.0.0.1:0")`.
- **Hermetic test transport:** `let (s_io, c_io) = tokio::io::duplex(8*1024); let server_task = tokio::spawn(async move { serve_server(handler, s_io).await }); let client = serve_client((), c_io).await?; let server = server_task.await??;` — **the server MUST be spawned concurrently** (the initialize handshake deadlocks if you `await serve_server` before `serve_client`). Then `client.list_tools(None).await?` / `client.call_tool(params).await?`, and `client.cancel().await?` / `server.cancel().await?`.
- **claude wiring (confirmed via `claude mcp add --help`):** transport `http` (= StreamableHttp). Per-request temp file:
  ```json
  {"mcpServers": {"llm-bridge": {"type": "http", "url": "http://127.0.0.1:<PORT>/mcp"}}}
  ```
  passed as `claude … --mcp-config <temp-file> --strict-mcp-config` (`--strict-mcp-config` ignores ALL other MCP config — the isolation the spec requires).

---

## Starting state (end of Phase 4a, on `master`)
`cargo test` → 98 (83 lib + 15 integration). Relevant Phase-4a surface this phase builds on:
- `openai.rs`: `ToolCall`/`FunctionCall`, `tool_calls` on `ChatMessage`/`ResponseMessage`, `DeltaToolCall`; `tools: Option<serde_json::Value>` (opaque — this phase adds typed `tool_defs()`); `has_tools()`.
- `engine/mod.rs`: `AgentEvent::ToolCall { id, name, args }`; `Caps.mcp_tools` (claude=true); `EventStream`; `Turn { …, engine, permissions }`.
- `suspend.rs`: `SuspendedSessions { register(pairs: Vec<(String, oneshot::Sender<String>)>, continuation: EventStream) -> Result<u64, RegisterError>, deliver(&[(String,String)]) -> Result<EventStream, DeliverError>, reap(u64), spawn_reaper(u64, Duration), live_count() }`; `RegisterError::Full`; `DeliverError::{Unknown, Partial, Duplicate}`.
- `routing.rs`: `tool_result_suffix(&[ChatMessage]) -> Option<Vec<(String,String)>>`.
- `http.rs`: `chat_completions` — follow-up routing → `deliver` (resume/409/400); `tools`→claude currently returns **`400 "Phase 4b"`** (THIS phase replaces that with the real tools-turn); `AppState { …, suspended, tool_result_timeout }`.
- `process.rs`: `ProcessSupervisor { sem: Arc<Semaphore> }`, `spawn_streaming(cmd, stdin, timeout)`.

---

## Scope note — Phase 4b completes the MCP bridge (spec §9 step 8). In scope: claude only.
- **codex MCP stays gated OFF** (`caps(Codex).mcp_tools == false`) pending the §9 codex `-c mcp_servers.*` injection spike; **agy never**. Unchanged from 4a — `tools` to codex/agy still returns the clear "not supported for engine" `400`.
- **Multi-round tool use** (the resumed continuation itself making *new* tool calls → parking again) IS supported by construction (the continuation stream can yield more `AgentEvent::ToolCall`s and re-register), and is covered by a hermetic test.
- The session-key projection folding `tool_calls` (spec §4.5 — deferred from 4a) is included here (Task 1) so post-tool turns don't collide.

---

## File Structure (Phase 4b changes)
```
Cargo.toml             # + rmcp 1.7 (client, transport-streamable-http-server[-session])
src/openai.rs          # + ToolDef/FunctionDef + ChatCompletionRequest::tool_defs()
src/session.rs         # project(): fold assistant tool_calls + role:tool tool_call_id into the key
src/mcp.rs    (NEW)    # McpBridge (ServerHandler): list_tools from tool defs; call_tool emits PendingToolCall + parks.
                       #   tool_defs -> rmcp Tool; McpServer harness (StreamableHttpService on ephemeral port);
                       #   temp mcp-config writer. Hermetic tests via in-memory rmcp client (spike pattern).
src/lib.rs             # + pub mod mcp;
src/engine/mod.rs      # Turn: + mcp_config: Option<PathBuf>
src/engine/claude.rs   # build_stream_command: when turn.mcp_config is Some -> --mcp-config <path> --strict-mcp-config
src/process.rs         # ProcessSupervisor: acquire_owned permit handle; release-on-park / re-acquire-on-resume
src/orchestrator.rs    # run_tools_turn: start server + spawn claude + collect ToolCalls + register suspension
                       #   (continuation owns child+server); permit accounting
src/http.rs            # chat_completions: replace `tools`->claude 400 "Phase 4b" with run_tools_turn; 503 on Full
tests/e2e_smoke.rs     # feature-gated real-claude MCP round-trip
README.md              # Status (Phase 4b)
```

---

## Task 1: rmcp dep + typed tool defs + tool-aware session projection

Foundations with no live server yet: add `rmcp`, parse the request's `tools` *definitions* (`tool_defs()`), and fold tool calls into the session key (spec §4.5). All hermetic.

**Files:** `Cargo.toml`, `src/openai.rs`, `src/session.rs`.

- [ ] **Step 1: Add rmcp + uuid** — `Cargo.toml` `[dependencies]`:

```toml
rmcp = { version = "1.7", features = ["client", "transport-streamable-http-server", "transport-streamable-http-server-session"] }
uuid = { version = "1", features = ["v4"] }   # high-entropy tool_call_ids (spec §4.6)
```

Run `cargo build` (expect: downloads + compiles rmcp + uuid; clean). Commit nothing yet.

- [ ] **Step 2: Write failing tests** — append to `#[cfg(test)] mod tests` in `src/openai.rs`:

```rust
    #[test]
    fn tool_defs_parses_function_definitions() {
        let json = r#"{"model":"m","tools":[
            {"type":"function","function":{"name":"search","description":"find","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}},
            {"type":"function","function":{"name":"noparams"}}
        ],"messages":[]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        let defs = req.tool_defs();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].function.name, "search");
        assert_eq!(defs[0].function.description.as_deref(), Some("find"));
        assert_eq!(defs[0].function.parameters["type"], "object");
        assert_eq!(defs[1].function.name, "noparams");
        assert!(defs[1].function.parameters.is_null() || defs[1].function.parameters.is_object());
    }

    #[test]
    fn tool_defs_empty_when_no_tools_or_malformed() {
        let none: ChatCompletionRequest = serde_json::from_str(r#"{"model":"m","messages":[]}"#).unwrap();
        assert!(none.tool_defs().is_empty());
        // a non-function/garbage entry is skipped, not a parse error
        let bad: ChatCompletionRequest = serde_json::from_str(r#"{"model":"m","tools":[{"x":1}],"messages":[]}"#).unwrap();
        assert!(bad.tool_defs().is_empty());
    }
```

And in `src/session.rs` tests, add a collision-guard test:

```rust
    #[test]
    fn key_folds_tool_calls_so_post_tool_threads_differ() {
        // Two threads identical except for the assistant's tool_calls / the tool result must NOT collide.
        let base = vec![msg(Role::User, "do it")];
        let mut a = base.clone();
        a.push(ChatMessage { role: Role::Assistant, content: None,
            tool_call_id: None,
            tool_calls: Some(vec![crate::openai::ToolCall { id: "c1".into(), kind: "function".into(),
                function: crate::openai::FunctionCall { name: "f".into(), arguments: "{\"x\":1}".into() } }]) });
        a.push(ChatMessage { role: Role::Tool, content: Some(MessageContent::Text("R1".into())),
            tool_call_id: Some("c1".into()), tool_calls: None });
        let mut b = base.clone();
        b.push(ChatMessage { role: Role::Assistant, content: None, tool_call_id: None,
            tool_calls: Some(vec![crate::openai::ToolCall { id: "c1".into(), kind: "function".into(),
                function: crate::openai::FunctionCall { name: "f".into(), arguments: "{\"x\":2}".into() } }]) });
        b.push(ChatMessage { role: Role::Tool, content: Some(MessageContent::Text("R2".into())),
            tool_call_id: Some("c1".into()), tool_calls: None });
        assert_ne!(lookup_key(&a, &entry(), None, &rt()), lookup_key(&b, &entry(), None, &rt()));
    }
```

(The `msg(..)` helper in session tests builds a `ChatMessage` — it currently sets `tool_calls: None`; the two new messages above set their own.)

- [ ] **Step 3: Run to confirm failure** — `cargo test --lib openai` (no `tool_defs`), `cargo test --lib session` (projection ignores tool_calls so the two keys collide).

- [ ] **Step 4: Implement `tool_defs` in `src/openai.rs`.** Add the typed defs + accessor:

```rust
/// An incoming OpenAI tool definition (`{"type":"function","function":{...}}`).
#[derive(Debug, Clone, Deserialize)]
pub struct ToolDef {
    #[serde(rename = "type", default = "function_kind")]
    pub kind: String,
    pub function: FunctionDef,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FunctionDef {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// JSON Schema for the arguments; `Value::Null` if omitted.
    #[serde(default)]
    pub parameters: serde_json::Value,
}
```

Add to `impl ChatCompletionRequest`:

```rust
    /// The request's tool definitions, parsed from the opaque `tools` array. Entries that don't
    /// match the `{type:"function", function:{name,...}}` shape are skipped (never an error).
    pub fn tool_defs(&self) -> Vec<ToolDef> {
        let Some(serde_json::Value::Array(items)) = &self.tools else { return Vec::new(); };
        items.iter().filter_map(|v| serde_json::from_value::<ToolDef>(v.clone()).ok())
            .filter(|d| !d.function.name.is_empty())
            .collect()
    }
```

(`function_kind` already exists from Phase 4a.)

- [ ] **Step 5: Fold tool calls into the session key** — `src/session.rs` `project()`:

```rust
fn project(messages: &[ChatMessage]) -> String {
    messages
        .iter()
        .map(|m| {
            let mut s = format!("{:?}:{}", m.role, m.text());
            // Fold the assistant's tool_calls (name + canonicalized args) so two threads that differ
            // only in tool activity don't hash identically (spec §4.5).
            if let Some(calls) = &m.tool_calls {
                for c in calls {
                    s.push_str(&format!("|tc:{}:{}:{}", c.id, c.function.name, c.function.arguments));
                }
            }
            // role:tool results already carry their content via text(); fold the linkage id too.
            if let Some(id) = &m.tool_call_id {
                s.push_str(&format!("|tcid:{id}"));
            }
            s
        })
        .collect::<Vec<_>>()
        .join("\n")
}
```

- [ ] **Step 6: Run to verify pass + full suite** — `cargo test --lib openai && cargo test --lib session && cargo test && cargo clippy --all-targets`. All green/clean.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/openai.rs src/session.rs
git commit -m "feat: rmcp dep + ChatCompletionRequest::tool_defs + tool-aware session projection"
```

---

## Task 2: `McpBridge` — the in-process MCP `ServerHandler` (list_tools + parked call_tool)

The rmcp handler claude connects to: `list_tools` exposes the request's tool defs; `call_tool` generates a high-entropy `tool_call_id`, emits a `PendingToolCall` to the orchestrator, and **parks on a `oneshot`** until the result is delivered. Proven shape (spike). Tested hermetically with an in-memory rmcp client.

**Files:** Create `src/mcp.rs`; modify `src/lib.rs`.

- [ ] **Step 1: Declare the module** — `src/lib.rs` (alphabetical): `pub mod mcp;`

- [ ] **Step 2: Write the failing test** — `#[cfg(test)] mod tests` in `src/mcp.rs`:

```rust
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
        assert_ne!(a, b); // not a per-turn counter
    }

    #[tokio::test]
    async fn list_tools_exposes_defs_and_call_tool_parks_then_returns() {
        let (calls_tx, mut calls_rx) = mpsc::unbounded_channel::<PendingToolCall>();
        let bridge = McpBridge::new(vec![def("search")], calls_tx);

        let (s_io, c_io) = tokio::io::duplex(8 * 1024);
        let server_task = tokio::spawn(async move { serve_server(bridge, s_io).await });
        let client = serve_client((), c_io).await.unwrap();
        let server = server_task.await.unwrap().unwrap();

        // The "client tool runtime": deliver a result into whatever call parks.
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
```

- [ ] **Step 3: Run to confirm failure** — `cargo test --lib mcp` (compile error: `McpBridge`/`PendingToolCall`/`new_tool_call_id` undefined).

- [ ] **Step 4: Write the implementation** (top of `src/mcp.rs`):

```rust
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

/// Build an rmcp `Tool` from an OpenAI tool definition.
pub fn tool_from_def(def: &ToolDef) -> Tool {
    let schema = def.function.parameters.as_object().cloned().unwrap_or_default();
    let description = def.function.description.clone().unwrap_or_default();
    Tool::new(def.function.name.clone(), description, Arc::new(schema))
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
            // PARK: claude blocks here (the process is held open) until the follow-up delivers.
            let result = rx
                .await
                .map_err(|_| McpError::internal_error("tool result channel dropped (timed out?)", None))?;
            Ok(CallToolResult::success(vec![Content::text(result)]))
        }
    }
}
```

- [ ] **Step 5: Run to verify pass + full suite** — `cargo test --lib mcp && cargo test && cargo clippy --all-targets`. All green/clean.

- [ ] **Step 6: Commit**

```bash
git add src/mcp.rs src/lib.rs
git commit -m "feat: McpBridge ServerHandler (list_tools from defs; parked call_tool; high-entropy ids)"
```

---

## Task 3: per-request MCP server harness + temp `--mcp-config` writer

Start a `StreamableHttpService(McpBridge)` on an ephemeral `127.0.0.1` port (proven shape), returning a handle that owns the calls receiver + a shutdown signal + the temp mcp-config file (RAII — server torn down and file deleted on drop). The full HTTP MCP round-trip is exercised by the real-claude e2e (Task 8); this task is hermetic (bind + config-file shape + clean shutdown).

**Files:** Modify `src/mcp.rs`.

- [ ] **Step 1: Write the failing test** — add to `#[cfg(test)] mod tests` in `src/mcp.rs`:

```rust
    #[tokio::test]
    async fn server_binds_and_writes_strict_http_config() {
        use super::McpServer;
        let server = McpServer::start(vec![def("search")]).await.unwrap();
        assert!(server.port() > 0);
        let cfg = std::fs::read_to_string(server.config_path()).unwrap();
        let v: serde_json::Value = serde_json::from_str(&cfg).unwrap();
        assert_eq!(v["mcpServers"]["llm-bridge"]["type"], "http");
        assert_eq!(v["mcpServers"]["llm-bridge"]["url"], format!("http://127.0.0.1:{}/mcp", server.port()));
        let path = server.config_path().to_path_buf();
        drop(server); // RAII: shuts the server down and deletes the temp config
        // a brief beat for the deletion + task abort
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!path.exists(), "temp mcp-config should be removed on drop");
    }
```

- [ ] **Step 2: Run to confirm failure** — `cargo test --lib mcp` (no `McpServer`).

- [ ] **Step 3: Implement `McpServer` in `src/mcp.rs`** (below `McpBridge`):

```rust
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use std::path::{Path, PathBuf};

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
                .with_graceful_shutdown(async { let _ = shutdown_rx.await; })
                .await;
        });

        let config_path = std::env::temp_dir().join(format!("llm-bridge-mcp-{}.json", uuid::Uuid::new_v4().simple()));
        let body = serde_json::json!({
            "mcpServers": { "llm-bridge": { "type": "http", "url": format!("http://127.0.0.1:{port}/mcp") } }
        });
        std::fs::write(&config_path, serde_json::to_vec(&body)?)?;

        Ok(McpServer { port, config_path, calls, shutdown: Some(shutdown_tx), join })
    }

    pub fn port(&self) -> u16 { self.port }
    pub fn config_path(&self) -> &Path { &self.config_path }
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
```

- [ ] **Step 4: Run to verify pass + full suite** — `cargo test --lib mcp && cargo test && cargo clippy --all-targets`. All green/clean.

- [ ] **Step 5: Commit**

```bash
git add src/mcp.rs
git commit -m "feat: McpServer harness (ephemeral StreamableHttpService + RAII temp --mcp-config)"
```

---

## Task 4: ClaudeAdapter `--mcp-config` wiring

`Turn` gains `mcp_config: Option<PathBuf>`; when set, the claude command adds `--mcp-config <path> --strict-mcp-config` (confirmed flags).

**Files:** Modify `src/engine/mod.rs`, `src/engine/claude.rs`, and every `Turn { .. }` construction.

- [ ] **Step 1: Write the failing test** — add to `#[cfg(test)] mod tests` in `src/engine/claude.rs` (the `turn(..)` helper builds a `Turn`):

```rust
    #[test]
    fn mcp_config_adds_strict_flags() {
        let a = ClaudeAdapter::new("claude", None);
        let mut t = turn(Mode::Agentic, Some("/work/repoA"), None);
        t.mcp_config = Some(std::path::PathBuf::from("/tmp/mcp.json"));
        let (cmd, _) = a.build_stream_command(&t, &[]);
        let args = args(&cmd);
        assert!(args.windows(2).any(|w| w == ["--mcp-config", "/tmp/mcp.json"]));
        assert!(args.contains(&"--strict-mcp-config".to_string()));
    }
```

- [ ] **Step 2: Run to confirm failure** — `cargo test --lib claude` (no `Turn.mcp_config`).

- [ ] **Step 3: Add the field** — in `src/engine/mod.rs` `Turn`, after `permissions`:

```rust
    /// `Some(path)` to a per-request MCP config file → claude `--mcp-config <path> --strict-mcp-config`.
    pub mcp_config: Option<std::path::PathBuf>,
```

Update EVERY `Turn { .. }` construction to add `mcp_config: None` (the claude/codex/agy test helpers, `engine/mod.rs` tests, `tests/e2e_smoke.rs`) and, in `orchestrator.rs` `run_request`'s two arms, `mcp_config: None`. `grep -rn "Turn {" src tests` to find them all.

- [ ] **Step 4: Wire the flags** — in `src/engine/claude.rs` `build_stream_command`, after the model/resume handling and before/with the mode handling, add:

```rust
        if let Some(mcp) = &turn.mcp_config {
            cmd.arg("--mcp-config").arg(mcp).arg("--strict-mcp-config");
        }
```

(codex/agy adapters ignore `mcp_config` — `caps().mcp_tools` is false for them, so it's never set.)

- [ ] **Step 5: Run to verify pass + full suite** — `cargo test && cargo clippy --all-targets`. All green/clean.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mod.rs src/engine/claude.rs src/orchestrator.rs tests/e2e_smoke.rs
git commit -m "feat: claude --mcp-config <file> --strict-mcp-config when Turn.mcp_config is set"
```

---

## Task 5: ProcessSupervisor — owned permit handle (release-on-park / re-acquire-on-resume)

A parked suspension must release its active `max_concurrency` permit (it's idle, awaiting the client) and re-acquire one on resume (spec §4.7). Expose the owned permit so the suspension can drop it and the runner can re-acquire.

**Files:** Modify `src/process.rs`.

- [ ] **Step 1: Write the failing test** — add to `#[cfg(test)] mod tests` in `src/process.rs`:

```rust
    #[tokio::test]
    async fn acquire_and_drop_releases_active_slot() {
        let sup = ProcessSupervisor::new(1); // one active slot
        let permit = sup.acquire().await;     // take it
        assert_eq!(sup.available(), 0);
        drop(permit);                          // park-release
        assert_eq!(sup.available(), 1);
        let _p2 = sup.acquire().await;         // resume re-acquires
        assert_eq!(sup.available(), 0);
    }
```

- [ ] **Step 2: Run to confirm failure** — `cargo test --lib process` (no `acquire`/`available`).

- [ ] **Step 3: Implement** — add to `impl ProcessSupervisor` in `src/process.rs`:

```rust
    /// Acquire an owned active-concurrency permit. Dropping it frees the slot (a parked suspension
    /// drops its permit while idle; the resume path re-acquires). Used by the tools-turn path.
    pub async fn acquire(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.sem.clone().acquire_owned().await.expect("semaphore not closed")
    }

    /// Currently-free active slots (for tests/observability).
    pub fn available(&self) -> usize {
        self.sem.available_permits()
    }
```

(`spawn_streaming` already acquires its own permit internally; the tools-turn path will instead hold an explicit `OwnedSemaphorePermit` so it can release on park. The non-tools path is unchanged.)

- [ ] **Step 4: Run to verify pass + full suite** — `cargo test --lib process && cargo test && cargo clippy --all-targets`. All green/clean.

- [ ] **Step 5: Commit**

```bash
git add src/process.rs
git commit -m "feat: ProcessSupervisor::acquire (owned permit) + available() for park/resume accounting"
```

---

## Task 6: `run_tools_turn` — held-open claude + suspension creation (the crux)

Wire the live bridge: start an `McpServer`, spawn claude pointed at it (`--mcp-config`), consume claude's stdout events AND the server's `calls` concurrently; when the agent makes tool calls, collect them as `AgentEvent::ToolCall` + per-id `oneshot` senders, **register a suspension** whose continuation owns the live child + server (so reap/teardown kills them) and **drops the active permit** (re-acquired on resume), and return the `tool_calls` to send the client. The follow-up's `deliver` (Phase 4a, already wired in `http.rs`) fires the `oneshot`s → `call_tool` returns → claude continues → the continuation streams the rest.

> **⚠️ Step 0 (REQUIRED real-claude observation — the one thing the rmcp spike did NOT prove):** before implementing the batch-collection, OBSERVE how the installed claude interleaves `stream-json` stdout with MCP `call_tool` when given `--mcp-config` + a prompt that calls a tool. Run a throwaway: an `McpServer` with one tool + `claude -p --output-format stream-json --verbose --mcp-config <file> --strict-mcp-config "use the X tool then answer"`, and log (a) what claude prints on stdout around the tool call (does it emit an `assistant` `tool_use` block? a `system`/`mcp` event?), (b) whether it makes calls sequentially (call→await result→next) or several before blocking, (c) that it blocks until `call_tool` returns. **Record the findings as comments in `run_tools_turn`**; they calibrate the "batch complete" detection below. (This needs a logged-in claude; it is the real-claude analogue of the Phase-4a hermetic registry tests.)

**Batch-complete detection (conservative default, refine per Step 0):** treat the turn as "ready for tool results" once the agent has emitted ≥1 `call_tool` AND no further `call_tool` arrives within a short **settle window** (`MCP_BATCH_SETTLE = 150ms`) — this covers both sequential (1 call → settle) and parallel (N calls within the window) emission without needing a claude-internal "done" signal. If Step 0 shows claude always calls sequentially, the window still yields correct (1-call) rounds.

**Files:** Modify `src/orchestrator.rs`.

- [ ] **Step 1: Write the failing hermetic test** — a FAKE engine (in-memory rmcp client, spike pattern) stands in for claude, driving the bridge so the test is deterministic. Add to `#[cfg(test)] mod tests` in `src/orchestrator.rs`:

```rust
    // Drives an McpBridge with an in-memory rmcp client acting as "claude": calls one tool, then
    // (after the result is delivered) the continuation yields the final answer.
    #[tokio::test]
    async fn tools_turn_collects_calls_registers_suspension_and_resumes() {
        use crate::mcp::{McpBridge, PendingToolCall};
        use crate::suspend::SuspendedSessions;
        use rmcp::{serve_client, serve_server};
        use tokio::sync::mpsc;

        // Bridge + in-memory "claude" client.
        let (calls_tx, mut calls_rx) = mpsc::unbounded_channel::<PendingToolCall>();
        let bridge = McpBridge::new(vec![/* one tool */ crate::openai::ToolDef {
            kind: "function".into(),
            function: crate::openai::FunctionDef { name: "search".into(), description: None, parameters: serde_json::json!({"type":"object"}) },
        }], calls_tx);
        let (s_io, c_io) = tokio::io::duplex(8 * 1024);
        let server_task = tokio::spawn(async move { serve_server(bridge, s_io).await });
        let claude = serve_client((), c_io).await.unwrap();
        let _server = server_task.await.unwrap().unwrap();

        let suspended = Arc::new(SuspendedSessions::new(8));

        // "claude" makes a tool call in the background; it blocks until we deliver the result.
        let claude_task = tokio::spawn(async move {
            let p = rmcp::model::CallToolRequestParams::new("search");
            let res = claude.call_tool(p).await.unwrap();
            res.content.iter().find_map(|c| c.as_text().map(|t| t.text.clone())).unwrap()
        });

        // Orchestrator side: collect the parked call, register a suspension whose continuation
        // yields the final answer once the result is delivered.
        let call = calls_rx.recv().await.unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        let id = call.id.clone();
        // continuation: wait for the result to be delivered into the bridge, then yield final text.
        let bridge_reply = call.reply;
        let continuation: crate::engine::EventStream = Box::pin(async_stream::stream! {
            let result = rx.await.unwrap_or_default();
            let _ = bridge_reply.send(result);   // unblock the parked call_tool
            yield crate::engine::AgentEvent::AssistantText("done".into());
            yield crate::engine::AgentEvent::Done { finish_reason: "stop".into() };
        });
        suspended.register(vec![(id.clone(), tx)], continuation).unwrap();

        // Follow-up delivers the tool result -> fires the oneshot -> claude's call_tool returns.
        let cont = suspended.deliver(&[(id.clone(), "RESULT".into())]).unwrap();
        // claude got its tool result:
        assert_eq!(claude_task.await.unwrap(), "RESULT");
        // the continuation streams the final answer:
        use futures::StreamExt;
        let evs: Vec<_> = cont.collect().await;
        assert!(evs.iter().any(|e| matches!(e, crate::engine::AgentEvent::AssistantText(t) if t == "done")));
    }
```

This test proves the **end-to-end suspension round-trip** (parked call → register → deliver → call_tool returns → continuation streams) hermetically. The production `run_tools_turn` (Step 3) wires the same pieces around a real spawned claude.

- [ ] **Step 2: Run to confirm failure** — `cargo test --lib orchestrator` (compile error / the test's helpers not yet importable). Confirm it fails before wiring.

- [ ] **Step 3: Implement `run_tools_turn` in `src/orchestrator.rs`.** After the Step-0 observation, implement the production path. The shape (calibrate the stdout handling per Step 0):

```rust
use crate::mcp::{McpServer, PendingToolCall};
use crate::suspend::{RegisterError, SuspendedSessions};
use std::time::Duration;

const MCP_BATCH_SETTLE: Duration = Duration::from_millis(150);

/// Outcome of a tools-turn: either the agent's tool_calls (suspension registered), or — if the
/// agent answered without calling any tool — a plain event stream.
pub enum ToolsTurn {
    ToolCalls(Vec<crate::openai::ToolCall>),
    Final(EventStream),
}

/// Run a `tools`-bearing claude turn against a per-request MCP server. Returns the tool_calls to
/// send the client (having registered the suspension that the follow-up resumes), or the final
/// answer stream if no tool was called. `Err(RegisterError::Full)` → HTTP 503.
#[allow(clippy::too_many_arguments)]
pub async fn run_tools_turn(
    supervisor: ProcessSupervisor,
    suspended: Arc<SuspendedSessions>,
    entry: &ModelEntry,
    req: &ChatCompletionRequest,
    rt: &RuntimeFingerprint,
    claude_config_dir: Option<PathBuf>,
    env_passthrough: Vec<String>,
    timeout: Duration,
    tool_result_timeout: Duration,
) -> Result<ToolsTurn, RegisterError> {
    // Refuse early if the suspended pool is full (we will register one on tool calls).
    if suspended.live_count() >= /* max */ supervisor.available().max(0) + suspended.live_count() {
        // NOTE: the real cap lives in SuspendedSessions (register returns Full); this early check is
        // an optimization. The authoritative 503 comes from `register` below.
    }

    let permit = supervisor.acquire().await; // active slot; released when we park
    let server = McpServer::start(req.tool_defs())
        .await
        .map_err(|_| RegisterError::Full)?; // treat server-start failure conservatively

    // Build the claude turn (resume vs fresh as in run_request), pointed at the MCP server.
    let sys = system_prompt_of(req);
    let rendered = render_turn(&req.messages);
    let turn = Turn {
        system_prompt: rendered.system_prompt.clone(),
        user_prompt: rendered.user_prompt.clone(),
        model: entry.model.clone(),
        workspace: entry.workspace.clone(),
        mode: entry.mode,
        resume: None, // (a fresh MCP session per tools-turn; resume-with-tools is future work)
        engine: entry.engine,
        permissions: entry.permissions.clone(),
        mcp_config: Some(server.config_path().to_path_buf()),
    };
    let _ = (&sys, claude_config_dir.as_ref(), &rt); // (sys/rt used by the suspension-store path)

    // Spawn claude (holding our explicit permit) and stream its events.
    let engine = Engine::Claude(ClaudeAdapter::new("claude", claude_config_dir));
    let (cmd, stdin) = engine.build_stream_command(&turn, &env_passthrough);
    let claude_lines = supervisor.spawn_streaming(cmd, stdin, timeout);

    // Concurrently consume (a) claude stdout events and (b) the server's parked tool calls,
    // collecting tool calls until the batch settles. (Step-0 observation calibrates whether any
    // claude stdout signals the batch; the settle window is the safe default.)
    //
    // For brevity the collection loop is sketched; implement per Step 0:
    //  - move `server` (its `.calls` receiver) + `claude_lines` into a select! loop;
    //  - on a PendingToolCall: push AgentEvent::ToolCall{id,name,args} + (id, reply) to pending;
    //  - if pending non-empty and no new call within MCP_BATCH_SETTLE -> batch done;
    //  - if claude_lines ends with a normal `result` (no tool calls) -> ToolsTurn::Final.
    //
    // On batch done: build the continuation EventStream that OWNS `server`, the remaining
    // `claude_lines`, and `permit` (re-acquired on resume — drop `permit` here to release the
    // active slot while parked), and resumes once the oneshots fire; then:
    //   let pairs: Vec<(String, oneshot::Sender<String>)> = pending senders;
    //   suspended.register(pairs, continuation)?;   // Full -> 503
    //   suspended.spawn_reaper(group_id, tool_result_timeout);
    //   return Ok(ToolsTurn::ToolCalls(tool_calls));
    todo!("wire the select! loop per the Step-0 observation; see the test in Step 1 for the proven round-trip")
}
```

> **Honesty note for the executor:** the `select!` collection loop + continuation construction is the genuinely novel integration. The Phase-4a registry + Task-1..5 pieces + the Step-1 hermetic test prove every sub-mechanism; Step 0's observation tells you exactly what claude emits so you can finalize the loop (and whether `resume` can carry tools). Do NOT ship the `todo!()` — replace it with the loop, keep the Step-1 test green, and add the real-claude e2e (Task 8). If Step 0 reveals claude's MCP interaction differs materially from this sketch, STOP and report findings before forcing the design.

- [ ] **Step 4: Run** — `cargo test --lib orchestrator` (the Step-1 round-trip test passes; `run_tools_turn` compiles once the loop replaces `todo!()`), then `cargo test && cargo clippy --all-targets`.

- [ ] **Step 5: Commit**

```bash
git add src/orchestrator.rs
git commit -m "feat: run_tools_turn — held-open claude + MCP server + suspension creation (resume round-trip)"
```

---

## Task 7: Wire `run_tools_turn` into `chat_completions`

Replace the Phase-4a `tools`→claude `400 "Phase 4b"` with the real tools-turn; map `ToolsTurn::ToolCalls` → an aggregated/streamed `tool_calls` response, `Final` → the stream, and `RegisterError::Full` → `503`.

**Files:** Modify `src/http.rs` (+ `AppState` already has `suspended`, `tool_result_timeout`; the supervisor/credentials/env/timeout come from the runner — expose what's needed).

- [ ] **Step 1: Write failing integration tests** — in `tests/http_integration.rs`, add a `503` test driving overflow via a pre-filled registry, and (feature-gated, Task 8) the real round-trip. Hermetic `503`:

```rust
#[tokio::test]
async fn tools_turn_503_when_suspended_pool_full() {
    // A registry already at capacity -> a new claude tools turn can't register -> 503.
    let st = state(None);
    // fill the pool to its cap by registering throwaway suspensions
    // (use SuspendedSessions::new(N) in state_with so N is small; here assert the 503 mapping)
    // ... (the exact fill uses st.suspended.register in a loop up to the cap)
    let _ = st; // see note
}
```

> Because `run_tools_turn` spawns a real claude, the FULL happy-path is the feature-gated e2e (Task 8). The hermetic HTTP test here asserts the **`503`-on-full** mapping (pre-fill the registry to its cap, then a `tools`→claude request returns `503`) and that the gate/routing from Phase 4a still hold. Keep `state_with`'s `SuspendedSessions::new(N)` small enough to fill in a test.

- [ ] **Step 2: Implement** — in `src/http.rs` `chat_completions`, replace the claude branch of the tools gate:

```rust
        // claude is MCP-capable: run the live tools-turn.
        return match crate::orchestrator::run_tools_turn(/* supervisor, */ state.suspended.clone(), &entry, &req, &rt, /* creds/env/timeouts */).await {
            Ok(crate::orchestrator::ToolsTurn::ToolCalls(calls)) => {
                // respond with the assistant tool_calls (aggregate) or an SSE tool_calls chunk
                // reuse response_from_events / the SSE mapper by feeding AgentEvent::ToolCall events,
                // OR build the ChatCompletionResponse directly from `calls`.
                tool_calls_response(calls, &model_id, streaming).into_response()
            }
            Ok(crate::orchestrator::ToolsTurn::Final(events)) => finish(events, model_id, streaming, progress).await,
            Err(_full) => err(StatusCode::SERVICE_UNAVAILABLE, "suspended tool-call pool is full; retry shortly", "overloaded"),
        };
```

(`AppState` must carry the supervisor + credentials + env_passthrough + timeouts for `run_tools_turn`, or move the runner's fields onto `AppState`. The Phase-4a `EngineProcessRunner` already holds supervisor/credentials/env/timeout — expose a handle, or thread them through `AppState`. Choose the smaller diff; `run_tools_turn`'s signature lists what it needs.)

- [ ] **Step 3: Run** — `cargo test && cargo clippy --all-targets`. Hermetic tests green; e2e is Task 8.

- [ ] **Step 4: Commit**

```bash
git add src/http.rs src/orchestrator.rs
git commit -m "feat: wire run_tools_turn into chat_completions (tool_calls response; 503 on suspended-pool full)"
```

---

## Task 8: Real-claude e2e + README + Definition of Done

**Files:** `tests/e2e_smoke.rs`, `README.md`.

- [ ] **Step 1: Feature-gated real-claude MCP round-trip** — append to `tests/e2e_smoke.rs` (inside the `#![cfg(feature = "e2e_smoke")]` module). Spin up `McpServer` with one tool, run a real claude `-p` turn via `--mcp-config` that calls it, deliver a result, assert claude's final answer reflects it. (This is the true validation of the Step-0 claude behavior + the held-open mechanic. Off by default; needs a logged-in claude.)

```rust
// claude_mcp_tool_round_trip: McpServer + a real `claude -p --mcp-config ...` turn that calls the
// tool; deliver a canned result via the parked oneshot; assert claude's answer reflects the result.
// (Body mirrors run_tools_turn; kept here as the manual end-to-end check.)
```

- [ ] **Step 2: README Status (Phase 4b)** — replace the `## Status (Phase 4a)` block:

````markdown
## Status (Phase 4b)
- ✅ Three engines; session resume (claude+codex); bubblewrap sandbox + canary probes
- ✅ **OpenAI `tools` end-to-end for claude**: an in-process `rmcp` MCP server exposes the request's tools, claude is wired per-invocation via `--mcp-config <temp> --strict-mcp-config`, tool calls become OpenAI `tool_calls`, and the client's tool-result follow-up resumes the held-open turn (suspended-session registry: all-or-`409`, duplicate/unknown→`400`, idle-reap, `max_suspended_sessions`→`503`, active-permit released while parked)
- ⛔ `tools` to codex/agy → `400 "not supported for engine …"` (codex gated pending the §9 injection spike; agy never); `sandbox_backend: container` → refused at startup
````

- [ ] **Step 3: Verify + commit** — `cargo test && cargo clippy --all-targets && cargo build --features e2e_smoke --tests`. Then:

```bash
git add tests/e2e_smoke.rs README.md
git commit -m "feat: real-claude MCP e2e (feature-gated) + README Status (Phase 4b complete)"
```

---

## Phase 4b Done — Definition of Done
- `cargo build` clean; `cargo test` green; `cargo clippy --all-targets` clean; `cargo build --features e2e_smoke --tests` compiles.
- A `tools` request to **claude** stands up an `rmcp` MCP server, wires claude via `--mcp-config --strict-mcp-config`, and returns OpenAI `tool_calls` (`finish_reason:"tool_calls"`); the client's tool-result follow-up resumes the held-open turn and streams the continuation.
- The held-open claude child + its MCP server are owned by the suspension's continuation (reap/timeout/teardown kills them); the active `max_concurrency` permit is released while parked and re-acquired on resume; a full suspended pool → `503`.
- The hermetic round-trip test (in-memory rmcp client) passes; the real-claude e2e passes locally (feature-gated).
- codex/agy `tools` still → clear `400`; the §9 codex MCP-injection spike remains the documented path to enabling codex `tools`.

**Phase 4b completes the spec's build sequence.** Remaining documented future work: the codex `-c mcp_servers.*` injection spike (to lift codex `tools`), the agy session-id/keyring spike, `sandbox_backend: container`, and multi-tenant namespacing — all called out in the spec as post-v1 extension points.
