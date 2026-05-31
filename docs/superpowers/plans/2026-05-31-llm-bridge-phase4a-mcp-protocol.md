# llm-bridge Phase 4a — MCP Tool-Call Protocol + Suspended-Session State Machine — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the **hermetic core** of the OpenAI `tools` → MCP bridge: the wire types (tool-calls out, tool-results in), the `AgentEvent::ToolCall` event, per-engine `mcp_tools` capability gating, **tool-result routing by message-list shape** (spec §4.3 step 2), and the **`SuspendedSessions` registry** — the turn-level tool-call group with all-or-`409` / duplicate-`400` / unknown-`400` / orphan-timeout / `max_suspended_sessions`-`503` semantics — all driven and tested by a scripted **fake bridge** (NO `rmcp`, no live child process).

**Architecture:** A request carrying `tools` (to an `mcp_tools`-capable engine — claude only in v1) runs a turn whose stream emits `AgentEvent::ToolCall`s; the orchestrator collects them, responds with OpenAI `tool_calls` + `finish_reason:"tool_calls"`, and **parks the turn's continuation** in `SuspendedSessions` keyed by the turn's `tool_call_id`s — each id holding a `oneshot` the follow-up delivers a result into, plus the parked continuation `EventStream`. The follow-up request (a trailing `role:"tool"` suffix) routes to the group: a complete result set fires every `oneshot` and drains the continuation as the response; a partial set → `409`; a duplicate/unknown id → `400`; an idle group is reaped after `tool_result_timeout_s`; overflow past `max_suspended_sessions` → `503`. In Phase 4a the continuation stream and the parked child are a **fake** that awaits the same `oneshot`s; Phase 4b swaps in the real `rmcp` server + held-open CLI behind the identical registry API.

**Tech Stack:** No new crates (tokio `sync::oneshot`/`Mutex`, `futures`/`async-stream`, `serde`). `rmcp` arrives in Phase 4b. Builds on Phases 1–3. Spec: `docs/superpowers/specs/2026-05-31-llm-bridge-design.md` (§3.1 consumer constraint, §4.3 step 2 routing, §4.4 `ToolCall` event + channel mapping, §4.6 MCP bridge + suspended groups, §4.7 accounting, §6 config, §8 testing, §9 step 8, §10 risks).

**Scope note — Phase 4a of the two-part Phase 4.** In scope: tool wire types, `ToolCall` event, `mcp_tools` caps gating, routing-by-shape, the `SuspendedSessions` registry + its error/accounting/timeout semantics, and the HTTP wiring that maps `ToolCall` → `tool_calls` and routes follow-ups — all hermetic (fake bridge). **Deliberately deferred to Phase 4b (refused/absent, never silently allowed):**
- **The live `rmcp` in-process MCP server**, per-invocation claude `--mcp-config <temp-file> --strict-mcp-config` wiring, the real held-open CLI child, and the real-claude e2e. Until 4b, no real engine emits `ToolCall`s — 4a is exercised entirely by a scripted `FakeBridge`. A `tools` request reaches the full protocol path (gating, suspension, routing) but the "engine" is the fake.
- **codex MCP** stays gated OFF (`caps(Codex).mcp_tools == false`) pending the §9 injection spike; **agy** never (`false`). A `tools` request to codex/agy → clear OpenAI error.
- Consuming the incoming tool **definitions** (`tools[].function`) to register them with the MCP server is **4b** — 4a only needs the `tool_call`s (out) and `tool` results (in).

---

## Starting state (end of Phase 3, on `master`)
`cargo test` → 75 (68 lib + 7 integration). Relevant shapes:
- `openai.rs`: `Role {System,Developer,User,Assistant,Tool}`; `ChatMessage { role, content: Option<MessageContent>, tool_call_id: Option<String> }`; `ChatCompletionRequest { model, messages, stream, tools: Option<serde_json::Value> }` with `is_streaming()` + `has_tools()` (true iff `tools` is a non-empty array); `ResponseMessage { role: &'static str, content: String }`; `Delta { role, content, reasoning_content }` (all `skip_serializing_if=None`); `ChatCompletionChunk`/`ChunkChoice`; `ApiError`.
- `engine/mod.rs`: `AgentEvent { AssistantText, Reasoning, ToolStart{name,args}, ToolResult{summary}, SessionId, Error, Done{finish_reason} }`; `Caps { streaming, resume_by_id, mcp_tools_phase1 }`; `caps_for(EngineKind)->Caps` (claude/codex stream+resume, agy neither; all `mcp_tools_phase1:false`); `Engine` enum + `EventStream = Pin<Box<dyn Stream<Item=AgentEvent>+Send>>`.
- `orchestrator.rs`: `trait TurnRunner { fn run_stream(&self, Turn) -> EventStream }`; `run_request(runner, sessions, entry, req, rt) -> EventStream`; `response_from_events(Vec<AgentEvent>, model_id) -> Result<ChatCompletionResponse, EngineError>`; `turn_resumable`.
- `http.rs`: `AppState { registry, bearer_token, runner, sessions, defaults, progress_channel, credentials }`; `chat_completions` — **rejects `req.has_tools()` with `400` "the `tools` field is not implemented yet (Phase 4)"**, resolves model (404), builds `rt`, calls `run_request`, SSE (`sse_response`) or aggregates (`response_from_events`).
- `config.rs`: `Defaults { timeout_s, max_concurrency, sandbox_backend, env_passthrough }`; `EngineKind`, `Mode`, `SandboxBackend::as_str()`, `EngineKind::as_str()`.
- `process.rs`: `ProcessSupervisor { sem: Arc<Semaphore> }` (the active `max_concurrency` cap).

---

## File Structure (Phase 4a changes)
```
src/openai.rs        # + ToolCall/FunctionCall, tool_calls on ChatMessage/ResponseMessage, DeltaToolCall on Delta
src/engine/mod.rs    # + AgentEvent::ToolCall{id,name,args}; Caps.mcp_tools_phase1 -> mcp_tools (claude=true, codex/agy=false)
src/routing.rs (NEW) # tool_result_suffix(): is this follow-up a role:"tool" suffix? extract (tool_call_id, content) pairs
src/suspend.rs (NEW) # SuspendedSessions registry: register (cap->503), deliver (all-or-409 / dup-400 / unknown-400), reap
src/orchestrator.rs  # tools-turn: collect ToolCalls -> tool_calls response + park continuation; aggregate maps ToolCall
src/http.rs          # gate tools per caps (claude ok; codex/agy clear error); route tool-result follow-ups; 409/400/503
src/config.rs        # Defaults: + tool_result_timeout_s (120), max_suspended_sessions (16)
src/lib.rs           # + pub mod routing; pub mod suspend;
tests/http_integration.rs  # tool_calls response shape; follow-up routing (complete=200 continuation / 409 / 400 / 503)
```
Each `src/*.rs` keeps unit tests in a bottom `#[cfg(test)] mod tests`. Filled in dependency order: openai → engine → config → routing → suspend → orchestrator → http.

---

## Task 1: OpenAI tool wire types — `tool_calls` out, tool results in

Adds the OpenAI function-calling wire shapes the bridge emits and consumes: `ToolCall`/`FunctionCall` (the assistant's calls, echoed back by the client), `tool_calls` on `ChatMessage` (incoming follow-up) and `ResponseMessage` (non-streaming), and `DeltaToolCall` on `Delta` (streaming). The incoming `tools` *definitions* stay an opaque `Option<Value>` (consumed in Phase 4b); `has_tools()` is unchanged.

**Files:** Modify `src/openai.rs`.

- [ ] **Step 1: Write the failing tests** — append to `#[cfg(test)] mod tests` in `src/openai.rs`:

```rust
    #[test]
    fn parses_tool_result_followup_with_assistant_tool_calls() {
        // A follow-up: assistant message carrying tool_calls, then a role:"tool" result.
        let json = r#"{"model":"m","messages":[
            {"role":"user","content":"do it"},
            {"role":"assistant","content":null,"tool_calls":[
                {"id":"call_abc","type":"function","function":{"name":"search","arguments":"{\"q\":\"x\"}"}}]},
            {"role":"tool","tool_call_id":"call_abc","content":"result text"}
        ]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        let asst = &req.messages[1];
        let calls = asst.tool_calls.as_ref().unwrap();
        assert_eq!(calls[0].id, "call_abc");
        assert_eq!(calls[0].function.name, "search");
        assert_eq!(calls[0].function.arguments, r#"{"q":"x"}"#);
        let tool = &req.messages[2];
        assert_eq!(tool.role, Role::Tool);
        assert_eq!(tool.tool_call_id.as_deref(), Some("call_abc"));
        assert_eq!(tool.text(), "result text");
    }

    #[test]
    fn serializes_tool_calls_in_response_message() {
        let msg = ResponseMessage {
            role: "assistant",
            content: String::new(),
            tool_calls: Some(vec![ToolCall {
                id: "call_1".into(), kind: "function".into(),
                function: FunctionCall { name: "edit".into(), arguments: "{}".into() },
            }]),
        };
        let s = serde_json::to_string(&msg).unwrap();
        assert!(s.contains(r#""tool_calls":[{"id":"call_1","type":"function","function":{"name":"edit","arguments":"{}"}}]"#), "{s}");
        // No tool_calls -> field omitted.
        let plain = ResponseMessage { role: "assistant", content: "hi".into(), tool_calls: None };
        assert!(!serde_json::to_string(&plain).unwrap().contains("tool_calls"));
    }

    #[test]
    fn serializes_delta_tool_call() {
        let d = Delta { role: None, content: None, reasoning_content: None, tool_calls: Some(vec![DeltaToolCall {
            index: 0, id: Some("call_1".into()), kind: Some("function"),
            function: DeltaFunctionCall { name: Some("edit".into()), arguments: Some("{}".into()) },
        }])};
        let s = serde_json::to_string(&d).unwrap();
        assert!(s.contains(r#""tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"edit","arguments":"{}"}}]"#), "{s}");
    }
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib openai`
Expected: compile error (`ToolCall`, `FunctionCall`, `tool_calls` field, `DeltaToolCall` not defined).

- [ ] **Step 3: Write the implementation** in `src/openai.rs`.

Add `tool_calls` to `ChatMessage` (after `tool_call_id`):

```rust
    #[serde(default)]
    pub tool_calls: Option<Vec<ToolCall>>,
```

Add the tool-call types (place after the `ChatMessage` impl). `kind` is a `String` (not `&'static str`) so the type both `Serialize`s and `Deserialize`s — the incoming follow-up carries `"type":"function"`, and a `default` keeps it optional:

```rust
/// An assistant tool call (OpenAI function-calling). `arguments` is a JSON-encoded string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type", default = "function_kind")]
    pub kind: String, // "function"
    pub function: FunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

fn function_kind() -> String { "function".to_string() }
```

Add `tool_calls` to `ResponseMessage`:

```rust
#[derive(Debug, Clone, Serialize)]
pub struct ResponseMessage {
    pub role: &'static str, // "assistant"
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
}
```

Add the streaming delta tool-call types and the `tool_calls` field on `Delta`:

```rust
#[derive(Debug, Clone, Serialize)]
pub struct DeltaToolCall {
    pub index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<&'static str>, // "function"
    pub function: DeltaFunctionCall,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeltaFunctionCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<String>,
}
```

In `Delta` add (keep it `#[derive(Default)]`-compatible — `Option` defaults to `None`):

```rust
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<DeltaToolCall>>,
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `cargo test --lib openai`
Expected: the new 3 tests pass plus the existing openai tests.
Run: `cargo test`
Expected: all pass. (NOTE: every `Delta { .. }` literal in `http.rs` now needs `tool_calls: None` — if `cargo test` shows missing-field errors in `http.rs`, that is expected and fixed in Task 6; for now, add `..Default::default()` is NOT possible since Delta literals are explicit. Instead, temporarily add `tool_calls: None` to the `Delta` literals in `http.rs` to keep the suite green — Task 6 rewrites that mapping anyway.)

To keep this task self-contained and green, add `tool_calls: None` to the literals the new fields break:
- `src/http.rs` `sse_response`: each explicit `Delta { .. }` literal (the `AssistantText` arm, the `reasoning_content` arm, and the `Error` arm; `Delta::default()` in the `Done` arm already works). `grep -n "Delta {" src/http.rs`.
- `src/orchestrator.rs` `response_from_events`: the `ResponseMessage { role: "assistant", content }` literal → add `tool_calls: None,`.
- Any `ResponseMessage { .. }` literal in `tests/` (none expected — tests assert on serialized JSON, not construct `ResponseMessage`). `grep -rn "ResponseMessage {" src tests` to be sure.

(Task 6 rewrites both `response_from_events` and the SSE mapping to actually populate these.)

- [ ] **Step 5: Commit**

```bash
git add src/openai.rs src/http.rs
git commit -m "feat: OpenAI tool-call wire types (ToolCall/FunctionCall, tool_calls on message/response/delta)"
```

---

## Task 2: `AgentEvent::ToolCall` + `Caps.mcp_tools` gating

Adds the normalized `ToolCall` event a bridge emits when the agent calls a client tool, and renames the Phase-1 placeholder `Caps.mcp_tools_phase1` → `mcp_tools`, set **true for claude only** (codex gated pending the §9 spike; agy never). New-variant fallout in the two exhaustive `AgentEvent` matches is handled minimally here (real mapping is Task 6).

**Files:** Modify `src/engine/mod.rs`, `src/orchestrator.rs`, `src/http.rs`.

- [ ] **Step 1: Write the failing tests** — in `src/engine/mod.rs` test module, (a) update `agent_event_variants_construct` to also construct `ToolCall`, and (b) replace the `mcp_tools_phase1` assertions:

```rust
    #[test]
    fn tool_call_event_constructs() {
        let _ = AgentEvent::ToolCall { id: "call_1".into(), name: "search".into(), args: "{}".into() };
    }

    #[test]
    fn mcp_tools_caps_claude_only() {
        use crate::config::EngineKind;
        assert!(caps_for(EngineKind::Claude).mcp_tools);   // claude: per-invocation --mcp-config (safe)
        assert!(!caps_for(EngineKind::Codex).mcp_tools);   // gated pending the §9 injection spike
        assert!(!caps_for(EngineKind::Agy).mcp_tools);     // no per-invocation MCP config at all
    }
```

Also update the existing `caps_per_engine` test: change every `!claude.mcp_tools_phase1` / `!codex.mcp_tools_phase1` / `!agy.mcp_tools_phase1` to reference `.mcp_tools` with the new truth values — claude `&& claude.mcp_tools`, codex `&& !codex.mcp_tools`, agy `&& !agy.mcp_tools`. And in `claude_caps`, change `assert!(!c.mcp_tools_phase1)` to `assert!(c.mcp_tools)`.

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib engine`
Expected: compile error (`ToolCall` variant / `mcp_tools` field absent).

- [ ] **Step 3: Edit `src/engine/mod.rs`.**

Add the `ToolCall` variant to `AgentEvent` (after `ToolResult`):

```rust
    /// The agent invoked a client tool via the MCP bridge — becomes an OpenAI `tool_call` (Phase 4).
    ToolCall { id: String, name: String, args: String },
```

Rename the `Caps` field and update `caps_for`:

```rust
#[derive(Debug, Clone, Copy)]
pub struct Caps {
    pub streaming: bool,
    pub resume_by_id: bool,
    /// Per-invocation MCP tool injection safe? claude: true (--mcp-config --strict-mcp-config);
    /// codex: false (gated pending the §9 spike); agy: false (no per-invocation MCP config).
    pub mcp_tools: bool,
}

pub fn caps_for(engine: EngineKind) -> Caps {
    match engine {
        EngineKind::Claude => Caps { streaming: true, resume_by_id: true, mcp_tools: true },
        EngineKind::Codex => Caps { streaming: true, resume_by_id: true, mcp_tools: false },
        EngineKind::Agy => Caps { streaming: false, resume_by_id: false, mcp_tools: false },
    }
}
```

- [ ] **Step 4: Keep the two exhaustive `AgentEvent` matches compiling** (minimal — real mapping is Task 6).

In `src/orchestrator.rs` `response_from_events`, add `ToolCall` to the no-op arm:

```rust
            AgentEvent::Reasoning(_)
            | AgentEvent::ToolStart { .. }
            | AgentEvent::ToolResult { .. }
            | AgentEvent::ToolCall { .. }
            | AgentEvent::SessionId(_) => {}
```

In `src/http.rs` `sse_response`, add a temporary no-op arm to the `match ev` (place beside the `SessionId` arm):

```rust
                AgentEvent::ToolCall { .. } => (None, None), // mapped to tool_calls in Task 6
```

- [ ] **Step 5: Run to verify pass + full suite**

Run: `cargo test --lib engine`
Expected: the new tests pass.
Run: `cargo test`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mod.rs src/orchestrator.rs src/http.rs
git commit -m "feat: AgentEvent::ToolCall + Caps.mcp_tools (claude-only; codex/agy gated)"
```

---

## Task 3: Config knobs — `tool_result_timeout_s` + `max_suspended_sessions`

Adds the two `defaults` the suspended-session pool needs (spec §6): how long a parked tool-call group waits for results, and the cap on parked groups.

**Files:** Modify `src/config.rs`.

- [ ] **Step 1: Write the failing test** — extend `applies_defaults_for_optional_fields` in `src/config.rs` tests with:

```rust
        assert_eq!(cfg.defaults.tool_result_timeout_s, 120);
        assert_eq!(cfg.defaults.max_suspended_sessions, 16);
```

And add a test that they parse from YAML:

```rust
    #[test]
    fn parses_suspension_knobs() {
        let yaml = r#"
server: { bind: "127.0.0.1:9000" }
defaults: { tool_result_timeout_s: 45, max_suspended_sessions: 4 }
models: []
"#;
        let cfg = parse_config(yaml).unwrap();
        assert_eq!(cfg.defaults.tool_result_timeout_s, 45);
        assert_eq!(cfg.defaults.max_suspended_sessions, 4);
    }
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib config`
Expected: compile error (fields absent).

- [ ] **Step 3: Edit `src/config.rs`.**

Add the fields to `Defaults` (after `env_passthrough`):

```rust
    #[serde(default = "default_tool_result_timeout_s")]
    pub tool_result_timeout_s: u64,
    #[serde(default = "default_max_suspended_sessions")]
    pub max_suspended_sessions: usize,
```

Add the default fns (near `default_timeout_s`):

```rust
fn default_tool_result_timeout_s() -> u64 { 120 }
fn default_max_suspended_sessions() -> usize { 16 }
```

Update `impl Default for Defaults` to include them:

```rust
impl Default for Defaults {
    fn default() -> Self {
        Defaults {
            timeout_s: default_timeout_s(),
            max_concurrency: default_max_concurrency(),
            sandbox_backend: SandboxBackend::default(),
            env_passthrough: default_env_passthrough(),
            tool_result_timeout_s: default_tool_result_timeout_s(),
            max_suspended_sessions: default_max_suspended_sessions(),
        }
    }
}
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `cargo test --lib config`
Expected: pass.
Run: `cargo test`
Expected: all pass. (`Defaults::default()` is used in `tests/http_integration.rs` and `validate.rs` tests — adding fields with defaults keeps those literals valid since they call `::default()`, not struct literals.)

- [ ] **Step 5: Commit**

```bash
git add src/config.rs
git commit -m "feat: config tool_result_timeout_s (120) + max_suspended_sessions (16)"
```

---

## Task 4: Tool-result routing by message-list shape

A pure function implementing spec §4.3 step 2: a request is a tool-result **follow-up** ONLY when its **trailing** messages are a `role:"tool"` suffix (no user message after). Historical `tool` messages that appear *before* a final user turn are an ordinary turn (the normal case once a thread has past tool use, since OpenWebUI resends full history) — NOT a follow-up.

**Files:** Create `src/routing.rs`; modify `src/lib.rs`.

- [ ] **Step 1: Declare the module** — add to `src/lib.rs` (alphabetical):

```rust
pub mod routing;
```

- [ ] **Step 2: Write the failing tests** — `#[cfg(test)] mod tests` in `src/routing.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::openai::{ChatMessage, MessageContent, Role};

    fn msg(role: Role, text: &str, tcid: Option<&str>) -> ChatMessage {
        ChatMessage {
            role,
            content: Some(MessageContent::Text(text.into())),
            tool_call_id: tcid.map(String::from),
            tool_calls: None,
        }
    }

    #[test]
    fn trailing_tool_suffix_is_a_followup() {
        let msgs = vec![
            msg(Role::User, "do it", None),
            msg(Role::Assistant, "", None),
            msg(Role::Tool, "result A", Some("call_a")),
            msg(Role::Tool, "result B", Some("call_b")),
        ];
        let got = tool_result_suffix(&msgs).unwrap();
        assert_eq!(got, vec![("call_a".to_string(), "result A".to_string()),
                             ("call_b".to_string(), "result B".to_string())]);
    }

    #[test]
    fn historical_tool_then_user_turn_is_not_a_followup() {
        // Past tool exchange, but the thread ENDS with a new user message -> ordinary turn.
        let msgs = vec![
            msg(Role::User, "first", None),
            msg(Role::Assistant, "", None),
            msg(Role::Tool, "old result", Some("call_old")),
            msg(Role::Assistant, "answer", None),
            msg(Role::User, "second", None),
        ];
        assert!(tool_result_suffix(&msgs).is_none());
    }

    #[test]
    fn ends_with_assistant_or_empty_is_not_a_followup() {
        assert!(tool_result_suffix(&[msg(Role::Assistant, "hi", None)]).is_none());
        assert!(tool_result_suffix(&[]).is_none());
        assert!(tool_result_suffix(&[msg(Role::User, "hi", None)]).is_none());
    }
}
```

- [ ] **Step 3: Run to confirm failure**

Run: `cargo test --lib routing`
Expected: compile error (`tool_result_suffix` undefined).

- [ ] **Step 4: Write the implementation** (top of `src/routing.rs`):

```rust
//! Tool-result routing by message-list SHAPE (spec §4.3 step 2). A request is a tool-result
//! follow-up ONLY when its trailing messages are a `role:"tool"` suffix (no user message after).
//! Historical tool messages before a final user turn are an ordinary turn, not a follow-up.
use crate::openai::{ChatMessage, Role};

/// If `messages` ends with one or more `role:"tool"` messages (a tool-result suffix), return the
/// `(tool_call_id, content)` pair for each in original order. Otherwise `None` (an ordinary turn).
pub fn tool_result_suffix(messages: &[ChatMessage]) -> Option<Vec<(String, String)>> {
    if messages.last().map(|m| m.role) != Some(Role::Tool) {
        return None;
    }
    let mut out: Vec<(String, String)> = Vec::new();
    for m in messages.iter().rev() {
        if m.role != Role::Tool {
            break;
        }
        out.push((m.tool_call_id.clone().unwrap_or_default(), m.text()));
    }
    out.reverse();
    Some(out)
}
```

- [ ] **Step 5: Run to verify pass + full suite**

Run: `cargo test --lib routing`
Expected: 3 passed.
Run: `cargo test`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/routing.rs src/lib.rs
git commit -m "feat: tool-result routing by message-list shape (trailing role:tool suffix)"
```

---

## Task 5: `SuspendedSessions` registry — turn-level groups, all-or-409, timeout, cap

The state machine (spec §4.6). A parked turn = a **group** of outstanding `tool_call_id`s, each holding a `oneshot::Sender<String>` the follow-up delivers a result into, plus the turn's continuation `EventStream` held until the **full** result set arrives. `register` enforces `max_suspended_sessions` (→ `503`); `deliver` is all-or-error (complete → fire all senders + return the continuation; partial → `409` + group stays live; duplicate/unknown → `400`); `reap` (driven by a `tool_result_timeout_s` timer) drops an idle group. Hermetic — the continuation is any `EventStream`; Phase 4b passes one backed by the live CLI + rmcp handlers.

> **Accounting note (spec §4.7):** the active-`max_concurrency` slot release-on-park / re-acquire-on-resume involves the `ProcessSupervisor` permit a live child holds — that is **Phase 4b** (4a has no child). Phase 4a enforces only the `max_suspended_sessions` count cap (`503`).

**Files:** Create `src/suspend.rs`; modify `src/lib.rs`.

- [ ] **Step 1: Declare the module** — add to `src/lib.rs` (alphabetical):

```rust
pub mod suspend;
```

- [ ] **Step 2: Write the failing tests** — `#[cfg(test)] mod tests` in `src/suspend.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::AgentEvent;
    use futures::StreamExt;
    use tokio::sync::oneshot;

    fn continuation() -> crate::engine::EventStream {
        Box::pin(futures::stream::iter(vec![
            AgentEvent::AssistantText("continued".into()),
            AgentEvent::Done { finish_reason: "stop".into() },
        ]))
    }

    /// Build (ids, senders-for-register, receivers-to-observe).
    fn channels(ids: &[&str]) -> (Vec<(String, oneshot::Sender<String>)>, Vec<oneshot::Receiver<String>>) {
        let mut pairs = Vec::new();
        let mut rxs = Vec::new();
        for id in ids {
            let (tx, rx) = oneshot::channel();
            pairs.push(((*id).to_string(), tx));
            rxs.push(rx);
        }
        (pairs, rxs)
    }

    #[tokio::test]
    async fn complete_delivery_fires_senders_and_returns_continuation() {
        let s = SuspendedSessions::new(8);
        let (pairs, mut rxs) = channels(&["call_a", "call_b"]);
        s.register(pairs, continuation()).unwrap();
        assert_eq!(s.live_count(), 1);

        let cont = s.deliver(&[("call_a".into(), "RA".into()), ("call_b".into(), "RB".into())]).unwrap();
        assert_eq!(rxs[0].try_recv().unwrap(), "RA");
        assert_eq!(rxs[1].try_recv().unwrap(), "RB");
        assert_eq!(s.live_count(), 0); // group removed
        let evs: Vec<_> = cont.collect().await;
        assert!(evs.iter().any(|e| matches!(e, AgentEvent::AssistantText(t) if t == "continued")));
    }

    #[tokio::test]
    async fn partial_set_is_409_and_keeps_group_live() {
        let s = SuspendedSessions::new(8);
        let (pairs, _rxs) = channels(&["call_a", "call_b"]);
        s.register(pairs, continuation()).unwrap();
        let err = s.deliver(&[("call_a".into(), "RA".into())]).unwrap_err();
        assert_eq!(err, DeliverError::Partial(vec!["call_b".into()]));
        assert_eq!(s.live_count(), 1); // still parked, awaiting the rest
    }

    #[tokio::test]
    async fn duplicate_and_unknown_are_400() {
        let s = SuspendedSessions::new(8);
        let (pairs, _rxs) = channels(&["call_a"]);
        s.register(pairs, continuation()).unwrap();
        assert_eq!(
            s.deliver(&[("call_a".into(), "x".into()), ("call_a".into(), "y".into())]).unwrap_err(),
            DeliverError::Duplicate("call_a".into())
        );
        assert_eq!(s.deliver(&[("nope".into(), "x".into())]).unwrap_err(), DeliverError::Unknown);
        assert_eq!(s.live_count(), 1);
    }

    #[tokio::test]
    async fn register_past_cap_is_full() {
        let s = SuspendedSessions::new(1);
        let (p1, _r1) = channels(&["a"]);
        s.register(p1, continuation()).unwrap();
        let (p2, _r2) = channels(&["b"]);
        assert_eq!(s.register(p2, continuation()).unwrap_err(), RegisterError::Full);
    }

    #[tokio::test]
    async fn reap_removes_idle_group() {
        let s = SuspendedSessions::new(8);
        let (pairs, _rxs) = channels(&["call_a"]);
        let gid = s.register(pairs, continuation()).unwrap();
        s.spawn_reaper(gid, std::time::Duration::from_millis(10));
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert_eq!(s.live_count(), 0);
        assert_eq!(s.deliver(&[("call_a".into(), "x".into())]).unwrap_err(), DeliverError::Unknown);
    }
}
```

- [ ] **Step 3: Run to confirm failure**

Run: `cargo test --lib suspend`
Expected: compile error (module empty).

- [ ] **Step 4: Write the implementation** (top of `src/suspend.rs`):

```rust
//! In-memory registry of suspended tool-call groups (spec §4.6). The process that hit client tool
//! calls is parked: each outstanding tool_call_id holds a `oneshot` the follow-up delivers a result
//! into, and the turn's continuation `EventStream` is held until the FULL result set arrives.
//! Complete follow-up → fire every sender + return the continuation; partial → 409 (group stays
//! live); duplicate/unknown id → 400; idle group reaped after `tool_result_timeout_s`; registering
//! past `max_suspended_sessions` → 503.
//!
//! Phase 4a is driven by a fake bridge (the continuation is a scripted stream awaiting the same
//! oneshots). Phase 4b stores the live CLI child + the rmcp server's parked call handlers behind
//! this identical API (and additionally manages the active-concurrency permit, §4.7).
use crate::engine::EventStream;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

#[derive(Debug, PartialEq, Eq)]
pub enum RegisterError {
    /// `max_suspended_sessions` reached — refuse the new suspension (HTTP 503).
    Full,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DeliverError {
    /// An id matches no live group (unknown or already-timed-out) — HTTP 400.
    Unknown,
    /// The group has outstanding ids not covered by this follow-up — HTTP 409. Group stays live.
    Partial(Vec<String>),
    /// An id supplied twice in the same follow-up — HTTP 400.
    Duplicate(String),
}

/// One parked turn.
struct Group {
    outstanding: HashSet<String>,
    senders: HashMap<String, oneshot::Sender<String>>,
    continuation: EventStream,
}

struct Inner {
    groups: HashMap<u64, Group>, // group_id -> Group
    index: HashMap<String, u64>, // tool_call_id -> group_id
}

#[derive(Clone)]
pub struct SuspendedSessions {
    inner: Arc<Mutex<Inner>>,
    max: usize,
    next_id: Arc<AtomicU64>,
}

impl SuspendedSessions {
    pub fn new(max_suspended_sessions: usize) -> Self {
        SuspendedSessions {
            inner: Arc::new(Mutex::new(Inner { groups: HashMap::new(), index: HashMap::new() })),
            max: max_suspended_sessions.max(1),
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Park a turn: `pairs` is (tool_call_id, result-delivery sender) per outstanding call;
    /// `continuation` resumes once every result is delivered. Returns the new group id, or
    /// `Full` when at `max_suspended_sessions`.
    pub fn register(
        &self,
        pairs: Vec<(String, oneshot::Sender<String>)>,
        continuation: EventStream,
    ) -> Result<u64, RegisterError> {
        let mut g = self.inner.lock().unwrap();
        if g.groups.len() >= self.max {
            return Err(RegisterError::Full);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let outstanding: HashSet<String> = pairs.iter().map(|(k, _)| k.clone()).collect();
        let senders: HashMap<String, oneshot::Sender<String>> = pairs.into_iter().collect();
        for k in &outstanding {
            g.index.insert(k.clone(), id);
        }
        g.groups.insert(id, Group { outstanding, senders, continuation });
        Ok(id)
    }

    /// Deliver a follow-up's result set. On a complete, valid set: fire every sender, remove the
    /// group, and return its continuation `EventStream` (the response to *this* follow-up).
    pub fn deliver(&self, results: &[(String, String)]) -> Result<EventStream, DeliverError> {
        let mut g = self.inner.lock().unwrap();

        // No duplicate ids within this follow-up.
        let mut seen = HashSet::new();
        for (id, _) in results {
            if !seen.insert(id.as_str()) {
                return Err(DeliverError::Duplicate(id.clone()));
            }
        }
        // Resolve the group from the first id; every id must belong to that SAME live group.
        let Some((first, _)) = results.first() else {
            return Err(DeliverError::Unknown);
        };
        let Some(&gid) = g.index.get(first) else {
            return Err(DeliverError::Unknown);
        };
        if results.iter().any(|(id, _)| g.index.get(id) != Some(&gid)) {
            return Err(DeliverError::Unknown); // an id from another/no group
        }
        // Completeness: the follow-up must cover every outstanding id of the group.
        {
            let group = g.groups.get(&gid).expect("indexed group present");
            let provided: HashSet<&str> = results.iter().map(|(id, _)| id.as_str()).collect();
            let mut missing: Vec<String> =
                group.outstanding.iter().filter(|id| !provided.contains(id.as_str())).cloned().collect();
            if !missing.is_empty() {
                missing.sort();
                return Err(DeliverError::Partial(missing));
            }
        }
        // Complete: remove the group, fire each sender, hand back the continuation.
        let mut group = g.groups.remove(&gid).unwrap();
        for id in &group.outstanding {
            g.index.remove(id);
        }
        for (id, content) in results {
            if let Some(tx) = group.senders.remove(id) {
                let _ = tx.send(content.clone());
            }
        }
        Ok(group.continuation)
    }

    /// Remove a group if still present. Dropping it drops the senders (parked calls see `Closed`)
    /// and the continuation (in Phase 4b that drops the held CLI child, killing it).
    pub fn reap(&self, group_id: u64) {
        let mut g = self.inner.lock().unwrap();
        if let Some(group) = g.groups.remove(&group_id) {
            for id in &group.outstanding {
                g.index.remove(id);
            }
        }
    }

    /// Spawn the orphan-timeout reaper for a group (`tool_result_timeout_s`). A no-op if the group
    /// was already delivered/removed by the time it fires.
    pub fn spawn_reaper(&self, group_id: u64, timeout: std::time::Duration) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            this.reap(group_id);
        });
    }

    pub fn live_count(&self) -> usize {
        self.inner.lock().unwrap().groups.len()
    }
}
```

- [ ] **Step 5: Run to verify pass + full suite**

Run: `cargo test --lib suspend`
Expected: 5 passed.
Run: `cargo test`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/suspend.rs src/lib.rs
git commit -m "feat: SuspendedSessions registry (turn-level groups, all-or-409, duplicate/unknown-400, cap-503, reaper)"
```

---

## Task 6: Map `AgentEvent::ToolCall` → OpenAI `tool_calls` (aggregate + SSE)

Replaces the Task-2 no-op handling: `response_from_events` collects `ToolCall`s into the response message's `tool_calls` (forcing `finish_reason:"tool_calls"`), and `sse_response` emits a `delta.tool_calls` chunk per `ToolCall`. Engine-agnostic — exercised here via a `FakeRunner` emitting `ToolCall`s on the ordinary path (gating/suspension wiring is Task 7).

**Files:** Modify `src/orchestrator.rs`, `src/http.rs`; add tests to `tests/http_integration.rs`.

- [ ] **Step 1: Write the failing tests.**

In `src/orchestrator.rs` test module, add an aggregate test:

```rust
    #[test]
    fn response_from_events_collects_tool_calls_and_forces_finish_reason() {
        let resp = aggregate(vec![
            AgentEvent::ToolCall { id: "call_1".into(), name: "search".into(), args: r#"{"q":"x"}"#.into() },
            AgentEvent::Done { finish_reason: "tool_calls".into() },
        ]);
        assert_eq!(resp.choices[0].finish_reason, "tool_calls");
        let calls = resp.choices[0].message.tool_calls.as_ref().unwrap();
        assert_eq!(calls[0].id, "call_1");
        assert_eq!(calls[0].function.name, "search");
        assert_eq!(calls[0].function.arguments, r#"{"q":"x"}"#);
        assert_eq!(resp.choices[0].message.content, "");
    }
```

In `tests/http_integration.rs`, add an SSE mapping test (the `FakeRunner` there emits whatever events it is given):

```rust
#[tokio::test]
async fn streaming_emits_tool_calls_chunk() {
    let runner = fake(vec![
        AgentEvent::ToolCall { id: "call_1".into(), name: "search".into(), args: "{}".into() },
        AgentEvent::Done { finish_reason: "tool_calls".into() },
    ]);
    let app = build_router(state_with(None, runner));
    let body = r#"{"model":"claude-text","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    let b = body_string(r).await;
    assert!(b.contains(r#""tool_calls":[{"index":0,"id":"call_1","type":"function""#), "{b}");
    assert!(b.contains(r#""name":"search""#), "{b}");
    assert!(b.contains(r#""finish_reason":"tool_calls""#), "{b}");
    assert!(b.trim_end().ends_with("data: [DONE]"), "{b}");
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib orchestrator` and `cargo test --test http_integration`
Expected: failures (tool_calls not populated; no `tool_calls` chunk emitted).

- [ ] **Step 3: Rewrite `response_from_events` in `src/orchestrator.rs`** to collect tool calls:

```rust
pub fn response_from_events(
    events: Vec<AgentEvent>,
    model_id: &str,
) -> Result<ChatCompletionResponse, EngineError> {
    let mut content = String::new();
    let mut tool_calls: Vec<crate::openai::ToolCall> = Vec::new();
    let mut finish_reason = "stop".to_string();
    for ev in events {
        match ev {
            AgentEvent::AssistantText(t) => content.push_str(&t),
            AgentEvent::ToolCall { id, name, args } => tool_calls.push(crate::openai::ToolCall {
                id,
                kind: "function".into(),
                function: crate::openai::FunctionCall { name, arguments: args },
            }),
            AgentEvent::Done { finish_reason: fr } => finish_reason = fr,
            AgentEvent::Error(m) => return Err(EngineError::Reported(m)),
            AgentEvent::Reasoning(_)
            | AgentEvent::ToolStart { .. }
            | AgentEvent::ToolResult { .. }
            | AgentEvent::SessionId(_) => {}
        }
    }
    // OpenAI: when the turn made tool calls, the message carries `tool_calls` and finishes "tool_calls".
    let (tool_calls, finish_reason) = if tool_calls.is_empty() {
        (None, finish_reason)
    } else {
        (Some(tool_calls), "tool_calls".to_string())
    };
    Ok(ChatCompletionResponse {
        id: format!("chatcmpl-{}", unix_now()),
        object: "chat.completion",
        created: unix_now(),
        model: model_id.to_string(),
        choices: vec![Choice {
            index: 0,
            message: ResponseMessage { role: "assistant", content, tool_calls },
            finish_reason,
        }],
        usage: Usage::default(),
    })
}
```

- [ ] **Step 4: Update `sse_response` in `src/http.rs`.**

Add the streaming types to the openai import: `use crate::openai::{ApiError, ChatCompletionChunk, ChatCompletionRequest, ChunkChoice, Delta, DeltaFunctionCall, DeltaToolCall};`.

Add a tool-call index counter beside `role_sent` (before the `async_stream::stream!`):

```rust
    let mut role_sent = false;
    let mut tool_idx: u32 = 0;
```

Replace the temporary `AgentEvent::ToolCall { .. } => (None, None)` arm with the real mapping:

```rust
                AgentEvent::ToolCall { id, name, args } => {
                    let role = (!role_sent).then(|| { role_sent = true; "assistant" });
                    let index = tool_idx;
                    tool_idx += 1;
                    let delta = Delta {
                        role,
                        content: None,
                        reasoning_content: None,
                        tool_calls: Some(vec![DeltaToolCall {
                            index,
                            id: Some(id),
                            kind: Some("function"),
                            function: DeltaFunctionCall { name: Some(name), arguments: Some(args) },
                        }]),
                    };
                    (Some(delta), None)
                }
```

- [ ] **Step 5: Run to verify pass + full suite**

Run: `cargo test --lib orchestrator && cargo test --test http_integration`
Expected: the new tests pass.
Run: `cargo test && cargo clippy --all-targets`
Expected: all pass; clippy clean.

- [ ] **Step 6: Commit**

```bash
git add src/orchestrator.rs src/http.rs tests/http_integration.rs
git commit -m "feat: map AgentEvent::ToolCall -> OpenAI tool_calls (aggregate + SSE delta)"
```

---

## Task 7: HTTP wiring — `AppState.suspended`, tools gating, follow-up routing

Ties the protocol together in `chat_completions`: a tool-result follow-up (routing-by-shape) is delivered to the `SuspendedSessions` registry (complete → drain the continuation as the response; partial → `409`; duplicate/unknown → `400`); a new `tools` turn is gated per `caps().mcp_tools` (codex/agy → clear error; claude → a clear "Phase 4b" deferral, since the live bridge that *creates* suspensions is 4b). Replaces the blanket `tools`→`400 (Phase 4)` rejection.

> **What 4a does NOT wire (4b):** the path that *registers* a suspension (the live claude MCP turn) — so in 4a no suspension is created in production, and `503`-on-overflow / active-permit accounting are exercised at the registry level (Task 5) and wired into the request path in 4b. The follow-up *routing/resume* path here is real and is tested by pre-registering a suspension.

**Files:** Modify `src/http.rs`, `src/main.rs`, `tests/http_integration.rs`.

- [ ] **Step 1: Write the failing integration tests** — in `tests/http_integration.rs`.

Update the imports and `state_with` to add the registry + a codex model (so gating is testable):

```rust
use llm_bridge::suspend::SuspendedSessions;
use std::time::Duration;
// (keep existing imports: Credentials, Defaults, EngineKind, Mode, ModelEntry, ProgressChannel, AgentEvent, EventStream, Turn, ...)
```

In `state_with`, add a codex model to the registry and the two new `AppState` fields:

```rust
    let models = vec![
        ModelEntry { id: "claude-text".into(), engine: EngineKind::Claude, model: Some("sonnet".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false },
        ModelEntry { id: "codex-text".into(), engine: EngineKind::Codex, model: Some("gpt-5".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false },
    ];
    AppState {
        registry: Arc::new(Registry::new(models)),
        bearer_token: token.map(String::from),
        runner,
        sessions: Arc::new(SessionStore::new()),
        defaults: Defaults::default(),
        progress_channel: ProgressChannel::ReasoningContent,
        credentials: Credentials::default(),
        suspended: Arc::new(SuspendedSessions::new(8)),
        tool_result_timeout: Duration::from_secs(120),
    }
```

Add the tests:

```rust
#[tokio::test]
async fn tools_to_codex_rejected_unsupported() {
    let app = build_router(state(None));
    let body = r#"{"model":"codex-text","tools":[{"type":"function","function":{"name":"f"}}],"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(r.status(), 400);
    assert!(body_string(r).await.contains("not supported for engine 'codex'"));
}

#[tokio::test]
async fn tools_to_claude_deferred_to_phase4b() {
    let app = build_router(state(None));
    let body = r#"{"model":"claude-text","tools":[{"type":"function","function":{"name":"f"}}],"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(r.status(), 400);
    assert!(body_string(r).await.contains("Phase 4b"));
}

// Build a state, pre-register a suspension keyed by the given ids, return (state, receivers).
fn state_with_suspension(ids: &[&str]) -> (AppState, Vec<tokio::sync::oneshot::Receiver<String>>) {
    let st = state(None);
    let mut pairs = Vec::new();
    let mut rxs = Vec::new();
    for id in ids {
        let (tx, rx) = tokio::sync::oneshot::channel();
        pairs.push(((*id).to_string(), tx));
        rxs.push(rx);
    }
    let cont: EventStream = Box::pin(futures::stream::iter(vec![
        AgentEvent::AssistantText("resumed".into()),
        AgentEvent::Done { finish_reason: "stop".into() },
    ]));
    st.suspended.register(pairs, cont).unwrap();
    (st, rxs)
}

fn followup_body(results: &[(&str, &str)]) -> String {
    let calls: Vec<String> = results.iter().map(|(id, _)|
        format!(r#"{{"id":"{id}","type":"function","function":{{"name":"f","arguments":"{{}}"}}}}"#)).collect();
    let tools: Vec<String> = results.iter().map(|(id, res)|
        format!(r#"{{"role":"tool","tool_call_id":"{id}","content":"{res}"}}"#)).collect();
    format!(r#"{{"model":"claude-text","messages":[
        {{"role":"user","content":"do it"}},
        {{"role":"assistant","content":null,"tool_calls":[{}]}},
        {}
    ]}}"#, calls.join(","), tools.join(","))
}

#[tokio::test]
async fn tool_result_followup_resumes_suspension() {
    let (st, _rxs) = state_with_suspension(&["call_a"]);
    let app = build_router(st);
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(followup_body(&[("call_a","RES")]))).unwrap()).await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(body_string(r).await.contains("resumed"));
}

#[tokio::test]
async fn partial_tool_results_409() {
    let (st, _rxs) = state_with_suspension(&["call_a", "call_b"]);
    let app = build_router(st);
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(followup_body(&[("call_a","RES")]))).unwrap()).await.unwrap();
    assert_eq!(r.status(), 409);
    assert!(body_string(r).await.contains("call_b"));
}

#[tokio::test]
async fn unknown_tool_result_400() {
    let app = build_router(state(None)); // no suspension registered
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(followup_body(&[("call_x","RES")]))).unwrap()).await.unwrap();
    assert_eq!(r.status(), 400);
}
```

Also update the existing `unknown_model_404_and_tools_400` test: the `tools` case now returns 400 with a DIFFERENT message ("Phase 4b" / "not supported") — it still asserts `r2.status() == 400`, so it stays valid. Leave it (or rename its comment).

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --test http_integration`
Expected: compile error (`AppState` missing `suspended`/`tool_result_timeout`).

- [ ] **Step 3: Edit `src/http.rs`.**

Add imports:

```rust
use crate::engine::EventStream;
use crate::routing::tool_result_suffix;
use crate::suspend::{DeliverError, SuspendedSessions};
```

Add the two fields to `AppState`:

```rust
    pub suspended: Arc<SuspendedSessions>,
    pub tool_result_timeout: Duration, // reserved for the registration path wired in Phase 4b
```

Rewrite `chat_completions` (model resolve first, then follow-up routing, then tools gating, then ordinary path):

```rust
async fn chat_completions(State(state): State<AppState>, Json(req): Json<ChatCompletionRequest>) -> Response {
    let Some(entry) = state.registry.resolve(&req.model) else {
        return err(StatusCode::NOT_FOUND, &format!("unknown model '{}'", req.model), "model_not_found");
    };
    let entry = entry.clone();
    let model_id = req.model.clone();
    let streaming = req.is_streaming();
    let progress = state.progress_channel;

    // (1) Tool-result follow-up? Route the trailing role:"tool" suffix to its parked suspension.
    if let Some(results) = tool_result_suffix(&req.messages) {
        return match state.suspended.deliver(&results) {
            Ok(continuation) => finish(continuation, model_id, streaming, progress).await,
            Err(DeliverError::Partial(missing)) => err(
                StatusCode::CONFLICT,
                &format!("incomplete tool results; still awaiting: {}", missing.join(", ")),
                "invalid_request_error",
            ),
            Err(DeliverError::Duplicate(id)) => err(
                StatusCode::BAD_REQUEST,
                &format!("duplicate tool result for tool_call_id '{id}'"),
                "invalid_request_error",
            ),
            Err(DeliverError::Unknown) => err(
                StatusCode::BAD_REQUEST,
                "unknown or expired tool_call_id(s); no live suspended turn matches",
                "invalid_request_error",
            ),
        };
    }

    // (2) New turn carrying `tools` → gate per engine capability.
    if req.has_tools() {
        if !crate::engine::caps_for(entry.engine).mcp_tools {
            return err(
                StatusCode::BAD_REQUEST,
                &format!("`tools` (function calling) is not supported for engine '{}'", entry.engine.as_str()),
                "invalid_request_error",
            );
        }
        // claude is MCP-capable, but the live in-process MCP bridge that creates suspensions
        // is delivered in Phase 4b.
        return err(
            StatusCode::BAD_REQUEST,
            "the MCP tool bridge is not yet enabled (Phase 4b)",
            "invalid_request_error",
        );
    }

    // (3) Ordinary turn.
    let engine_home = match entry.engine {
        EngineKind::Claude => state.credentials.claude_config_dir.clone(),
        EngineKind::Codex => state.credentials.codex_home.clone(),
        EngineKind::Agy => state.credentials.agy_config_dir.clone(),
    };
    let rt = runtime_fingerprint(&engine_home, state.defaults.sandbox_backend.as_str());
    let events = run_request(state.runner.clone(), state.sessions.clone(), &entry, &req, &rt);
    finish(events, model_id, streaming, progress).await
}

/// Shared tail: stream the events as SSE, or aggregate into one completion.
async fn finish(events: EventStream, model_id: String, streaming: bool, progress: ProgressChannel) -> Response {
    if streaming {
        sse_response(events, model_id, progress).into_response()
    } else {
        let collected: Vec<AgentEvent> = events.collect().await;
        match response_from_events(collected, &model_id) {
            Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
            Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "engine_error"),
        }
    }
}
```

(If `state.defaults.sandbox_backend.as_str()` differs from the Phase-3 fingerprint string, keep whatever the current Phase-3 code uses — the point is only to preserve the existing ordinary-path behavior. `as_str()` is correct post-Task-6 of Phase 3.)

- [ ] **Step 4: Edit `src/main.rs`** — build the registry + pass the new `AppState` fields:

```rust
    let state = AppState {
        registry: Arc::new(Registry::new(cfg.models.clone())),
        bearer_token: cfg.server.bearer_token.clone(),
        runner,
        sessions: Arc::new(llm_bridge::session::SessionStore::new()),
        defaults: cfg.defaults.clone(),
        progress_channel: cfg.server.progress_channel,
        credentials: cfg.credentials.clone(),
        suspended: Arc::new(llm_bridge::suspend::SuspendedSessions::new(cfg.defaults.max_suspended_sessions)),
        tool_result_timeout: Duration::from_secs(cfg.defaults.tool_result_timeout_s),
    };
```

(`Duration` is already imported in `main.rs`.)

- [ ] **Step 5: Run to verify pass + full suite + clippy**

Run: `cargo test --test http_integration`
Expected: the 5 new tests pass (codex-unsupported, claude-4b, resume-200, partial-409, unknown-400) plus the existing ones.
Run: `cargo test && cargo clippy --all-targets`
Expected: all pass; clippy clean. (`tool_result_timeout` is stored-but-unused until Phase 4b — it is `pub` on `AppState`, so no dead-code warning.)
Run: `cargo build --features e2e_smoke --tests`
Expected: compiles.

- [ ] **Step 6: Commit**

```bash
git add src/http.rs src/main.rs tests/http_integration.rs
git commit -m "feat: tools gating (per caps) + tool-result follow-up routing to SuspendedSessions (resume/409/400)"
```

---

## Task 8: README status + scope note

Records the Phase 4a state (tool-call protocol + suspension state machine in; live bridge in 4b). No e2e (4b owns real-claude tool use).

**Files:** Modify `README.md`.

- [ ] **Step 1: Update the `README.md` Status section** — replace the "## Status (Phase 3)" block with:

````markdown
## Status (Phase 4a)
- ✅ `GET /health`, `GET /v1/models`, `POST /v1/chat/completions` — streaming (SSE) and non-streaming
- ✅ Three engines: `claude` (stream-json), `codex` (`exec --json`), `agy` (`--print`); session resume for claude+codex; bubblewrap sandbox + startup canary probes
- ✅ **OpenAI `tools` protocol core**: `tool_calls` out (aggregate + SSE `delta.tool_calls`), tool-result follow-up **routing by message shape**, and the **suspended tool-call registry** (turn-level groups: all-or-`409`, duplicate/unknown→`400`, idle-reap, `max_suspended_sessions`→`503`)
- ⛔ The **live MCP bridge** (in-process `rmcp` server + claude `--mcp-config` wiring) lands in **Phase 4b** — until then `tools` to claude → `400 "Phase 4b"`; `tools` to codex/agy → `400 "not supported for engine …"` (codex gated pending the §9 spike; agy never)
````

- [ ] **Step 2: Verify + commit**

Run: `cargo test && cargo clippy --all-targets`
Expected: all pass; clippy clean.

```bash
git add README.md
git commit -m "docs: README Status (Phase 4a) — tool-call protocol + suspension registry; live bridge in 4b"
```

---

## Phase 4a Done — Definition of Done
- `cargo build` clean; `cargo test` green; `cargo clippy --all-targets` clean. No new crate dependencies (rmcp arrives in 4b).
- **Wire types:** `tool_calls` round-trip — emitted on the assistant response (aggregate `message.tool_calls`, SSE `delta.tool_calls`, `finish_reason:"tool_calls"`), and parsed on the incoming follow-up (assistant `tool_calls` + `role:"tool"` results).
- **Capability gating:** `caps(Claude).mcp_tools == true`, codex/agy `false`. A `tools` request to codex/agy returns a clear "not supported for engine '…'" `400`.
- **Routing by shape:** a thread *ending* with a `role:"tool"` suffix routes to a suspension; a thread containing historical tool messages but *ending with a user turn* is an ordinary turn (verified by `routing` tests).
- **Suspension state machine:** `register` enforces `max_suspended_sessions` (`503`); `deliver` is all-or-error — complete → fire every `oneshot` + return the continuation `EventStream` (drained as the follow-up response); partial → `409` (group stays live); duplicate/unknown id → `400`; an idle group is reaped after `tool_result_timeout_s` (`reap` + `spawn_reaper`). All unit-tested hermetically; the follow-up resume/`409`/`400` paths are HTTP-tested by pre-registering a suspension.
- **Deferred to Phase 4b (clearly errored, never silently allowed):** the live in-process `rmcp` MCP server, per-invocation claude `--mcp-config <temp-file> --strict-mcp-config` wiring, the held-open CLI child, the suspension *creation* path (so `503`-on-overflow + active-`max_concurrency` permit release/re-acquire wire into the request path there), consuming the incoming tool *definitions*, and the real-claude e2e. `tools` to claude returns `400 "Phase 4b"` until then.

**Next:** Phase 4b — stand up the `rmcp` `ServerHandler` (`list_tools` from the request's tool defs; async `call_tool` that generates a `tool_call_id`, emits `AgentEvent::ToolCall`, and parks on a `oneshot`), serve it over SSE on an ephemeral port, wire claude via a per-request temp `--mcp-config --strict-mcp-config`, register the parked continuation in `SuspendedSessions` (the API built here), and add the feature-gated real-claude round-trip e2e.
