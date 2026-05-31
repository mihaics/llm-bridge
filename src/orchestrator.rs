//! Turn orchestration: the streaming `TurnRunner`, the production `EngineProcessRunner`, the
//! session-aware `run_request` (key resolution, resume vs fresh, capture + index advancement),
//! and the events->response aggregation used by the non-streaming path.
use crate::config::{Credentials, EngineKind, Mode, ModelEntry, SandboxBackend};
use crate::engine::agy::AgyAdapter;
use crate::engine::claude::ClaudeAdapter;
use crate::engine::codex::CodexAdapter;
use crate::engine::{caps_for, AgentEvent, Engine, EngineError, EventStream, Turn};
use crate::openai::{
    ChatCompletionRequest, ChatCompletionResponse, ChatMessage, Choice, ResponseMessage, Role, Usage,
};
use crate::mcp::McpServer;
use crate::openai::{FunctionCall, ToolCall};
use crate::process::ProcessSupervisor;
use crate::session::{lookup_key, stored_key_after, RuntimeFingerprint, SessionStore};
use crate::suspend::SuspendedSessions;
use crate::transcript::render_turn;
use futures::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Injectable streaming run layer. Production spawns claude; tests return a canned stream.
pub trait TurnRunner: Send + Sync {
    fn run_stream(&self, turn: Turn) -> EventStream;
}

/// Production runner: dispatches per `turn.engine`, builds that engine's command, and streams its
/// parsed events through the shared supervisor. Spawn/timeout failures surface as `AgentEvent::Error`.
/// At clean EOF, if the engine emitted no terminal event (agy's plain-text path), a synthetic
/// `Done { "stop" }` is appended so the aggregator/SSE layer always sees a finish.
pub struct EngineProcessRunner {
    pub supervisor: ProcessSupervisor,
    pub credentials: Credentials,
    pub env_passthrough: Vec<String>,
    pub timeout: Duration,
    pub sandbox_backend: SandboxBackend,
}

fn engine_label(k: EngineKind) -> &'static str {
    match k {
        EngineKind::Claude => "claude",
        EngineKind::Codex => "codex",
        EngineKind::Agy => "agy",
    }
}

impl EngineProcessRunner {
    fn build_engine(&self, kind: EngineKind) -> Engine {
        match kind {
            EngineKind::Claude => Engine::Claude(ClaudeAdapter::new("claude", self.credentials.claude_config_dir.clone())),
            EngineKind::Codex => Engine::Codex(CodexAdapter::new("codex", self.credentials.codex_home.clone())),
            EngineKind::Agy => Engine::Agy(AgyAdapter::new("agy", self.credentials.agy_config_dir.clone())),
        }
    }
}

impl TurnRunner for EngineProcessRunner {
    fn run_stream(&self, turn: Turn) -> EventStream {
        let engine = self.build_engine(turn.engine);
        let (cmd, stdin) = engine.build_stream_command(&turn, &self.env_passthrough);
        // Expose ONLY the resolved engine's own home dir read-only inside the sandbox.
        let engine_home = match turn.engine {
            EngineKind::Claude => self.credentials.claude_config_dir.clone(),
            EngineKind::Codex => self.credentials.codex_home.clone(),
            EngineKind::Agy => self.credentials.agy_config_dir.clone(),
        };
        let ro_paths: Vec<PathBuf> = engine_home.into_iter().collect();
        let cmd = crate::sandbox::maybe_wrap(self.sandbox_backend, turn.workspace.as_deref(), &ro_paths, cmd);
        let lines = self.supervisor.spawn_streaming(cmd, stdin, self.timeout);
        let kind = turn.engine;
        Box::pin(async_stream::stream! {
            futures::pin_mut!(lines);
            let mut saw_terminal = false;
            while let Some(item) = lines.next().await {
                match item {
                    Ok(line) => {
                        for ev in engine.parse_stream_line(&line) {
                            if matches!(ev, AgentEvent::Done { .. } | AgentEvent::Error(_)) {
                                saw_terminal = true;
                            }
                            yield ev;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                        yield AgentEvent::Error(format!("{} timed out", engine_label(kind)));
                        return;
                    }
                    Err(e) => {
                        yield AgentEvent::Error(format!("process error: {e}"));
                        return;
                    }
                }
            }
            if !saw_terminal {
                yield AgentEvent::Done { finish_reason: "stop".to_string() };
            }
        })
    }
}

/// Runtime fingerprint for the resolved engine's home dir + the active sandbox backend.
pub fn runtime_fingerprint(engine_home: &Option<PathBuf>, sandbox_backend: &str) -> RuntimeFingerprint {
    RuntimeFingerprint {
        engine_home: engine_home.as_ref().map(|p| p.display().to_string()),
        sandbox_backend: sandbox_backend.to_string(),
    }
}

/// The system/developer prompt the request would run under (for the key + fresh sessions).
fn system_prompt_of(req: &ChatCompletionRequest) -> Option<String> {
    render_turn(&req.messages).system_prompt
}

/// Whether this turn's native session may be resumed/stored. Beyond the engine's `resume_by_id`,
/// codex `text` mode runs with `--ephemeral` (no persisted session), so storing its session id
/// would later resume against a session codex never kept — gate it off.
fn turn_resumable(entry: &ModelEntry) -> bool {
    caps_for(entry.engine).resume_by_id
        && !(entry.engine == EngineKind::Codex && entry.mode == Mode::Text)
}

/// Text of the final user message (the live instruction fed on a resume hit).
fn final_user_text(messages: &[ChatMessage]) -> String {
    messages
        .iter()
        .rev()
        .find(|m| m.role == Role::User)
        .map(|m| m.text())
        .unwrap_or_default()
}

/// Resolve the request against the session store and return a live event stream. On a HIT, resume
/// the stored claude session feeding only the new user turn; on a MISS, start fresh with the full
/// read-only transcript. As the stream runs it captures the session id and, on completion, stores
/// the advanced key so the next turn resolves to a hit.
pub fn run_request(
    runner: Arc<dyn TurnRunner>,
    sessions: Arc<SessionStore>,
    entry: &ModelEntry,
    req: &ChatCompletionRequest,
    rt: &RuntimeFingerprint,
) -> EventStream {
    let sys = system_prompt_of(req);
    let key = lookup_key(&req.messages, entry, sys.as_deref(), rt);
    let can_resume = turn_resumable(entry);
    let hit = if can_resume { sessions.get(&key) } else { None };

    let rendered = render_turn(&req.messages);
    let turn = match &hit {
        Some(sid) => Turn {
            system_prompt: None, // the resumed session already holds it
            user_prompt: final_user_text(&req.messages),
            model: entry.model.clone(),
            workspace: entry.workspace.clone(),
            mode: entry.mode,
            resume: Some(sid.clone()),
            engine: entry.engine,
            permissions: entry.permissions.clone(),
            mcp_config: None,
        },
        None => Turn {
            system_prompt: rendered.system_prompt.clone(),
            user_prompt: rendered.user_prompt.clone(),
            model: entry.model.clone(),
            workspace: entry.workspace.clone(),
            mode: entry.mode,
            resume: None,
            engine: entry.engine,
            permissions: entry.permissions.clone(),
            mcp_config: None,
        },
    };

    let inner = runner.run_stream(turn);
    // Owned copies for the post-stream store update.
    let messages = req.messages.clone();
    let entry = entry.clone();
    let sys_owned = sys.clone();
    let rt_owned = rt.clone();
    let resumed_sid = hit.clone();
    // `can_resume` is `Copy`, so the stream closure captures it by value directly.

    Box::pin(async_stream::stream! {
        futures::pin_mut!(inner);
        let mut session_id = resumed_sid;       // hit: keep the resumed id; miss: filled by init
        let mut assistant_text = String::new();
        while let Some(ev) = inner.next().await {
            if let AgentEvent::SessionId(s) = &ev {
                session_id = Some(s.clone());
            }
            if let AgentEvent::AssistantText(t) = &ev {
                assistant_text.push_str(t);
            }
            yield ev;
        }
        if can_resume {
            if let Some(sid) = session_id {
                let advanced = stored_key_after(&messages, &assistant_text, &entry, sys_owned.as_deref(), &rt_owned);
                sessions.insert(advanced, sid);
            }
        }
    })
}

/// Outcome of a `tools`-bearing claude turn run against a per-request MCP server.
pub enum ToolsTurn {
    /// The agent made tool calls; the suspension is registered. Send these to the client.
    ToolCalls(Vec<ToolCall>),
    /// The agent answered without calling a tool — stream this.
    Final(EventStream),
}

/// Failure modes of `run_tools_turn` (the HTTP layer maps these to 503 / 500).
pub enum ToolsTurnError {
    /// `max_suspended_sessions` reached — could not register the parked turn.
    Full,
    /// Spawn/startup failure (e.g. the MCP server failed to bind).
    Internal(String),
}

/// Settle window: after the FIRST tool call arrives, keep collecting parallel calls until this much
/// time passes with no new `PendingToolCall` (claude blocks inside each MCP call until we reply).
const TOOL_CALL_SETTLE: Duration = Duration::from_millis(150);

/// Run a `tools`-bearing claude turn against a fresh, per-request MCP server. While the agent is
/// thinking we hold an active-concurrency permit; the moment it calls a tool, claude BLOCKS inside
/// the MCP call (a `PendingToolCall` arrives). We collect the batch (settle window covers parallel
/// calls), register a suspension whose continuation OWNS the still-running claude child + the live
/// `McpServer`, DROP the permit (parked == idle), and return the OpenAI `tool_calls`. The client's
/// tool-result follow-up fires the parked calls (via `SuspendedSessions::deliver`), the continuation
/// re-acquires a permit and streams claude's remaining output to completion.
///
/// If the agent answers WITHOUT calling a tool, returns `Final` streaming claude's events: the
/// events parsed during collection are buffered and replayed on the `Final` stream (so claude's
/// actual answer + finish_reason are preserved). On the tool-call path the buffered pre-tool events
/// are discarded — the `tool_calls` response is just the calls (acceptable for v1).
///
/// LIMITATION: an external `sandbox_backend` is NOT yet supported. Wrapping the claude spawn in the
/// sandbox while keeping the temp `--mcp-config` reachable inside (e.g. bwrap) is future work; until
/// then this refuses (rather than silently bypassing the sandbox) for any backend other than `none`.
#[allow(clippy::too_many_arguments)]
pub async fn run_tools_turn(
    supervisor: ProcessSupervisor,
    suspended: Arc<SuspendedSessions>,
    entry: &ModelEntry,
    req: &ChatCompletionRequest,
    claude_config_dir: Option<PathBuf>,
    env_passthrough: Vec<String>,
    sandbox_backend: SandboxBackend,
    timeout: Duration,
    tool_result_timeout: Duration,
) -> Result<ToolsTurn, ToolsTurnError> {
    // (0) Refuse tools + external sandbox: `run_tools_turn` spawns claude UNMETERED without
    //     `sandbox::maybe_wrap`, so honoring a non-`none` backend here would be a silent bypass.
    //     Refuse before touching the permit / MCP server (future work: wrap + expose the mcp-config).
    if sandbox_backend != SandboxBackend::None {
        return Err(ToolsTurnError::Internal(
            "`tools` (MCP bridge) is not yet supported with an external sandbox_backend; use sandbox_backend: none".into(),
        ));
    }

    // (0b) Early Full guard: if the pool is already at capacity, refuse before spawning anything.
    if suspended.is_full() {
        return Err(ToolsTurnError::Full);
    }

    // (1) Hold the active slot for the whole collection phase.
    let permit = supervisor.acquire().await;

    // (2) Stand up a fresh per-request MCP server exposing the request's tool defs.
    let mut server =
        McpServer::start(req.tool_defs()).await.map_err(|e| ToolsTurnError::Internal(e.to_string()))?;

    // (3) Build a fresh claude turn (resume: None — resume-with-tools is out of scope) pointed at
    //     the MCP config so claude connects to our server (`--mcp-config http --strict-mcp-config`).
    let rendered = render_turn(&req.messages);
    let turn = Turn {
        system_prompt: rendered.system_prompt,
        user_prompt: rendered.user_prompt,
        model: entry.model.clone(),
        workspace: entry.workspace.clone(),
        mode: entry.mode,
        resume: None,
        engine: entry.engine,
        permissions: entry.permissions.clone(),
        mcp_config: Some(server.config_path().to_path_buf()),
    };
    let engine = Engine::Claude(ClaudeAdapter::new("claude", claude_config_dir));
    let (cmd, stdin) = engine.build_stream_command(&turn, &env_passthrough);

    // (4) Spawn UNDER OUR permit (unmetered: the variant does not take a permit itself). Box-pin so
    //     the stream is `Unpin` + movable: we poll it by reference during collection, then `move` it
    //     into the continuation/final stream.
    let mut lines: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<String>> + Send>> =
        Box::pin(supervisor.spawn_streaming_unmetered(cmd, stdin, timeout));

    // (5) Collect loop: race claude's stdout against incoming tool calls.
    let mut calls: Vec<ToolCall> = Vec::new();
    let mut pairs: Vec<(String, tokio::sync::oneshot::Sender<String>)> = Vec::new();
    let mut saw_final = false; // a terminal claude event seen before any tool call
    // Buffer EVERY event parsed during collection. On the no-tool `Final` path these are replayed so
    // claude's actual answer (`AssistantText`) + real `finish_reason` survive (Fix #1). On the
    // tool-call path they are discarded (the response is just the tool calls).
    let mut collected: Vec<AgentEvent> = Vec::new();

    loop {
        if pairs.is_empty() {
            // No tool call yet: wait for EITHER a claude line OR the first PendingToolCall.
            tokio::select! {
                pc = server.calls.recv() => match pc {
                    Some(pc) => push_call(&mut calls, &mut pairs, pc),
                    None => break, // server gone (shouldn't happen while we hold it)
                },
                line = lines.next() => match line {
                    Some(Ok(line)) => {
                        for ev in engine.parse_stream_line(&line) {
                            if matches!(ev, AgentEvent::Done { .. } | AgentEvent::Error(_)) {
                                saw_final = true;
                            }
                            collected.push(ev); // buffer the terminal event too, before breaking
                        }
                        if saw_final { break; }
                    }
                    // Stream error during collection: buffer it as a terminal Error (so it's
                    // surfaced, not swallowed), then stop. `saw_final = true` because a terminal
                    // event IS buffered — the Final stream won't add a duplicate Done.
                    Some(Err(e)) if e.kind() == std::io::ErrorKind::TimedOut => {
                        collected.push(AgentEvent::Error("claude timed out".to_string()));
                        saw_final = true;
                        break;
                    }
                    Some(Err(e)) => {
                        collected.push(AgentEvent::Error(format!("process error: {e}")));
                        saw_final = true;
                        break;
                    }
                    // Clean EOF with no tool calls: leave `saw_final = false` so the Final stream
                    // synthesizes the terminal `Done` (matches EngineProcessRunner's EOF handling).
                    None => break,
                },
            }
        } else {
            // We have at least one tool call; collect any parallel calls until the settle window
            // elapses with no new PendingToolCall. (claude is blocked inside the MCP call(s).)
            match tokio::time::timeout(TOOL_CALL_SETTLE, server.calls.recv()).await {
                Ok(Some(pc)) => push_call(&mut calls, &mut pairs, pc),
                Ok(None) => break,   // server gone
                Err(_) => break,     // settle window elapsed: the batch is complete
            }
        }
    }

    // (6a) No tool calls: the agent answered. Build a `Final` stream that REPLAYS the buffered
    //      `collected` events (claude's actual answer + its terminal) then drains any remaining
    //      lines (usually none after the terminal). The collection `permit` MOVES into the stream so
    //      the Final drain stays metered — matching the ordinary turn path (Fix #1 + Fix #3).
    if pairs.is_empty() {
        let final_stream: EventStream = Box::pin(async_stream::stream! {
            let _permit = permit;  // hold the active slot until the Final stream ends (metered)
            let _server = server;  // keep the MCP server alive while draining
            let mut saw_terminal = saw_final;
            for ev in collected {
                yield ev; // replay claude's pre-terminal answer + the terminal event we buffered
            }
            while let Some(item) = lines.next().await {
                match item {
                    Ok(line) => {
                        for ev in engine.parse_stream_line(&line) {
                            if matches!(ev, AgentEvent::Done { .. } | AgentEvent::Error(_)) {
                                saw_terminal = true;
                            }
                            yield ev;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                        yield AgentEvent::Error("claude timed out".to_string());
                        return;
                    }
                    Err(e) => {
                        yield AgentEvent::Error(format!("process error: {e}"));
                        return;
                    }
                }
            }
            // Append a synthetic terminal only if neither a `Done` nor an `Error` was emitted.
            if !saw_terminal {
                yield AgentEvent::Done { finish_reason: "stop".to_string() };
            }
        });
        return Ok(ToolsTurn::Final(final_stream));
    }

    // (6b) Non-empty batch: claude is parked inside the MCP call(s). Build the continuation that
    //      OWNS the live child-stream + the MCP server and, on resume, re-acquires a permit and
    //      drains the rest of claude's output.
    let supervisor_for_cont = supervisor.clone();
    let continuation: EventStream = Box::pin(async_stream::stream! {
        // Re-acquire the active slot on resume (the parked turn released it while idle).
        let _permit = supervisor_for_cont.acquire().await;
        // Own the MCP server (claude may call more tools) and the remaining child-stream.
        let mut server = server;
        let mut lines = lines;
        let mut saw_terminal = false;
        // Once `server.calls.recv()` yields `None` (claude closed its MCP session before stdout EOF)
        // the receiver is permanently ready, so keep selecting on it would hot-spin. Fuse it: stop
        // selecting that arm once closed and just drain `lines` to the terminal event (Fix #4).
        let mut calls_open = true;
        loop {
            tokio::select! {
                // MULTI-ROUND (v1 limitation): a second batch of tool calls during the continuation
                // is NOT implemented. We reply to any such call with an error so claude does not
                // block forever, log it, and let it run to its terminal event.
                pc = server.calls.recv(), if calls_open => {
                    match pc {
                        Some(pc) => {
                            tracing::warn!(
                                tool = %pc.name,
                                "multi-round tool call during continuation is unsupported (v1); returning error to claude"
                            );
                            let _ = pc.reply.send(
                                "error: multi-round tool calls are not supported in this bridge version".to_string()
                            );
                        }
                        None => calls_open = false, // MCP session closed: stop selecting this arm
                    }
                }
                item = lines.next() => match item {
                    Some(Ok(line)) => {
                        for ev in engine.parse_stream_line(&line) {
                            if matches!(ev, AgentEvent::Done { .. } | AgentEvent::Error(_)) {
                                saw_terminal = true;
                            }
                            yield ev;
                        }
                    }
                    Some(Err(e)) if e.kind() == std::io::ErrorKind::TimedOut => {
                        yield AgentEvent::Error("claude timed out".to_string());
                        return;
                    }
                    Some(Err(e)) => {
                        yield AgentEvent::Error(format!("process error: {e}"));
                        return;
                    }
                    None => break, // clean EOF
                },
            }
        }
        if !saw_terminal {
            yield AgentEvent::Done { finish_reason: "stop".to_string() };
        }
        // NOTE: no session is stored after a tools continuation — resume-with-tools is out of
        // scope for v1, so a resumed tools turn always takes the full-transcript miss path.
        drop(server); // tear down the MCP server + temp config once claude is done
    });

    let group = suspended.register(pairs, continuation).map_err(|_| ToolsTurnError::Full)?;
    suspended.spawn_reaper(group, tool_result_timeout);
    // Release the collection slot: a parked suspension is idle (the continuation re-acquires).
    drop(permit);
    Ok(ToolsTurn::ToolCalls(calls))
}

/// Push a `PendingToolCall` into the OpenAI `calls` vec (the response) and the `pairs` vec (the
/// suspension's result-delivery senders). The `PendingToolCall` id is authoritative (its `call_…`
/// id, NOT claude's internal `toolu_…`).
fn push_call(
    calls: &mut Vec<ToolCall>,
    pairs: &mut Vec<(String, tokio::sync::oneshot::Sender<String>)>,
    pc: crate::mcp::PendingToolCall,
) {
    calls.push(ToolCall {
        id: pc.id.clone(),
        kind: "function".into(),
        function: FunctionCall { name: pc.name, arguments: pc.args },
    });
    pairs.push((pc.id, pc.reply));
}

/// Aggregate a finished event stream into one OpenAI chat.completion. Only `AssistantText` goes
/// into `content`; reasoning/tool progress is dropped in the non-streaming representation.
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

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineKind, Mode, ModelEntry};
    use crate::engine::AgentEvent;
    use crate::openai::{ChatCompletionRequest, ChatMessage, MessageContent, Role};
    use crate::session::{RuntimeFingerprint, SessionStore};
    use futures::StreamExt;
    use std::sync::Arc;

    fn entry() -> ModelEntry {
        ModelEntry { id: "m".into(), engine: EngineKind::Claude, model: Some("opus".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false }
    }
    fn rt() -> RuntimeFingerprint {
        RuntimeFingerprint { engine_home: None, sandbox_backend: "none".into() }
    }
    fn umsg(t: &str) -> ChatMessage {
        ChatMessage { role: Role::User, content: Some(MessageContent::Text(t.into())), tool_call_id: None, tool_calls: None }
    }
    fn req(msgs: Vec<ChatMessage>) -> ChatCompletionRequest {
        ChatCompletionRequest { model: "m".into(), messages: msgs, stream: Some(false), tools: None }
    }

    /// Fake runner: records the Turn it was given, returns canned events incl. a SessionId.
    struct FakeRunner { events: Vec<AgentEvent>, seen: Arc<std::sync::Mutex<Option<Turn>>> }
    impl TurnRunner for FakeRunner {
        fn run_stream(&self, turn: Turn) -> crate::engine::EventStream {
            *self.seen.lock().unwrap() = Some(turn);
            Box::pin(futures::stream::iter(self.events.clone()))
        }
    }

    fn aggregate(events: Vec<AgentEvent>) -> crate::openai::ChatCompletionResponse {
        response_from_events(events, "m").unwrap()
    }

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

    #[test]
    fn response_from_events_only_text_into_content() {
        let resp = aggregate(vec![
            AgentEvent::SessionId("s".into()),
            AgentEvent::Reasoning("thinking".into()),
            AgentEvent::ToolStart { name: "Edit".into(), args: "{}".into() },
            AgentEvent::AssistantText("pong".into()),
            AgentEvent::Done { finish_reason: "stop".into() },
        ]);
        assert_eq!(resp.choices[0].message.content, "pong"); // reasoning/tool dropped from content
        assert_eq!(resp.choices[0].finish_reason, "stop");
    }

    #[tokio::test]
    async fn miss_uses_full_transcript_fresh_then_stores_advanced_key() {
        let store = Arc::new(SessionStore::new());
        let seen = Arc::new(std::sync::Mutex::new(None));
        let runner: Arc<dyn TurnRunner> = Arc::new(FakeRunner {
            events: vec![AgentEvent::SessionId("sess-X".into()), AgentEvent::AssistantText("answer one".into()), AgentEvent::Done { finish_reason: "stop".into() }],
            seen: seen.clone(),
        });
        let messages = vec![umsg("first")];
        let stream = run_request(runner, store.clone(), &entry(), &req(messages.clone()), &rt());
        let collected: Vec<_> = stream.collect().await;
        assert!(collected.iter().any(|e| matches!(e, AgentEvent::AssistantText(t) if t == "answer one")));
        // miss => fresh (no resume), full transcript fed
        let turn = seen.lock().unwrap().clone().unwrap();
        assert!(turn.resume.is_none());
        assert!(turn.user_prompt.contains("first"));
        // index advanced: the next turn (history + assistant) must now hit with session id sess-X
        let m2 = vec![umsg("first"), ChatMessage { role: Role::Assistant, content: Some(MessageContent::Text("answer one".into())), tool_call_id: None, tool_calls: None }, umsg("second")];
        let k = crate::session::lookup_key(&m2, &entry(), None, &rt());
        assert_eq!(store.get(&k), Some("sess-X".to_string()));
    }

    #[tokio::test]
    async fn hit_resumes_with_last_turn_only() {
        let store = Arc::new(SessionStore::new());
        let seen = Arc::new(std::sync::Mutex::new(None));
        // Pre-seed the index so the second turn is a hit.
        let m_prev = [umsg("first"), ChatMessage { role: Role::Assistant, content: Some(MessageContent::Text("answer one".into())), tool_call_id: None, tool_calls: None }];
        let k = crate::session::lookup_key(&[umsg("first"), m_prev[1].clone(), umsg("second")], &entry(), None, &rt());
        store.insert(k, "sess-prev".into());
        let runner: Arc<dyn TurnRunner> = Arc::new(FakeRunner {
            events: vec![AgentEvent::AssistantText("answer two".into()), AgentEvent::Done { finish_reason: "stop".into() }],
            seen: seen.clone(),
        });
        let messages = vec![m_prev[0].clone(), m_prev[1].clone(), umsg("second")];
        let _ = run_request(runner, store, &entry(), &req(messages), &rt()).collect::<Vec<_>>().await;
        let turn = seen.lock().unwrap().clone().unwrap();
        assert_eq!(turn.resume.as_deref(), Some("sess-prev"));
        assert_eq!(turn.user_prompt, "second");          // only the new user turn
        assert!(turn.system_prompt.is_none());            // session already holds it
    }

    #[tokio::test]
    async fn agy_never_resumes_or_stores() {
        let store = Arc::new(SessionStore::new());
        let seen = Arc::new(std::sync::Mutex::new(None));
        let agy = ModelEntry { id: "m".into(), engine: EngineKind::Agy, model: None,
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false };
        let runner: Arc<dyn TurnRunner> = Arc::new(FakeRunner {
            events: vec![AgentEvent::SessionId("ignored".into()), AgentEvent::AssistantText("a".into()), AgentEvent::Done { finish_reason: "stop".into() }],
            seen: seen.clone(),
        });
        let messages = vec![umsg("first")];
        let _ = run_request(runner, store.clone(), &agy, &req(messages), &rt()).collect::<Vec<_>>().await;
        assert!(seen.lock().unwrap().clone().unwrap().resume.is_none());
        // Even after a SessionId event, a non-resume engine stores nothing.
        let m2 = vec![umsg("first"), ChatMessage { role: Role::Assistant, content: Some(MessageContent::Text("a".into())), tool_call_id: None, tool_calls: None }, umsg("second")];
        assert_eq!(store.get(&crate::session::lookup_key(&m2, &agy, None, &rt())), None);
    }

    #[tokio::test]
    async fn codex_text_mode_never_resumes_or_stores() {
        // codex `text` runs --ephemeral (no persisted session), so resume must be gated off even
        // though caps_for(Codex).resume_by_id == true — otherwise a later turn would resume a
        // session codex never kept.
        let store = Arc::new(SessionStore::new());
        let seen = Arc::new(std::sync::Mutex::new(None));
        let codex_text = ModelEntry { id: "m".into(), engine: EngineKind::Codex, model: None,
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false };
        let runner: Arc<dyn TurnRunner> = Arc::new(FakeRunner {
            events: vec![AgentEvent::SessionId("eph".into()), AgentEvent::AssistantText("a".into()), AgentEvent::Done { finish_reason: "stop".into() }],
            seen: seen.clone(),
        });
        let messages = vec![umsg("first")];
        let _ = run_request(runner, store.clone(), &codex_text, &req(messages), &rt()).collect::<Vec<_>>().await;
        assert!(seen.lock().unwrap().clone().unwrap().resume.is_none());
        let m2 = vec![umsg("first"), ChatMessage { role: Role::Assistant, content: Some(MessageContent::Text("a".into())), tool_call_id: None, tool_calls: None }, umsg("second")];
        assert_eq!(store.get(&crate::session::lookup_key(&m2, &codex_text, None, &rt())), None);
    }

    /// Fix #2: a tools-turn with a non-`none` sandbox_backend must REFUSE up front (before spawning
    /// anything) rather than silently bypassing the sandbox. No real claude is reached — the early
    /// guard returns `Internal` before the permit is acquired or the MCP server is started.
    #[tokio::test]
    async fn tools_turn_refuses_external_sandbox_backend() {
        use crate::process::ProcessSupervisor;
        use crate::suspend::SuspendedSessions;

        let supervisor = ProcessSupervisor::new(1);
        let suspended = Arc::new(SuspendedSessions::new(8));
        let entry = entry();
        let request = req(vec![umsg("hi")]); // empty tools; we never reach tool parsing anyway

        let out = run_tools_turn(
            supervisor.clone(),
            suspended,
            &entry,
            &request,
            None,
            Vec::new(),
            crate::config::SandboxBackend::Bubblewrap,
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(5),
        )
        .await;

        assert!(matches!(out, Err(ToolsTurnError::Internal(_))));
        // Refused before touching the permit pool: the active slot is untouched.
        assert_eq!(supervisor.available(), 1);
    }

    /// Hermetic round-trip of the sub-mechanisms `run_tools_turn` composes (NO real claude): an
    /// `McpBridge` driven by an in-memory rmcp client mints a `PendingToolCall` and PARKS inside
    /// `call_tool`; we take its `reply` sender as a suspension pair (exactly as `run_tools_turn`
    /// does), register a continuation that streams a final answer, then `deliver` the client's
    /// tool result. Asserts: the parked `call_tool` returns the delivered text AND the continuation
    /// streams — proving collect(reply) → register → deliver → resume works end to end.
    #[tokio::test]
    async fn suspend_bridge_roundtrip_parks_then_delivers_and_resumes() {
        use crate::mcp::{McpBridge, PendingToolCall};
        use crate::openai::{FunctionDef, ToolDef};
        use crate::suspend::SuspendedSessions;
        use rmcp::model::CallToolRequestParams;
        use rmcp::{serve_client, serve_server};
        use tokio::sync::mpsc;

        let def = ToolDef {
            kind: "function".into(),
            function: FunctionDef {
                name: "search".into(),
                description: Some("d".into()),
                parameters: serde_json::json!({"type":"object","properties":{"q":{"type":"string"}}}),
            },
        };
        let (calls_tx, mut calls_rx) = mpsc::unbounded_channel::<PendingToolCall>();
        let bridge = McpBridge::new(vec![def], calls_tx);

        // Wire an in-memory rmcp client <-> the bridge server.
        let (s_io, c_io) = tokio::io::duplex(8 * 1024);
        let server_task = tokio::spawn(async move { serve_server(bridge, s_io).await });
        let client = serve_client((), c_io).await.unwrap();
        let mcp_server = server_task.await.unwrap().unwrap();

        let suspended = Arc::new(SuspendedSessions::new(8));

        // Collector: receive the PendingToolCall, register it (id, reply) + a scripted continuation.
        let suspended_c = suspended.clone();
        let collector = tokio::spawn(async move {
            let pc = calls_rx.recv().await.unwrap();
            assert!(pc.id.starts_with("call_"));
            assert_eq!(pc.name, "search");
            let continuation: EventStream = Box::pin(futures::stream::iter(vec![
                AgentEvent::AssistantText("the answer".into()),
                AgentEvent::Done { finish_reason: "stop".into() },
            ]));
            let group = suspended_c.register(vec![(pc.id.clone(), pc.reply)], continuation).unwrap();
            (pc.id, group)
        });

        // The agent calls the tool — this PARKS in call_tool until we deliver.
        let params = CallToolRequestParams::new("search")
            .with_arguments(serde_json::json!({"q":"rust"}).as_object().unwrap().clone());
        let call_fut = tokio::spawn(async move { client.call_tool(params).await });

        let (call_id, _group) = collector.await.unwrap();
        assert_eq!(suspended.live_count(), 1);

        // Deliver the client's tool result: fires the parked sender + hands back the continuation.
        let continuation = suspended
            .deliver(&[(call_id.clone(), "TOOL-RESULT".into())])
            .unwrap();
        assert_eq!(suspended.live_count(), 0);

        // The parked call_tool now returns the delivered text.
        let result = call_fut.await.unwrap().unwrap();
        let text = result.content.iter().find_map(|c| c.as_text().map(|t| t.text.clone())).unwrap();
        assert_eq!(text, "TOOL-RESULT");

        // The continuation streams the final answer.
        let evs: Vec<_> = continuation.collect().await;
        assert!(evs.iter().any(|e| matches!(e, AgentEvent::AssistantText(t) if t == "the answer")));
        assert!(evs.iter().any(|e| matches!(e, AgentEvent::Done { .. })));

        mcp_server.cancel().await.unwrap();
    }
}
