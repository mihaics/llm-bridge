//! Turn orchestration: the streaming `TurnRunner`, the production `EngineProcessRunner`, the
//! session-aware `run_request` (key resolution, resume vs fresh, capture + index advancement),
//! and the events->response aggregation used by the non-streaming path.
use crate::config::{Credentials, EngineKind, ModelEntry};
use crate::engine::agy::AgyAdapter;
use crate::engine::claude::ClaudeAdapter;
use crate::engine::codex::CodexAdapter;
use crate::engine::{caps_for, AgentEvent, Engine, EngineError, EventStream, Turn};
use crate::openai::{
    ChatCompletionRequest, ChatCompletionResponse, ChatMessage, Choice, ResponseMessage, Role, Usage,
};
use crate::process::ProcessSupervisor;
use crate::session::{lookup_key, stored_key_after, RuntimeFingerprint, SessionStore};
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
    let can_resume = caps_for(entry.engine).resume_by_id;
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

/// Aggregate a finished event stream into one OpenAI chat.completion. Only `AssistantText` goes
/// into `content`; reasoning/tool progress is dropped in the non-streaming representation.
pub fn response_from_events(
    events: Vec<AgentEvent>,
    model_id: &str,
) -> Result<ChatCompletionResponse, EngineError> {
    let mut content = String::new();
    let mut finish_reason = "stop".to_string();
    for ev in events {
        match ev {
            AgentEvent::AssistantText(t) => content.push_str(&t),
            AgentEvent::Done { finish_reason: fr } => finish_reason = fr,
            AgentEvent::Error(m) => return Err(EngineError::Reported(m)),
            AgentEvent::Reasoning(_)
            | AgentEvent::ToolStart { .. }
            | AgentEvent::ToolResult { .. }
            | AgentEvent::SessionId(_) => {}
        }
    }
    Ok(ChatCompletionResponse {
        id: format!("chatcmpl-{}", unix_now()),
        object: "chat.completion",
        created: unix_now(),
        model: model_id.to_string(),
        choices: vec![Choice {
            index: 0,
            message: ResponseMessage { role: "assistant", content },
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
        ChatMessage { role: Role::User, content: Some(MessageContent::Text(t.into())), tool_call_id: None }
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
        let m2 = vec![umsg("first"), ChatMessage { role: Role::Assistant, content: Some(MessageContent::Text("answer one".into())), tool_call_id: None }, umsg("second")];
        let k = crate::session::lookup_key(&m2, &entry(), None, &rt());
        assert_eq!(store.get(&k), Some("sess-X".to_string()));
    }

    #[tokio::test]
    async fn hit_resumes_with_last_turn_only() {
        let store = Arc::new(SessionStore::new());
        let seen = Arc::new(std::sync::Mutex::new(None));
        // Pre-seed the index so the second turn is a hit.
        let m_prev = [umsg("first"), ChatMessage { role: Role::Assistant, content: Some(MessageContent::Text("answer one".into())), tool_call_id: None }];
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
        let m2 = vec![umsg("first"), ChatMessage { role: Role::Assistant, content: Some(MessageContent::Text("a".into())), tool_call_id: None }, umsg("second")];
        assert_eq!(store.get(&crate::session::lookup_key(&m2, &agy, None, &rt())), None);
    }
}
