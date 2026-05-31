//! Turn orchestration: the injectable `TurnRunner`, the production `ClaudeProcessRunner`, and the
//! pure request->Turn and events->response mappings. The HTTP handler (http.rs) calls these.
use crate::config::ModelEntry;
use crate::engine::claude::ClaudeAdapter;
use crate::engine::{AgentEvent, Engine, EngineError, Turn};
use crate::openai::{
    ChatCompletionRequest, ChatCompletionResponse, Choice, ResponseMessage, Usage,
};
use crate::process::{ProcessError, ProcessSupervisor};
use crate::transcript::render_turn;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("timed out")]
    Timeout,
    #[error("spawn failed: {0}")]
    Spawn(String),
    #[error("engine error: {0}")]
    Engine(String),
}

pub type RunFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<AgentEvent>, RunError>> + Send + 'a>>;

/// Injectable: runs a Turn and returns normalized events. Production spawns claude; tests fake it.
pub trait TurnRunner: Send + Sync {
    fn run<'a>(&'a self, entry: &'a ModelEntry, turn: Turn) -> RunFuture<'a>;
}

/// Production runner: builds the claude command, runs it through the shared supervisor, parses.
pub struct ClaudeProcessRunner {
    pub supervisor: ProcessSupervisor,
    pub claude_config_dir: Option<PathBuf>,
    pub env_passthrough: Vec<String>,
    pub timeout: Duration,
}

impl TurnRunner for ClaudeProcessRunner {
    fn run<'a>(&'a self, _entry: &'a ModelEntry, turn: Turn) -> RunFuture<'a> {
        Box::pin(async move {
            let engine = Engine::Claude(ClaudeAdapter::new("claude", self.claude_config_dir.clone()));
            let (cmd, stdin) = engine.build_command(&turn, &self.env_passthrough);
            let output = self
                .supervisor
                .run(cmd, stdin, self.timeout)
                .await
                .map_err(|e| match e {
                    ProcessError::Timeout => RunError::Timeout,
                    ProcessError::Io(io) => RunError::Spawn(io.to_string()),
                })?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(RunError::Engine(format!(
                    "claude exited {}: {}",
                    output.status,
                    stderr.trim()
                )));
            }
            let stdout = String::from_utf8_lossy(&output.stdout);
            engine.parse_output(&stdout).map_err(|e| RunError::Engine(e.to_string()))
        })
    }
}

/// Map an incoming request + resolved entry into an engine Turn (fresh-session replay).
pub fn turn_from_request(req: &ChatCompletionRequest, entry: &ModelEntry) -> Turn {
    let rendered = render_turn(&req.messages);
    Turn {
        system_prompt: rendered.system_prompt,
        user_prompt: rendered.user_prompt,
        model: entry.model.clone(),
        workspace: entry.workspace.clone(),
        mode: entry.mode,
        resume: None,
    }
}

/// Assemble normalized events into an OpenAI chat.completion (or an error if the engine failed).
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
        usage: Usage::default(), // token accounting is Phase 2+
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

    fn entry() -> ModelEntry {
        ModelEntry { id: "m".into(), engine: EngineKind::Claude, model: Some("opus".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false }
    }

    fn req() -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: "m".into(),
            messages: vec![
                ChatMessage { role: Role::System, content: Some(MessageContent::Text("sys".into())), tool_call_id: None },
                ChatMessage { role: Role::User, content: Some(MessageContent::Text("hello".into())), tool_call_id: None },
            ],
            stream: Some(false),
            tools: None,
        }
    }

    #[test]
    fn turn_from_request_maps_system_and_prompt() {
        let t = turn_from_request(&req(), &entry());
        assert_eq!(t.system_prompt.as_deref(), Some("sys"));
        assert_eq!(t.user_prompt, "hello");
        assert_eq!(t.model.as_deref(), Some("opus"));
        assert_eq!(t.mode, Mode::Text);
    }

    #[test]
    fn response_from_events_concatenates_text_and_sets_finish() {
        let events = vec![
            AgentEvent::SessionId("s".into()),
            AgentEvent::AssistantText("pong".into()),
            AgentEvent::Done { finish_reason: "stop".into() },
        ];
        let resp = response_from_events(events, "m").unwrap();
        assert_eq!(resp.choices[0].message.content, "pong");
        assert_eq!(resp.choices[0].finish_reason, "stop");
        assert_eq!(resp.model, "m");
        assert_eq!(resp.object, "chat.completion");
    }

    #[test]
    fn response_from_events_error_becomes_err() {
        let events = vec![AgentEvent::Error("boom".into())];
        assert!(response_from_events(events, "m").is_err());
    }
}
