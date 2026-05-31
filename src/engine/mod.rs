//! Engine abstraction. Production uses an enum (no `dyn`); each variant owns one CLI's quirks.
//! Implements Claude via `--output-format stream-json` (events parsed line-by-line); the
//! non-streaming endpoint aggregates the same event stream.
pub mod claude;

use crate::config::Mode;
use futures::Stream;
use std::path::PathBuf;
use std::pin::Pin;
use thiserror::Error;

/// A boxed, owned stream of normalized engine events.
pub type EventStream = Pin<Box<dyn Stream<Item = AgentEvent> + Send>>;

/// A normalized unit of work handed to an adapter to build its CLI invocation.
#[derive(Debug, Clone)]
pub struct Turn {
    pub system_prompt: Option<String>,
    /// The user-facing prompt to feed (a single new turn on resume, or the full transcript on a miss).
    pub user_prompt: String,
    pub model: Option<String>,
    pub workspace: Option<PathBuf>,
    pub mode: Mode,
    /// `Some(session_id)` to resume an existing claude session; `None` for a fresh session.
    pub resume: Option<String>,
}

/// Normalized engine output events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentEvent {
    AssistantText(String),
    /// Model reasoning ("thinking") — routed to the progress channel, never to `content`.
    Reasoning(String),
    /// A tool the agent started (agentic mode) — progress channel.
    ToolStart { name: String, args: String },
    /// A tool result the agent received — progress channel.
    ToolResult { summary: String },
    SessionId(String),
    Error(String),
    Done { finish_reason: String },
}

#[derive(Debug, Clone, Copy)]
pub struct Caps {
    pub streaming: bool,
    pub resume_by_id: bool,
    /// Per-invocation MCP tool injection safe? Always false in Phase 1 (bridge = Phase 4).
    pub mcp_tools_phase1: bool,
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("engine reported an error: {0}")]
    Reported(String),
    #[error("failed to parse engine output: {0}")]
    Parse(String),
}

/// Production dispatch enum. (Codex/Agy variants arrive in Phase 3.)
pub enum Engine {
    Claude(claude::ClaudeAdapter),
}

impl Engine {
    pub fn caps(&self) -> Caps {
        match self {
            Engine::Claude(_) => Caps { streaming: true, resume_by_id: true, mcp_tools_phase1: false },
        }
    }

    /// Build the streaming (stream-json) command + stdin payload.
    pub fn build_stream_command(&self, turn: &Turn, env_passthrough: &[String]) -> (tokio::process::Command, Option<String>) {
        match self {
            Engine::Claude(a) => a.build_stream_command(turn, env_passthrough),
        }
    }

    /// Parse ONE line of streaming output into zero or more events.
    pub fn parse_stream_line(&self, line: &str) -> Vec<AgentEvent> {
        match self {
            Engine::Claude(a) => a.parse_stream_line(line),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_caps() {
        let e = Engine::Claude(crate::engine::claude::ClaudeAdapter::new("claude", None));
        let c = e.caps();
        assert!(c.streaming);
        assert!(c.resume_by_id);
        assert!(!c.mcp_tools_phase1);
    }

    #[test]
    fn agent_event_variants_construct() {
        let _ = AgentEvent::AssistantText("hi".into());
        let _ = AgentEvent::Reasoning("thinking".into());
        let _ = AgentEvent::ToolStart { name: "Edit".into(), args: "{}".into() };
        let _ = AgentEvent::ToolResult { summary: "ok".into() };
        let _ = AgentEvent::SessionId("sid".into());
        let _ = AgentEvent::Error("boom".into());
        let _ = AgentEvent::Done { finish_reason: "stop".into() };
    }

    #[test]
    fn turn_has_resume_field() {
        let t = Turn {
            system_prompt: None, user_prompt: "x".into(), model: None,
            workspace: None, mode: crate::config::Mode::Text, resume: Some("sid".into()),
        };
        assert_eq!(t.resume.as_deref(), Some("sid"));
    }
}
