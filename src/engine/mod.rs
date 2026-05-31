//! Engine abstraction. Production uses an enum (no `dyn`); each variant owns one CLI's quirks.
//! Implements Claude via `--output-format stream-json` (events parsed line-by-line); the
//! non-streaming endpoint aggregates the same event stream.
pub mod agy;
pub mod claude;
pub mod codex;

use crate::config::{EngineKind, Mode};
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
    /// `Some(session_id)` to resume an existing session; `None` for a fresh session.
    pub resume: Option<String>,
    /// Which engine this turn runs on (drives per-engine dispatch in the runner).
    pub engine: EngineKind,
    /// The model's `permissions` string (drives codex `-s` sandbox level); `None` ⇒ engine default.
    pub permissions: Option<String>,
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

/// Per-engine capabilities, independent of any adapter instance (so the runner/orchestrator can
/// query caps without constructing an adapter). agy is non-streaming and its resume is gated off
/// until the §9 spike proves session-id capture + credential isolation.
pub fn caps_for(engine: EngineKind) -> Caps {
    match engine {
        EngineKind::Claude => Caps { streaming: true, resume_by_id: true, mcp_tools_phase1: false },
        EngineKind::Codex => Caps { streaming: true, resume_by_id: true, mcp_tools_phase1: false },
        EngineKind::Agy => Caps { streaming: false, resume_by_id: false, mcp_tools_phase1: false },
    }
}

/// Production dispatch enum (no `dyn`); each variant owns one CLI's quirks.
pub enum Engine {
    Claude(claude::ClaudeAdapter),
    Codex(codex::CodexAdapter),
    Agy(agy::AgyAdapter),
}

impl Engine {
    pub fn kind(&self) -> EngineKind {
        match self {
            Engine::Claude(_) => EngineKind::Claude,
            Engine::Codex(_) => EngineKind::Codex,
            Engine::Agy(_) => EngineKind::Agy,
        }
    }

    pub fn caps(&self) -> Caps {
        caps_for(self.kind())
    }

    /// Build the streaming command + stdin payload (claude feeds the prompt on stdin; codex/agy
    /// pass it as the trailing positional argument and use no stdin).
    pub fn build_stream_command(&self, turn: &Turn, env_passthrough: &[String]) -> (tokio::process::Command, Option<String>) {
        match self {
            Engine::Claude(a) => a.build_stream_command(turn, env_passthrough),
            Engine::Codex(a) => a.build_stream_command(turn, env_passthrough),
            Engine::Agy(a) => a.build_stream_command(turn, env_passthrough),
        }
    }

    /// Parse ONE line of streaming output into zero or more events.
    pub fn parse_stream_line(&self, line: &str) -> Vec<AgentEvent> {
        match self {
            Engine::Claude(a) => a.parse_stream_line(line),
            Engine::Codex(a) => a.parse_stream_line(line),
            Engine::Agy(a) => a.parse_stream_line(line),
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
            engine: crate::config::EngineKind::Claude, permissions: None,
        };
        assert_eq!(t.resume.as_deref(), Some("sid"));
    }

    #[test]
    fn caps_per_engine() {
        use crate::config::EngineKind;
        let claude = caps_for(EngineKind::Claude);
        assert!(claude.streaming && claude.resume_by_id && !claude.mcp_tools_phase1);
        let codex = caps_for(EngineKind::Codex);
        assert!(codex.streaming && codex.resume_by_id && !codex.mcp_tools_phase1);
        let agy = caps_for(EngineKind::Agy);
        assert!(!agy.streaming && !agy.resume_by_id && !agy.mcp_tools_phase1); // non-streaming, resume gated off
    }

    #[test]
    fn engine_kind_reports_variant() {
        use crate::config::EngineKind;
        let e = Engine::Codex(crate::engine::codex::CodexAdapter::new("codex", None));
        assert_eq!(e.kind(), EngineKind::Codex);
        assert!(e.caps().streaming);
    }

    #[test]
    fn turn_carries_engine() {
        use crate::config::EngineKind;
        let t = Turn {
            system_prompt: None, user_prompt: "x".into(), model: None, workspace: None,
            mode: crate::config::Mode::Text, resume: None, engine: EngineKind::Agy, permissions: None,
        };
        assert_eq!(t.engine, EngineKind::Agy);
    }
}
