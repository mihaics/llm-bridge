//! Engine abstraction. Production uses an enum (no `dyn`); each variant owns one CLI's quirks.
//! Phase 1 implements only Claude and only non-streaming (single JSON object) parsing.
pub mod claude;

use crate::config::Mode;
use std::path::PathBuf;
use thiserror::Error;

/// A normalized unit of work handed to an adapter to build its CLI invocation.
#[derive(Debug, Clone)]
pub struct Turn {
    pub system_prompt: Option<String>,
    /// The user-facing prompt to feed (Phase 1: full read-only transcript replay).
    pub user_prompt: String,
    pub model: Option<String>,
    pub workspace: Option<PathBuf>,
    pub mode: Mode,
}

/// Normalized engine output events (Phase 1 subset; ToolStart/ToolResult/ToolCall in Phases 2/4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentEvent {
    AssistantText(String),
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

    /// Build the spawn command + optional stdin payload for this turn. `env_passthrough` is the
    /// allowlist of env vars to keep after scrubbing the rest.
    pub fn build_command(&self, turn: &Turn, env_passthrough: &[String]) -> (tokio::process::Command, Option<String>) {
        match self {
            Engine::Claude(a) => a.build_command(turn, env_passthrough),
        }
    }

    pub fn parse_output(&self, stdout: &str) -> Result<Vec<AgentEvent>, EngineError> {
        match self {
            Engine::Claude(a) => a.parse_output(stdout),
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
        assert!(c.streaming);          // claude supports stream-json (used in Phase 2)
        assert!(c.resume_by_id);
        assert!(!c.mcp_tools_phase1);  // MCP bridge is Phase 4
    }

    #[test]
    fn agent_event_variants_construct() {
        let _ = AgentEvent::AssistantText("hi".into());
        let _ = AgentEvent::SessionId("sid".into());
        let _ = AgentEvent::Error("boom".into());
        let _ = AgentEvent::Done { finish_reason: "stop".into() };
    }
}
