//! ClaudeAdapter: drive `claude -p --output-format json` (non-streaming, Phase 1) with a scrubbed
//! environment so model-run tools can't read service secrets.
use super::{AgentEvent, EngineError, Turn};
use crate::config::Mode;
use std::path::PathBuf;
use tokio::process::Command;

/// Tools blocked in text mode so the model is a pure generator (matches the PoC).
const BLOCKED_TOOLS: &[&str] = &[
    "Bash", "Read", "Edit", "Write", "Glob", "Grep", "WebFetch", "WebSearch", "NotebookEdit", "Task",
];

pub struct ClaudeAdapter {
    pub bin: String,
    pub config_dir: Option<PathBuf>,
}

impl ClaudeAdapter {
    pub fn new(bin: &str, config_dir: Option<PathBuf>) -> Self {
        ClaudeAdapter { bin: bin.into(), config_dir }
    }

    /// Build the spawn command and the stdin payload (the user prompt). The prompt goes via stdin
    /// (not a positional arg) because `--disallowed-tools` is variadic and would swallow it.
    pub fn build_command(&self, turn: &Turn, env_passthrough: &[String]) -> (Command, Option<String>) {
        let mut cmd = Command::new(&self.bin);

        // Scrub the environment: start empty, re-add only the allowlist. This keeps secrets like
        // ANTHROPIC_API_KEY out of the shells the agent may spawn (spec §4.8).
        cmd.env_clear();
        for (k, v) in allowlisted_env(env_passthrough, |k| std::env::var(k).ok()) {
            cmd.env(k, v);
        }

        cmd.arg("-p").arg("--output-format").arg("json");
        if let Some(model) = &turn.model {
            cmd.arg("--model").arg(model);
        }
        if let Some(system) = &turn.system_prompt {
            cmd.arg("--append-system-prompt").arg(system);
        }
        // Set AFTER env_clear/allowlist so it isn't wiped. File-based auth lives here (we never
        // pass an API key through env for claude — see env scrub above).
        if let Some(dir) = &self.config_dir {
            cmd.env("CLAUDE_CONFIG_DIR", dir);
        }

        match turn.mode {
            Mode::Text => {
                // Pure generator: block all tools. With every tool disabled the model has no file
                // or command access, so cwd is irrelevant; Phase 2 reinstates the empty-dir
                // hardening under the managed run lifecycle.
                cmd.arg("--disallowed-tools");
                for t in BLOCKED_TOOLS {
                    cmd.arg(t);
                }
            }
            Mode::Agentic => {
                if let Some(ws) = &turn.workspace {
                    cmd.current_dir(ws);
                    cmd.arg("--add-dir").arg(ws);
                }
                cmd.arg("--permission-mode").arg("bypassPermissions");
            }
        }

        (cmd, Some(turn.user_prompt.clone()))
    }

    /// Parse claude's single `--output-format json` object into normalized events.
    pub fn parse_output(&self, stdout: &str) -> Result<Vec<AgentEvent>, EngineError> {
        let v: serde_json::Value = serde_json::from_str(stdout.trim())
            .map_err(|e| EngineError::Parse(format!("{e}: {}", stdout.trim())))?;

        if v.get("is_error").and_then(|b| b.as_bool()).unwrap_or(false) {
            let msg = v.get("result").and_then(|r| r.as_str()).unwrap_or("claude reported an error");
            return Ok(vec![AgentEvent::Error(msg.to_string())]);
        }

        let mut events = Vec::new();
        if let Some(sid) = v.get("session_id").and_then(|s| s.as_str()) {
            events.push(AgentEvent::SessionId(sid.to_string()));
        }
        let result = v.get("result").and_then(|r| r.as_str()).unwrap_or("");
        events.push(AgentEvent::AssistantText(result.to_string()));
        let finish = v.get("stop_reason").and_then(|s| s.as_str()).unwrap_or("stop");
        events.push(AgentEvent::Done { finish_reason: normalize_finish(finish) });
        Ok(events)
    }

    /// Build the spawn command and stdin payload using stream-json. On resume, add `--resume <sid>`
    /// and omit the system prompt (the session already holds it); otherwise append the system prompt.
    pub fn build_stream_command(&self, turn: &Turn, env_passthrough: &[String]) -> (Command, Option<String>) {
        let mut cmd = Command::new(&self.bin);

        cmd.env_clear();
        for (k, v) in allowlisted_env(env_passthrough, |k| std::env::var(k).ok()) {
            cmd.env(k, v);
        }

        cmd.arg("-p").arg("--output-format").arg("stream-json").arg("--verbose");
        if let Some(model) = &turn.model {
            cmd.arg("--model").arg(model);
        }
        match &turn.resume {
            Some(sid) => {
                cmd.arg("--resume").arg(sid);
            }
            None => {
                if let Some(system) = &turn.system_prompt {
                    cmd.arg("--append-system-prompt").arg(system);
                }
            }
        }
        if let Some(dir) = &self.config_dir {
            cmd.env("CLAUDE_CONFIG_DIR", dir);
        }

        match turn.mode {
            Mode::Text => {
                cmd.arg("--disallowed-tools");
                for t in BLOCKED_TOOLS {
                    cmd.arg(t);
                }
            }
            Mode::Agentic => {
                if let Some(ws) = &turn.workspace {
                    cmd.current_dir(ws);
                    cmd.arg("--add-dir").arg(ws);
                }
                cmd.arg("--permission-mode").arg("bypassPermissions");
            }
        }

        (cmd, Some(turn.user_prompt.clone()))
    }

    /// Parse ONE line of claude `stream-json` output into zero or more normalized events.
    /// Unknown/blank/non-JSON lines yield nothing (claude interleaves hook + rate-limit noise).
    pub fn parse_stream_line(&self, line: &str) -> Vec<super::AgentEvent> {
        use super::AgentEvent;
        let line = line.trim();
        if line.is_empty() {
            return Vec::new();
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        match v.get("type").and_then(|t| t.as_str()) {
            Some("system") => {
                // Emit SessionId once, on the canonical init event.
                if v.get("subtype").and_then(|s| s.as_str()) == Some("init") {
                    if let Some(sid) = v.get("session_id").and_then(|s| s.as_str()) {
                        out.push(AgentEvent::SessionId(sid.to_string()));
                    }
                }
            }
            Some("assistant") => {
                if let Some(blocks) = v.pointer("/message/content").and_then(|c| c.as_array()) {
                    for b in blocks {
                        match b.get("type").and_then(|t| t.as_str()) {
                            Some("text") => {
                                if let Some(t) = b.get("text").and_then(|t| t.as_str()) {
                                    out.push(AgentEvent::AssistantText(t.to_string()));
                                }
                            }
                            Some("thinking") => {
                                if let Some(t) = b.get("thinking").and_then(|t| t.as_str()) {
                                    out.push(AgentEvent::Reasoning(t.to_string()));
                                }
                            }
                            Some("tool_use") => {
                                let name = b.get("name").and_then(|n| n.as_str()).unwrap_or("tool").to_string();
                                let args = b.get("input").map(|i| i.to_string()).unwrap_or_default();
                                out.push(AgentEvent::ToolStart { name, args });
                            }
                            _ => {}
                        }
                    }
                }
            }
            Some("user") => {
                // Tool results come back as user messages with tool_result content blocks.
                if let Some(blocks) = v.pointer("/message/content").and_then(|c| c.as_array()) {
                    for b in blocks {
                        if b.get("type").and_then(|t| t.as_str()) == Some("tool_result") {
                            out.push(AgentEvent::ToolResult { summary: "tool result".to_string() });
                        }
                    }
                }
            }
            Some("result") => {
                if v.get("is_error").and_then(|e| e.as_bool()).unwrap_or(false) {
                    let msg = v.get("result").and_then(|r| r.as_str()).unwrap_or("claude reported an error");
                    out.push(AgentEvent::Error(msg.to_string()));
                } else {
                    let finish = v.get("stop_reason").and_then(|s| s.as_str()).unwrap_or("stop");
                    // Do NOT re-emit `result` text — assistant text blocks already carried it.
                    out.push(AgentEvent::Done { finish_reason: normalize_finish(finish) });
                }
            }
            _ => {}
        }
        out
    }
}

/// The (key, value) pairs to set after `env_clear`: each allowlisted key that the lookup resolves.
/// Pure + injectable so the policy is unit-testable without touching the real environment.
pub(crate) fn allowlisted_env<F: Fn(&str) -> Option<String>>(
    passthrough: &[String],
    lookup: F,
) -> Vec<(String, String)> {
    passthrough.iter().filter_map(|k| lookup(k).map(|v| (k.clone(), v))).collect()
}

/// Map claude stop reasons to OpenAI finish_reasons.
fn normalize_finish(reason: &str) -> String {
    match reason {
        "end_turn" | "stop_sequence" => "stop",
        "max_tokens" => "length",
        other => other,
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Mode;
    use crate::engine::{AgentEvent, Turn};
    use std::path::PathBuf;

    fn turn(mode: Mode, ws: Option<&str>, resume: Option<&str>) -> Turn {
        Turn {
            system_prompt: Some("be terse".into()),
            user_prompt: "hi".into(),
            model: Some("opus".into()),
            workspace: ws.map(PathBuf::from),
            mode,
            resume: resume.map(String::from),
        }
    }

    fn args(cmd: &tokio::process::Command) -> Vec<String> {
        cmd.as_std().get_args().map(|a| a.to_string_lossy().into_owned()).collect()
    }

    #[test]
    fn fresh_text_mode_uses_stream_json_and_system_and_blocks_tools() {
        let a = ClaudeAdapter::new("claude", None);
        let (cmd, stdin) = a.build_stream_command(&turn(Mode::Text, None, None), &[]);
        let args = args(&cmd);
        assert!(args.windows(2).any(|w| w == ["--output-format", "stream-json"]));
        assert!(args.contains(&"--verbose".to_string()));
        assert!(args.contains(&"--append-system-prompt".to_string()));
        assert!(args.contains(&"--disallowed-tools".to_string()));
        assert!(!args.contains(&"--resume".to_string()));
        assert_eq!(stdin.as_deref(), Some("hi"));
    }

    #[test]
    fn resume_adds_resume_flag_and_omits_system_prompt() {
        let a = ClaudeAdapter::new("claude", None);
        let (cmd, _) = a.build_stream_command(&turn(Mode::Text, None, Some("sess-1")), &[]);
        let args = args(&cmd);
        assert!(args.windows(2).any(|w| w == ["--resume", "sess-1"]));
        // On resume the session already holds the system context — do not re-append it.
        assert!(!args.contains(&"--append-system-prompt".to_string()));
    }

    #[test]
    fn agentic_mode_workspace_and_bypass() {
        let a = ClaudeAdapter::new("claude", None);
        let (cmd, _) = a.build_stream_command(&turn(Mode::Agentic, Some("/work/repoA"), None), &[]);
        let args = args(&cmd);
        assert!(args.windows(2).any(|w| w == ["--permission-mode", "bypassPermissions"]));
        assert!(args.iter().any(|a| a == "/work/repoA"));
        assert!(!args.contains(&"--disallowed-tools".to_string()));
    }

    #[test]
    fn env_allowlist_keeps_only_passthrough_vars() {
        let lookup = |k: &str| match k { "PATH" => Some("/usr/bin".to_string()), "SECRET" => Some("x".to_string()), _ => None };
        let env = allowlisted_env(&["PATH".into(), "MISSING".into()], lookup);
        assert!(env.iter().any(|(k, v)| k == "PATH" && v == "/usr/bin"));
        assert!(!env.iter().any(|(k, _)| k == "SECRET"));
        assert!(!env.iter().any(|(k, _)| k == "MISSING"));
    }

    #[test]
    fn parse_stream_text_fixture_yields_session_text_and_done() {
        let a = ClaudeAdapter::new("claude", None);
        let raw = std::fs::read_to_string("tests/fixtures/claude_stream_text.jsonl").unwrap();
        let events: Vec<AgentEvent> = raw.lines().flat_map(|l| a.parse_stream_line(l)).collect();
        assert!(events.contains(&AgentEvent::SessionId("sess-abc".into())));
        assert!(events.contains(&AgentEvent::Reasoning("the user wants pong".into())));
        assert!(events.contains(&AgentEvent::AssistantText("pong".into())));
        assert!(events.iter().any(|e| matches!(e, AgentEvent::Done { finish_reason } if finish_reason == "stop")));
        // The `result` text must NOT be re-emitted as a second AssistantText (it duplicates the block).
        assert_eq!(events.iter().filter(|e| matches!(e, AgentEvent::AssistantText(_))).count(), 1);
    }

    #[test]
    fn parse_stream_error_fixture_yields_error() {
        let a = ClaudeAdapter::new("claude", None);
        let raw = std::fs::read_to_string("tests/fixtures/claude_stream_error.jsonl").unwrap();
        let events: Vec<AgentEvent> = raw.lines().flat_map(|l| a.parse_stream_line(l)).collect();
        assert!(events.iter().any(|e| matches!(e, AgentEvent::Error(m) if m.contains("context limit"))));
    }

    #[test]
    fn parse_ignores_non_json_and_unknown_lines() {
        let a = ClaudeAdapter::new("claude", None);
        assert!(a.parse_stream_line("not json").is_empty());
        assert!(a.parse_stream_line(r#"{"type":"rate_limit_event"}"#).is_empty());
    }
}
