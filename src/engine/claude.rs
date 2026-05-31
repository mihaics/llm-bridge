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

    fn turn(mode: Mode, ws: Option<&str>) -> Turn {
        Turn {
            system_prompt: Some("be terse".into()),
            user_prompt: "hi".into(),
            model: Some("opus".into()),
            workspace: ws.map(PathBuf::from),
            mode,
        }
    }

    fn arg_strings(cmd: &tokio::process::Command) -> Vec<String> {
        cmd.as_std().get_args().map(|a| a.to_string_lossy().into_owned()).collect()
    }

    #[test]
    fn text_mode_blocks_tools_and_passes_model_and_system() {
        let a = ClaudeAdapter::new("claude", None);
        let (cmd, stdin) = a.build_command(&turn(Mode::Text, None), &[]);
        let args = arg_strings(&cmd);
        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"json".to_string()));
        assert!(args.contains(&"--disallowed-tools".to_string()));
        assert!(args.contains(&"--model".to_string()) && args.contains(&"opus".to_string()));
        assert!(args.contains(&"--append-system-prompt".to_string()));
        assert_eq!(stdin.as_deref(), Some("hi")); // prompt via stdin
    }

    #[test]
    fn agentic_mode_adds_workspace_and_bypasses_permissions() {
        let a = ClaudeAdapter::new("claude", None);
        let (cmd, _) = a.build_command(&turn(Mode::Agentic, Some("/work/repoA")), &[]);
        let args = arg_strings(&cmd);
        assert!(args.contains(&"--permission-mode".to_string()));
        assert!(args.contains(&"bypassPermissions".to_string()));
        assert!(args.contains(&"--add-dir".to_string()));
        assert!(args.iter().any(|a| a == "/work/repoA"));
        assert!(!args.contains(&"--disallowed-tools".to_string()));
    }

    #[test]
    fn env_allowlist_keeps_only_passthrough_vars() {
        let lookup = |k: &str| match k {
            "PATH" => Some("/usr/bin".to_string()),
            "SECRET" => Some("xyz".to_string()),
            _ => None,
        };
        let env = allowlisted_env(&["PATH".into(), "MISSING".into()], lookup);
        assert!(env.iter().any(|(k, v)| k == "PATH" && v == "/usr/bin"));
        assert!(!env.iter().any(|(k, _)| k == "SECRET"));   // not allowlisted -> dropped
        assert!(!env.iter().any(|(k, _)| k == "MISSING"));  // allowlisted but absent -> not added
    }

    #[test]
    fn parses_success_result_into_events() {
        let a = ClaudeAdapter::new("claude", None);
        let raw = std::fs::read_to_string("tests/fixtures/claude_result.json").unwrap();
        let events = a.parse_output(&raw).unwrap();
        assert!(events.contains(&AgentEvent::SessionId("391b532e-a8ee-4bbf-9689-ab6891d09e90".into())));
        assert!(events.contains(&AgentEvent::AssistantText("pong".into())));
        assert!(events.iter().any(|e| matches!(e, AgentEvent::Done { .. })));
    }

    #[test]
    fn parses_error_result() {
        let a = ClaudeAdapter::new("claude", None);
        let raw = r#"{"is_error":true,"result":"context limit","session_id":"x"}"#;
        let events = a.parse_output(raw).unwrap();
        assert!(events.iter().any(|e| matches!(e, AgentEvent::Error(m) if m.contains("context limit"))));
    }
}
