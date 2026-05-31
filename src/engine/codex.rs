//! CodexAdapter: drive `codex exec --json` (JSONL events parsed line-by-line, with
//! `codex exec resume <id>` for continuity) under a scrubbed env + a dedicated `CODEX_HOME`, and
//! with `shell_environment_policy.inherit=core` so codex's own tool subprocesses don't inherit
//! service secrets. The prompt is passed as the trailing positional argument (no stdin).
use super::{AgentEvent, Turn};
use crate::config::Mode;
use crate::engine::claude::allowlisted_env;
use std::path::PathBuf;
use tokio::process::Command;

pub struct CodexAdapter {
    pub bin: String,
    pub codex_home: Option<PathBuf>,
}

/// Map the config `permissions` string to a codex `-s` sandbox level. Never `danger-full-access`
/// for request-driven runs. `None` (no permissions set) is the agentic default `workspace-write`;
/// an explicit but UNRECOGNIZED string fails safe to `read-only` rather than silently granting writes.
fn sandbox_level(permissions: Option<&str>) -> &'static str {
    match permissions {
        Some("read-only") => "read-only",
        Some("workspace-write") | None => "workspace-write",
        Some(_) => "read-only", // unrecognized permission → fail-safe to read-only
    }
}

impl CodexAdapter {
    pub fn new(bin: &str, codex_home: Option<PathBuf>) -> Self {
        CodexAdapter { bin: bin.into(), codex_home }
    }

    pub fn build_stream_command(&self, turn: &Turn, env_passthrough: &[String]) -> (Command, Option<String>) {
        let mut cmd = Command::new(&self.bin);

        cmd.env_clear();
        for (k, v) in allowlisted_env(env_passthrough, |k| std::env::var(k).ok()) {
            cmd.env(k, v);
        }
        if let Some(home) = &self.codex_home {
            cmd.env("CODEX_HOME", home);
        }

        cmd.arg("exec");
        if let Some(sid) = &turn.resume {
            cmd.arg("resume").arg(sid);
        }
        cmd.arg("--json").arg("--ignore-user-config");
        // Strip secrets from codex's own spawned tool shells (codex confines this; claude/agy can't).
        cmd.arg("-c").arg("shell_environment_policy.inherit=core");
        if let Some(model) = &turn.model {
            cmd.arg("--model").arg(model);
        }

        match turn.mode {
            Mode::Text => {
                // Pure generator: read-only sandbox, no workspace, never persist a session.
                cmd.arg("-s").arg("read-only").arg("--ephemeral");
            }
            Mode::Agentic => {
                cmd.arg("-s").arg(sandbox_level(turn.permissions.as_deref()));
                if let Some(ws) = &turn.workspace {
                    cmd.arg("--cd").arg(ws);
                }
            }
        }

        cmd.arg(&turn.user_prompt); // trailing positional prompt
        (cmd, None)
    }

    /// Parse ONE line of codex `exec --json` JSONL into zero or more normalized events. Tolerant of
    /// top-level `{"type":..}` and older `{"msg":{"type":..}}` nesting; unknown lines yield nothing.
    pub fn parse_stream_line(&self, line: &str) -> Vec<AgentEvent> {
        let line = line.trim();
        if line.is_empty() {
            return Vec::new();
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            return Vec::new();
        };
        // Events may be top-level or wrapped under `msg`.
        let ev = v.get("msg").unwrap_or(&v);
        let mut out = Vec::new();
        match ev.get("type").and_then(|t| t.as_str()) {
            Some("session_configured") | Some("session.created") | Some("session_created") => {
                let sid = ev.get("session_id").or_else(|| ev.get("id")).and_then(|s| s.as_str());
                if let Some(sid) = sid {
                    out.push(AgentEvent::SessionId(sid.to_string()));
                }
            }
            Some("agent_reasoning") => {
                if let Some(t) = ev.get("text").and_then(|t| t.as_str()) {
                    out.push(AgentEvent::Reasoning(t.to_string()));
                }
            }
            Some("exec_command_begin") | Some("tool_call") | Some("mcp_tool_call_begin") => {
                let name = ev
                    .pointer("/command/0")
                    .and_then(|n| n.as_str())
                    .or_else(|| ev.get("tool").and_then(|t| t.as_str()))
                    .or_else(|| ev.get("name").and_then(|t| t.as_str()))
                    .unwrap_or("tool")
                    .to_string();
                let args = ev
                    .get("command")
                    .or_else(|| ev.get("args"))
                    .or_else(|| ev.get("input"))
                    .map(|c| c.to_string())
                    .unwrap_or_default();
                out.push(AgentEvent::ToolStart { name, args });
            }
            // Final assistant message only (not deltas) — see fixture/parser note.
            Some("agent_message") => {
                if let Some(t) = ev.get("message").or_else(|| ev.get("text")).and_then(|t| t.as_str()) {
                    out.push(AgentEvent::AssistantText(t.to_string()));
                }
            }
            Some("task_complete") | Some("turn_complete") | Some("task.completed") | Some("turn.completed") => {
                out.push(AgentEvent::Done { finish_reason: "stop".to_string() });
            }
            Some("error") | Some("stream_error") => {
                let msg = ev.get("message").and_then(|m| m.as_str()).unwrap_or("codex reported an error");
                out.push(AgentEvent::Error(msg.to_string()));
            }
            _ => {}
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineKind, Mode};
    use crate::engine::{AgentEvent, Turn};
    use std::path::PathBuf;

    fn turn(mode: Mode, ws: Option<&str>, resume: Option<&str>) -> Turn {
        Turn {
            system_prompt: Some("be terse".into()),
            user_prompt: "hi".into(),
            model: Some("gpt-5".into()),
            workspace: ws.map(PathBuf::from),
            mode,
            resume: resume.map(String::from),
            engine: EngineKind::Codex,
            permissions: Some("workspace-write".into()),
        }
    }
    fn args(cmd: &tokio::process::Command) -> Vec<String> {
        cmd.as_std().get_args().map(|a| a.to_string_lossy().into_owned()).collect()
    }

    #[test]
    fn fresh_agentic_uses_exec_json_sandbox_cd_and_prompt_positional() {
        let a = CodexAdapter::new("codex", None);
        let (cmd, stdin) = a.build_stream_command(&turn(Mode::Agentic, Some("/work/repoB"), None), &[]);
        let args = args(&cmd);
        assert_eq!(args.first().map(String::as_str), Some("exec"));
        assert!(args.contains(&"--json".to_string()));
        assert!(args.contains(&"--ignore-user-config".to_string()));
        assert!(args.windows(2).any(|w| w == ["--model", "gpt-5"]));
        assert!(args.windows(2).any(|w| w == ["-s", "workspace-write"]));
        assert!(args.windows(2).any(|w| w == ["--cd", "/work/repoB"]));
        assert!(args.windows(2).any(|w| w[0] == "-c" && w[1] == "shell_environment_policy.inherit=core"));
        assert!(!args.contains(&"--ephemeral".to_string())); // agentic resumes -> persistent
        assert_eq!(args.last().map(String::as_str), Some("hi")); // prompt is the trailing positional
        assert!(stdin.is_none()); // codex takes the prompt as an arg, not stdin
    }

    #[test]
    fn unknown_permission_fails_safe_to_read_only() {
        // An explicit but unrecognized permission string must NOT grant writes.
        let a = CodexAdapter::new("codex", None);
        let mut t = turn(Mode::Agentic, Some("/work/repoB"), None);
        t.permissions = Some("totally-bogus".into());
        let args = args(&a.build_stream_command(&t, &[]).0);
        assert!(args.windows(2).any(|w| w == ["-s", "read-only"]));
        assert!(!args.windows(2).any(|w| w == ["-s", "workspace-write"]));
    }

    #[test]
    fn text_mode_is_readonly_and_ephemeral() {
        let a = CodexAdapter::new("codex", None);
        let (cmd, _) = a.build_stream_command(&turn(Mode::Text, None, None), &[]);
        let args = args(&cmd);
        assert!(args.windows(2).any(|w| w == ["-s", "read-only"]));
        assert!(args.contains(&"--ephemeral".to_string())); // text never resumes
        assert!(!args.contains(&"--cd".to_string()));
    }

    #[test]
    fn resume_uses_exec_resume_subcommand() {
        let a = CodexAdapter::new("codex", None);
        let (cmd, _) = a.build_stream_command(&turn(Mode::Agentic, Some("/work/repoB"), Some("sid-9")), &[]);
        let args = args(&cmd);
        // `codex exec resume <sid> ...`
        assert_eq!(&args[0..3], &["exec".to_string(), "resume".to_string(), "sid-9".to_string()]);
    }

    #[test]
    fn parse_text_fixture_yields_session_reasoning_tool_text_done() {
        let a = CodexAdapter::new("codex", None);
        let raw = std::fs::read_to_string("tests/fixtures/codex_stream_text.jsonl").unwrap();
        let evs: Vec<AgentEvent> = raw.lines().flat_map(|l| a.parse_stream_line(l)).collect();
        assert!(evs.contains(&AgentEvent::SessionId("codex-sess-1".into())));
        assert!(evs.contains(&AgentEvent::Reasoning("the user wants pong".into())));
        assert!(evs.iter().any(|e| matches!(e, AgentEvent::ToolStart { name, .. } if name == "bash")));
        assert!(evs.contains(&AgentEvent::AssistantText("pong".into())));
        assert!(evs.iter().any(|e| matches!(e, AgentEvent::Done { finish_reason } if finish_reason == "stop")));
        assert_eq!(evs.iter().filter(|e| matches!(e, AgentEvent::AssistantText(_))).count(), 1);
    }

    #[test]
    fn parse_error_fixture_yields_error() {
        let a = CodexAdapter::new("codex", None);
        let raw = std::fs::read_to_string("tests/fixtures/codex_stream_error.jsonl").unwrap();
        let evs: Vec<AgentEvent> = raw.lines().flat_map(|l| a.parse_stream_line(l)).collect();
        assert!(evs.iter().any(|e| matches!(e, AgentEvent::Error(m) if m.contains("context window"))));
    }

    #[test]
    fn parse_tolerates_msg_nesting_and_ignores_unknown() {
        let a = CodexAdapter::new("codex", None);
        // older schema: {"id":..,"msg":{"type":..}}
        assert_eq!(a.parse_stream_line(r#"{"id":"0","msg":{"type":"agent_message","message":"hey"}}"#),
                   vec![AgentEvent::AssistantText("hey".into())]);
        assert!(a.parse_stream_line("not json").is_empty());
        assert!(a.parse_stream_line(r#"{"type":"token_count","total":5}"#).is_empty());
    }
}
