//! AgyAdapter: drive `agy --print` (plain text, non-streaming) under a scrubbed env. agy exposes no
//! config-dir/MCP/profile flag and its session-id capture + credential isolation are unproven
//! (spec §3.2, §9), so v1 ignores resume (stateless full-transcript replay, gated by
//! `caps(Agy).resume_by_id == false`) and runs under the operator's own login. The prompt is the
//! trailing positional argument; each output line becomes one `AssistantText`.
use super::{AgentEvent, Turn};
use crate::config::Mode;
use crate::engine::claude::allowlisted_env;
use std::path::PathBuf;
use tokio::process::Command;

pub struct AgyAdapter {
    pub bin: String,
    /// Reserved for the §9 spike (agy has no proven config-dir redirect); unused in v1.
    pub config_dir: Option<PathBuf>,
}

impl AgyAdapter {
    pub fn new(bin: &str, config_dir: Option<PathBuf>) -> Self {
        AgyAdapter { bin: bin.into(), config_dir }
    }

    pub fn build_stream_command(&self, turn: &Turn, env_passthrough: &[String]) -> (Command, Option<String>) {
        let mut cmd = Command::new(&self.bin);

        cmd.env_clear();
        for (k, v) in allowlisted_env(env_passthrough, |k| std::env::var(k).ok()) {
            cmd.env(k, v);
        }

        cmd.arg("--print");
        if let Some(model) = &turn.model {
            cmd.arg("--model").arg(model);
        }
        // NOTE: resume intentionally ignored in v1 (no --conversation/--continue) — gated off.
        if turn.mode == Mode::Agentic {
            if let Some(ws) = &turn.workspace {
                cmd.current_dir(ws);
                cmd.arg("--add-dir").arg(ws);
            }
            cmd.arg("--sandbox"); // agy's coarse boolean sandbox
        }

        cmd.arg(&turn.user_prompt); // trailing positional prompt
        (cmd, None)
    }

    /// Each output line is a chunk of the answer. We re-append the newline that `BufReader::lines`
    /// strips, so multi-line answers keep their formatting once aggregated.
    ///
    /// Unlike the claude/codex parsers, this intentionally does NOT skip empty lines: agy emits
    /// plain text, so a blank line is real content (a paragraph break) and must be preserved —
    /// whereas the JSONL engines' blank lines are inter-event noise. (`BufReader::lines` already
    /// drops the single trailing newline, so no phantom trailing blank is produced.)
    pub fn parse_stream_line(&self, line: &str) -> Vec<AgentEvent> {
        vec![AgentEvent::AssistantText(format!("{line}\n"))]
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
            model: Some("gemini-2.5-pro".into()),
            workspace: ws.map(PathBuf::from),
            mode,
            resume: resume.map(String::from),
            engine: EngineKind::Agy,
            permissions: None,
            mcp_config: None,
        }
    }
    fn args(cmd: &tokio::process::Command) -> Vec<String> {
        cmd.as_std().get_args().map(|a| a.to_string_lossy().into_owned()).collect()
    }

    #[test]
    fn fresh_agentic_uses_print_adddir_sandbox_and_positional_prompt() {
        let a = AgyAdapter::new("agy", None);
        let (cmd, stdin) = a.build_stream_command(&turn(Mode::Agentic, Some("/work/repoC"), None), &[]);
        let args = args(&cmd);
        assert!(args.contains(&"--print".to_string()));
        assert!(args.windows(2).any(|w| w == ["--add-dir", "/work/repoC"]));
        assert!(args.contains(&"--sandbox".to_string()));
        assert_eq!(args.last().map(String::as_str), Some("hi")); // positional prompt
        assert!(stdin.is_none());
    }

    #[test]
    fn text_mode_omits_workspace_and_sandbox_flag() {
        let a = AgyAdapter::new("agy", None);
        let (cmd, _) = a.build_stream_command(&turn(Mode::Text, None, None), &[]);
        let args = args(&cmd);
        assert!(args.contains(&"--print".to_string()));
        assert!(!args.contains(&"--add-dir".to_string()));
    }

    #[test]
    fn resume_is_ignored_no_conversation_flag() {
        // agy resume is gated off in v1 — even with a resume id, no --conversation/--continue is added.
        let a = AgyAdapter::new("agy", None);
        let (cmd, _) = a.build_stream_command(&turn(Mode::Agentic, Some("/work/repoC"), Some("conv-1")), &[]);
        let args = args(&cmd);
        assert!(!args.iter().any(|a| a == "--conversation" || a == "--continue" || a == "conv-1"));
    }

    #[test]
    fn parse_emits_one_assistant_text_per_line() {
        let a = AgyAdapter::new("agy", None);
        assert_eq!(a.parse_stream_line("pong"), vec![AgentEvent::AssistantText("pong\n".into())]);
        // No SessionId, no Done — agy provides neither on stdout (runner supplies the terminal Done).
        assert!(!a.parse_stream_line("anything").iter().any(|e| matches!(e, AgentEvent::SessionId(_) | AgentEvent::Done { .. })));
    }

    #[test]
    fn parse_fixture_accumulates_full_text() {
        let a = AgyAdapter::new("agy", None);
        let raw = std::fs::read_to_string("tests/fixtures/agy_print.txt").unwrap();
        let text: String = raw.lines().flat_map(|l| a.parse_stream_line(l)).filter_map(|e| match e {
            AgentEvent::AssistantText(t) => Some(t), _ => None,
        }).collect();
        assert!(text.contains("pong"));
        assert!(text.contains("second line"));
    }
}
