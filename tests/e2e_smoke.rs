//! Real-`claude` smoke test. Off by default. Run with:
//!   cargo test --features e2e_smoke --test e2e_smoke -- --nocapture
//! Requires a logged-in `claude` on PATH. The env allowlist must include what claude needs to run.
#![cfg(feature = "e2e_smoke")]

use llm_bridge::config::Mode;
use llm_bridge::engine::claude::ClaudeAdapter;
use llm_bridge::engine::{AgentEvent, Engine, Turn};
use llm_bridge::process::ProcessSupervisor;
use std::time::Duration;

#[tokio::test]
async fn claude_text_mode_returns_text() {
    use futures::StreamExt;
    let engine = Engine::Claude(ClaudeAdapter::new("claude", None));
    let turn = Turn {
        system_prompt: None,
        user_prompt: "Reply with exactly: pong".into(),
        model: Some("sonnet".into()),
        workspace: None,
        mode: Mode::Text,
        resume: None,
        engine: llm_bridge::config::EngineKind::Claude,
        permissions: None,
    };
    let passthrough: Vec<String> =
        ["PATH", "HOME", "LANG", "LC_ALL", "TERM", "USER"].iter().map(|s| s.to_string()).collect();
    let (cmd, stdin) = engine.build_stream_command(&turn, &passthrough);
    let lines = ProcessSupervisor::new(1).spawn_streaming(cmd, stdin, Duration::from_secs(120));
    futures::pin_mut!(lines);
    let mut text = String::new();
    while let Some(item) = lines.next().await {
        let line = item.expect("stream line");
        for ev in engine.parse_stream_line(&line) {
            if let AgentEvent::AssistantText(t) = ev { text.push_str(&t); }
        }
    }
    assert!(text.to_lowercase().contains("pong"), "got: {text}");
}

#[tokio::test]
async fn claude_stream_text_mode_yields_text() {
    use futures::StreamExt;
    use llm_bridge::engine::{AgentEvent, Engine, Turn};
    use llm_bridge::engine::claude::ClaudeAdapter;
    use llm_bridge::process::ProcessSupervisor;
    use llm_bridge::config::Mode;
    use std::time::Duration;

    let engine = Engine::Claude(ClaudeAdapter::new("claude", None));
    let turn = Turn {
        system_prompt: None, user_prompt: "Reply with exactly: pong".into(),
        model: Some("sonnet".into()), workspace: None, mode: Mode::Text, resume: None,
        engine: llm_bridge::config::EngineKind::Claude, permissions: None,
    };
    let passthrough: Vec<String> = ["PATH","HOME","LANG","LC_ALL","TERM","USER"].iter().map(|s| s.to_string()).collect();
    let (cmd, stdin) = engine.build_stream_command(&turn, &passthrough);
    let lines = ProcessSupervisor::new(1).spawn_streaming(cmd, stdin, Duration::from_secs(120));
    futures::pin_mut!(lines);
    let mut text = String::new();
    while let Some(item) = lines.next().await {
        let line = item.expect("stream line");
        for ev in engine.parse_stream_line(&line) {
            if let AgentEvent::AssistantText(t) = ev { text.push_str(&t); }
        }
    }
    assert!(text.to_lowercase().contains("pong"), "got: {text}");
}
