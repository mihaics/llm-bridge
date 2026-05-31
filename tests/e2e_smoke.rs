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
    let engine = Engine::Claude(ClaudeAdapter::new("claude", None));
    let turn = Turn {
        system_prompt: None,
        user_prompt: "Reply with exactly: pong".into(),
        model: Some("sonnet".into()),
        workspace: None,
        mode: Mode::Text,
        resume: None,
    };
    let passthrough: Vec<String> =
        ["PATH", "HOME", "LANG", "LC_ALL", "TERM", "USER"].iter().map(|s| s.to_string()).collect();
    let (cmd, stdin) = engine.build_command(&turn, &passthrough);
    let out = ProcessSupervisor::new(1)
        .run(cmd, stdin, Duration::from_secs(120))
        .await
        .expect("claude ran");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let events = engine.parse_output(&String::from_utf8_lossy(&out.stdout)).unwrap();
    let text: String = events.iter().filter_map(|e| match e {
        AgentEvent::AssistantText(t) => Some(t.clone()),
        _ => None,
    }).collect();
    assert!(text.to_lowercase().contains("pong"), "got: {text}");
}
