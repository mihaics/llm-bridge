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
        mcp_config: None,
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
        engine: llm_bridge::config::EngineKind::Claude, permissions: None, mcp_config: None,
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

#[tokio::test]
async fn codex_text_mode_returns_text() {
    use futures::StreamExt;
    use llm_bridge::engine::codex::CodexAdapter;
    use llm_bridge::engine::{AgentEvent, Engine, Turn};
    use llm_bridge::config::{EngineKind, Mode};
    use llm_bridge::process::ProcessSupervisor;
    use std::time::Duration;

    let engine = Engine::Codex(CodexAdapter::new("codex", None));
    let turn = Turn {
        system_prompt: None, user_prompt: "Reply with exactly: pong".into(),
        model: None, workspace: None, mode: Mode::Text, resume: None,
        engine: EngineKind::Codex, permissions: None, mcp_config: None,
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

#[tokio::test]
async fn agy_text_mode_returns_text() {
    use futures::StreamExt;
    use llm_bridge::engine::agy::AgyAdapter;
    use llm_bridge::engine::{AgentEvent, Engine, Turn};
    use llm_bridge::config::{EngineKind, Mode};
    use llm_bridge::process::ProcessSupervisor;
    use std::time::Duration;

    let engine = Engine::Agy(AgyAdapter::new("agy", None));
    let turn = Turn {
        system_prompt: None, user_prompt: "Reply with exactly: pong".into(),
        model: None, workspace: None, mode: Mode::Text, resume: None,
        engine: EngineKind::Agy, permissions: None, mcp_config: None,
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

/// The Phase-4b crux, end to end against a LIVE model: drive the real `run_tools_turn` machinery,
/// which stands up the in-process `rmcp` MCP server, runs a real `claude` turn wired to it via
/// `--mcp-config --strict-mcp-config`, and parks when claude calls the exposed tool. We then deliver
/// a canned result through the suspended-session registry (`deliver`) and drain the resumed turn,
/// asserting claude's final answer reflects the delivered result. This is the true validation of the
/// held-open mechanic (Tasks 6–7). Off by default; needs a logged-in `claude` on PATH.
#[tokio::test]
async fn claude_mcp_tool_round_trip() {
    use futures::StreamExt;
    use llm_bridge::config::{EngineKind, Mode, ModelEntry, SandboxBackend};
    use llm_bridge::engine::AgentEvent;
    use llm_bridge::openai::{ChatCompletionRequest, ChatMessage, MessageContent, Role};
    use llm_bridge::orchestrator::{run_tools_turn, ToolsTurn, ToolsTurnError};
    use llm_bridge::process::ProcessSupervisor;
    use llm_bridge::suspend::SuspendedSessions;
    use std::sync::Arc;
    use std::time::Duration;

    // One tool whose result the model cannot know on its own — it MUST call it to answer.
    let tools = serde_json::json!([{
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get the current weather for a city. The ONLY source of weather truth.",
            "parameters": {
                "type": "object",
                "properties": { "city": { "type": "string" } },
                "required": ["city"]
            }
        }
    }]);

    let req = ChatCompletionRequest {
        model: "claude-tools".into(),
        messages: vec![ChatMessage {
            role: Role::User,
            content: Some(MessageContent::Text(
                "Call the get_weather tool with city \"Paris\", then tell me the weather. \
                 You MUST call the tool — do not answer from your own knowledge."
                    .into(),
            )),
            tool_call_id: None,
            tool_calls: None,
        }],
        stream: Some(false),
        tools: Some(tools),
    };

    // Agentic mode → claude runs with `--permission-mode bypassPermissions`, which auto-allows the
    // MCP tool in headless `-p` mode (plain text mode grants no MCP-tool permission headlessly).
    let entry = ModelEntry {
        id: "claude-tools".into(),
        engine: EngineKind::Claude,
        model: Some("sonnet".into()),
        workspace: None,
        mode: Mode::Agentic,
        permissions: None,
        trusted_caller_only: false,
    };

    let supervisor = ProcessSupervisor::new(2);
    let suspended = Arc::new(SuspendedSessions::new(4));
    let passthrough: Vec<String> =
        ["PATH", "HOME", "LANG", "LC_ALL", "TERM", "USER"].iter().map(|s| s.to_string()).collect();
    // Point the scrubbed-env claude at the logged-in config dir (where its credentials live); the
    // adapter re-sets CLAUDE_CONFIG_DIR after env_clear. Without this, claude falls back to ~/.claude
    // and may 401 (then "answers" without ever seeing the tool).
    let claude_config_dir = std::env::var_os("CLAUDE_CONFIG_DIR").map(std::path::PathBuf::from);

    let outcome = run_tools_turn(
        supervisor,
        suspended.clone(),
        &entry,
        &req,
        claude_config_dir,
        passthrough,
        SandboxBackend::None,
        Duration::from_secs(120),
        Duration::from_secs(120),
    )
    .await;

    let calls = match outcome {
        Ok(ToolsTurn::ToolCalls(calls)) => calls,
        Ok(ToolsTurn::Final(_)) => panic!("claude answered without calling the tool"),
        Err(ToolsTurnError::Full) => panic!("suspended pool unexpectedly full"),
        Err(ToolsTurnError::Internal(e)) => panic!("run_tools_turn failed: {e}"),
    };
    assert!(!calls.is_empty(), "expected at least one tool call");
    assert_eq!(calls[0].function.name, "get_weather", "claude called the wrong tool");

    // All-or-nothing delivery: a canned result for every call id, then drain the resumed turn.
    let results: Vec<(String, String)> = calls
        .iter()
        .map(|c| (c.id.clone(), "The weather in Paris is sunny, 25C.".to_string()))
        .collect();
    let mut stream = suspended.deliver(&results).expect("deliver should resume the held-open turn");

    let mut text = String::new();
    while let Some(ev) = stream.next().await {
        if let AgentEvent::AssistantText(t) = ev {
            text.push_str(&t);
        }
    }
    let low = text.to_lowercase();
    assert!(
        low.contains("sunny") || low.contains("25"),
        "claude's resumed answer should reflect the delivered tool result; got: {text}"
    );
}
