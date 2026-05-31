# llm-bridge Phase 2 — SSE Streaming + Session Resume — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add real **SSE streaming** (`stream: true`) and **content-hash session resume** to the Phase-1 claude shim, unifying both the streaming and non-streaming paths on claude's `--output-format stream-json` event stream.

**Architecture:** The ClaudeAdapter switches to `claude -p --output-format stream-json --verbose` (message-level events: `system`→session_id, `assistant` content blocks→text/reasoning/tool_use, `result`→finish). `ProcessSupervisor` gains `spawn_streaming` returning a live `Stream` of stdout lines (permit held + child killed on drop, timeout enforced). The orchestrator resolves a request to a **session key** (projection + ModelEntry + tool-config + system-prompt + runtime fingerprints); a **hit** resumes the stored claude session (`--resume <sid>`) feeding only the new turn, a **miss** starts fresh with the full read-only transcript. After every turn it captures the session id and **advances the index**. The HTTP layer streams events as OpenAI `chat.completion.chunk`s (`delta.content` for the answer; `delta.reasoning_content` for reasoning/tool progress under the `reasoning_content` profile, suppressed under `omit`); non-streaming aggregates the same event stream into one completion.

**Tech Stack:** Adds `futures` (Stream/StreamExt), `async-stream` (the `stream!` macro for the line→event and capture/advance wrappers). Builds on Phase 1 (tokio, axum 0.8, serde). Spec: `docs/superpowers/specs/2026-05-31-llm-bridge-design.md` (§4.4 events, §4.5 sessions, §4.6 n/a here, §5 data flow).

**Scope note — Phase 2 of 4.** Out of scope (still refused or absent): the MCP `tools` bridge (Phase 4 — `tools` still → 400), Codex/Agy adapters + `sandbox_backend` (Phase 3). Session persistence across server restarts is **in-memory only** in Phase 2 (a restart loses the index and correctly degrades to the full-transcript miss path); a `sled`/file backend is a later add. Non-streaming responses carry only the final answer in `content` (reasoning/tool progress is dropped in non-streaming; it appears only in the streaming `reasoning_content` channel).

---

## ⚠️ Sequencing strategy (ADDITIVE — read before executing)

Phase 2 is a cross-cutting change (the `TurnRunner` trait flips from aggregate `run` to streaming
`run_stream`). To keep **every task end with a green full `cargo test`**, follow the *additive*
strategy below — do NOT remove or repurpose Phase-1 code until the single atomic switch task:

- **Tasks 1–4 are purely additive.** Keep the Phase-1 json path intact: keep
  `ClaudeAdapter::{build_command, parse_output}`, keep `ProcessSupervisor::run`, keep
  `TurnRunner::run` and the Phase-1 `http.rs`/`http_integration.rs` exactly as they are. *Add*
  the new pieces beside them: new `AgentEvent` variants + `Turn.resume` + `EventStream` (T1),
  `ClaudeAdapter::{build_stream_command, parse_stream_line}` (T2), `spawn_streaming` (T3),
  `session.rs` (T4). After each of T1–T4, all 43 Phase-1 tests still pass plus the new ones.
- **Tasks 5 + 6 together are the ONE atomic switch.** Task 5 rewrites `orchestrator.rs` (trait →
  `run_stream`, `ClaudeProcessRunner`, `run_request`) — which **leaves the lib intentionally red**
  (the Phase-1 `http.rs` still calls the old `TurnRunner::run`). **Do not run the full suite or
  commit at the end of Task 5.** Task 6 then adds the OpenAI chunk types, rewrites `http.rs`
  (SSE + aggregate + new `AppState` fields) and `tests/http_integration.rs`, **deletes** the
  now-dead legacy code (`ProcessSupervisor::run`, `ProcessError`, `ClaudeAdapter::build_command`,
  `parse_output`, `Engine::{build_command, parse_output}`), restores a green full suite, and makes
  **one commit** covering both tasks. (Lifetimes prevent a clean default-impl bridge, so the
  trait switch is atomic across these two tasks — treat 5+6 as a single unit when executing.)
- **Task 7** wires `main`, the e2e smoke, and the README.

The task bodies below are written in this additive shape. Where a task says "replace", it means
*add alongside*; only Task 5 deletes Phase-1 code.

---

## Starting state (end of Phase 1)
`cargo test` → 43 passing on `master`. Relevant current shapes:
- `engine/mod.rs`: `Turn { system_prompt, user_prompt, model, workspace, mode }`; `AgentEvent { AssistantText, SessionId, Error, Done }`; `Engine::build_command(&self, &Turn, &[String]) -> (Command, Option<String>)`; `Engine::parse_output(&self, &str) -> Result<Vec<AgentEvent>, EngineError>`.
- `engine/claude.rs`: `ClaudeAdapter::{new, build_command, parse_output}` (uses `--output-format json`), `allowlisted_env`, `normalize_finish`, `BLOCKED_TOOLS`.
- `process.rs`: `ProcessSupervisor { sem }`, `run(cmd, stdin, timeout) -> Result<Output, ProcessError>`; `ProcessError { Timeout, Io }`.
- `orchestrator.rs`: `TurnRunner::run(&self, &ModelEntry, Turn) -> RunFuture`, `RunError`, `ClaudeProcessRunner`, `turn_from_request`, `response_from_events`.
- `http.rs`: `AppState { registry, bearer_token, runner }`, `build_router`, `chat_completions` (rejects stream/tools, runs runner, aggregates).
- `openai.rs`: request/response/error types.

---

## File Structure (Phase 2 changes)
```
Cargo.toml                 # + futures, async-stream
src/engine/mod.rs          # AgentEvent: +Reasoning/ToolStart/ToolResult; Turn: +resume; replace parse_output with parse_stream_line; EventStream type
src/engine/claude.rs       # build_command -> stream-json + resume; parse_stream_line (replaces parse_output); update tests + fixtures
src/process.rs             # + spawn_streaming (live line Stream); keep run() (still used? no -> remove) 
src/session.rs   (NEW)     # SessionStore (in-memory), session_key, projection, fingerprints, index advancement
src/orchestrator.rs        # TurnRunner::run_stream (EventStream); ClaudeProcessRunner::run_stream; run_request (key/hit-miss/resume/capture/advance); response_from_events (handle new events); rendering helpers
src/openai.rs              # + ChatCompletionChunk / ChunkChoice / Delta (streaming wire types)
src/http.rs                # AppState +sessions; chat_completions: SSE when stream:true, aggregate otherwise
src/main.rs                # build SessionStore, put in AppState
tests/fixtures/claude_stream_text.jsonl       (NEW) stream-json for a plain text answer
tests/fixtures/claude_stream_error.jsonl      (NEW) stream-json error result
tests/http_integration.rs  # streaming + non-streaming + resume tests (FakeRunner updated to run_stream)
README.md                  # status update
```

---

## Task 1: Dependencies + event/Turn extensions + EventStream type

**Files:** Modify `Cargo.toml`, `src/engine/mod.rs`.

- [ ] **Step 1: Add deps to `Cargo.toml`** (under `[dependencies]`, after `tracing-subscriber`)

```toml
futures = "0.3"
async-stream = "0.3"
```

- [ ] **Step 2: Write failing tests** — replace the existing `#[cfg(test)] mod tests` in `src/engine/mod.rs` with:

```rust
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
```

- [ ] **Step 3: Run, expect fail**

Run: `cargo test --lib engine`
Expected: compile errors (new variants / `resume` field not defined).

- [ ] **Step 4: Edit `src/engine/mod.rs`** — apply these exact changes:

(a) Replace the `Turn` struct with:
```rust
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
```

(b) Replace the `AgentEvent` enum with:
```rust
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
```

(c) Add this near the top of the file (after the `use` lines), defining the boxed event stream alias:
```rust
use futures::Stream;
use std::pin::Pin;

/// A boxed, owned stream of normalized engine events.
pub type EventStream = Pin<Box<dyn Stream<Item = AgentEvent> + Send>>;
```

(d) **Do NOT touch `parse_output` or add an `Engine::parse_stream_line` dispatch here** — that is
Task 2 (kept additive). Task 1 only adds the new *types*.

(e) Because `Turn` gained the `resume` field, every existing `Turn { ... }` construction must add
`resume: None` to keep compiling. Update them:
- `src/orchestrator.rs` → `turn_from_request` builds a `Turn` (add `resume: None`).
- `src/orchestrator.rs` test module → the `Turn`/`req` helpers, if any construct `Turn`.
- `src/engine/claude.rs` test module → the `turn(...)` helper (add `resume: None`).
- `tests/e2e_smoke.rs` → the `Turn { ... }` literal (add `resume: None`).

(f) Because `AgentEvent` gained variants, `response_from_events` in `src/orchestrator.rs` must
handle them. Add the new variants to its existing catch-all arm so they're ignored for `content`:
```rust
            AgentEvent::Reasoning(_)
            | AgentEvent::ToolStart { .. }
            | AgentEvent::ToolResult { .. }
            | AgentEvent::SessionId(_) => {}
```
(replace the existing `AgentEvent::SessionId(_) => {}` arm with the combined arm above).

- [ ] **Step 5: Run, expect pass (fully green — additive)**

Run: `cargo test`
Expected: all 43 Phase-1 tests still pass, plus the 3 new engine tests. The crate stays fully
compiling because Task 1 is purely additive — `build_command`/`parse_output`/`TurnRunner::run`
and the whole Phase-1 path are untouched.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src/engine/mod.rs src/engine/claude.rs src/orchestrator.rs tests/e2e_smoke.rs
git commit -m "feat: add AgentEvent reasoning/tool variants, Turn.resume, EventStream (additive)"
```

> **DONE_WITH_CONCERNS is expected here:** this task intentionally leaves temporary shims (empty `parse_stream_line`, ignored tests) that Tasks 2 and 5 replace. Report which tests you ignored.

---

## Task 2: ClaudeAdapter — stream-json command + line parser

**Files:** Modify `src/engine/claude.rs`; create `tests/fixtures/claude_stream_text.jsonl`, `tests/fixtures/claude_stream_error.jsonl`.

- [ ] **Step 1: Create `tests/fixtures/claude_stream_text.jsonl`** (one JSON object per line; real shapes captured from claude):

```
{"type":"system","subtype":"init","session_id":"sess-abc","cwd":"/tmp"}
{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"the user wants pong"}]},"session_id":"sess-abc"}
{"type":"assistant","message":{"content":[{"type":"text","text":"pong"}]},"session_id":"sess-abc"}
{"type":"result","subtype":"success","is_error":false,"result":"pong","session_id":"sess-abc","stop_reason":"end_turn"}
```

- [ ] **Step 2: Create `tests/fixtures/claude_stream_error.jsonl`**:

```
{"type":"system","subtype":"init","session_id":"sess-err"}
{"type":"result","subtype":"error","is_error":true,"result":"context limit exceeded","session_id":"sess-err"}
```

- [ ] **Step 3: Write failing tests** — **replace** the `#[cfg(test)] mod tests` in `src/engine/claude.rs` with the module below. (The Phase-1 json `build_command` tests are dropped here; that json path stays compiled and is still exercised via the orchestrator/`http_integration` non-streaming tests until Task 5 removes it.) These tests target the NEW `build_stream_command`:

```rust
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
```

- [ ] **Step 4: Run, expect fail**

Run: `cargo test --lib engine::claude`
Expected: failures (`build_stream_command` and `parse_stream_line` don't exist yet).

- [ ] **Step 5: ADD the streaming methods to `src/engine/claude.rs`** — keep everything from Phase 1
(`ClaudeAdapter`/`new`/`BLOCKED_TOOLS`/`allowlisted_env`/`normalize_finish`/`build_command`/`parse_output`).
**Add** two new methods to `impl ClaudeAdapter` (alongside the existing ones):

```rust
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
```

- [ ] **Step 6: Add the `Engine` dispatch methods in `src/engine/mod.rs`** — **add** (keep the
existing `build_command`/`parse_output` dispatch methods) to `impl Engine`:

```rust
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
```

- [ ] **Step 7: Run, expect pass (fully green)**

Run: `cargo test`
Expected: the 7 `engine::claude` tests pass, and the full suite stays green (Phase-1 json
`build_command`/`parse_output` and all 43 prior tests are untouched).

- [ ] **Step 8: Commit**

```bash
git add src/engine/mod.rs src/engine/claude.rs tests/fixtures/claude_stream_text.jsonl tests/fixtures/claude_stream_error.jsonl
git commit -m "feat: add ClaudeAdapter stream-json command (+resume) and per-line parser (additive)"
```

---

## Task 3: ProcessSupervisor.spawn_streaming

**Files:** Modify `src/process.rs`.

- [ ] **Step 1: Write failing tests** — add these to the `#[cfg(test)] mod tests` in `src/process.rs` (keep the existing 4 tests):

```rust
    #[tokio::test]
    async fn spawn_streaming_yields_stdout_lines() {
        use futures::StreamExt;
        let sup = ProcessSupervisor::new(2);
        let mut cmd = Command::new("bash");
        cmd.arg("-c").arg("printf 'a\\nb\\nc\\n'");
        let stream = sup.spawn_streaming(cmd, None, Duration::from_secs(5));
        let lines: Vec<String> = stream.filter_map(|r| async move { r.ok() }).collect().await;
        assert_eq!(lines, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn spawn_streaming_writes_stdin() {
        use futures::StreamExt;
        let sup = ProcessSupervisor::new(2);
        let cmd = Command::new("cat");
        let stream = sup.spawn_streaming(cmd, Some("hello\n".into()), Duration::from_secs(5));
        let lines: Vec<String> = stream.filter_map(|r| async move { r.ok() }).collect().await;
        assert_eq!(lines, vec!["hello"]);
    }

    #[tokio::test]
    async fn spawn_streaming_times_out() {
        use futures::StreamExt;
        let sup = ProcessSupervisor::new(2);
        let mut cmd = Command::new("bash");
        cmd.arg("-c").arg("printf 'first\\n'; sleep 5; printf 'late\\n'");
        let stream = sup.spawn_streaming(cmd, None, Duration::from_millis(300));
        let results: Vec<_> = stream.collect().await;
        // first line arrives, then a timeout error terminates the stream (no "late").
        assert_eq!(results.iter().filter(|r| matches!(r, Ok(l) if l == "first")).count(), 1);
        assert!(results.iter().any(|r| matches!(r, Err(e) if e.kind() == std::io::ErrorKind::TimedOut)));
        assert!(!results.iter().any(|r| matches!(r, Ok(l) if l == "late")));
    }
```

- [ ] **Step 2: Run, expect fail**

Run: `cargo test --lib process`
Expected: compile error (`spawn_streaming` undefined).

- [ ] **Step 3: Add the implementation to `src/process.rs`** — add these imports at the top (alongside existing ones) and the method to `impl ProcessSupervisor`:

```rust
// add to the top `use` section:
use futures::Stream;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
```

```rust
    /// Spawn a child and stream its stdout line-by-line as it arrives. The concurrency permit and
    /// the child are owned by the returned stream: dropping the stream frees the permit and kills
    /// the child (kill_on_drop). A per-stream deadline turns into a terminal TimedOut error.
    pub fn spawn_streaming(
        &self,
        mut cmd: Command,
        stdin: Option<String>,
        timeout: Duration,
    ) -> impl Stream<Item = std::io::Result<String>> + Send {
        let sem = self.sem.clone();
        async_stream::stream! {
            let _permit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(_) => { yield Err(std::io::Error::other("supervisor shut down")); return; }
            };

            cmd.stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            cmd.kill_on_drop(true);

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => { yield Err(e); return; }
            };

            if let Some(payload) = stdin {
                if let Some(mut sink) = child.stdin.take() {
                    use tokio::io::AsyncWriteExt;
                    let _ = sink.write_all(payload.as_bytes()).await;
                    let _ = sink.shutdown().await;
                }
            }

            let stdout = match child.stdout.take() {
                Some(s) => s,
                None => { yield Err(std::io::Error::other("no stdout")); return; }
            };
            let mut reader = BufReader::new(stdout).lines();
            let deadline = tokio::time::Instant::now() + timeout;

            loop {
                match tokio::time::timeout_at(deadline, reader.next_line()).await {
                    Ok(Ok(Some(line))) => yield Ok(line),
                    Ok(Ok(None)) => break,                       // clean EOF
                    Ok(Err(e)) => { yield Err(e); break; }
                    Err(_elapsed) => {
                        yield Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "child timed out"));
                        break;
                    }
                }
            }
            // `child` (kill_on_drop) and `_permit` drop here when the stream is dropped/exhausted.
            drop(child);
        }
    }
```

Note: `run()` (the Phase-1 aggregate spawn) is no longer used after Task 5 switches the runner to streaming. Leave `run()` in place for now; Task 5's commit removes it once nothing references it. Keep `ProcessError` (still used by `run()` until then).

- [ ] **Step 4: Run, expect pass**

Run: `cargo test --lib process`
Expected: `test result: ok. 7 passed` (4 existing + 3 new).

- [ ] **Step 5: Commit**

```bash
git add src/process.rs
git commit -m "feat: ProcessSupervisor::spawn_streaming (live stdout line stream, permit+child owned, timeout)"
```

---

## Task 4: SessionStore + content-hash key

**Files:** Create `src/session.rs`; add `pub mod session;` to `src/lib.rs`.

- [ ] **Step 1: Register the module** — add `pub mod session;` to `src/lib.rs` (keep alphabetical-ish ordering; place after `pub mod registry;`).

- [ ] **Step 2: Write failing tests** (`#[cfg(test)] mod tests` in `src/session.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineKind, Mode, ModelEntry};
    use crate::openai::{ChatMessage, MessageContent, Role};

    fn entry() -> ModelEntry {
        ModelEntry { id: "m".into(), engine: EngineKind::Claude, model: Some("opus".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false }
    }
    fn rt() -> RuntimeFingerprint {
        RuntimeFingerprint { claude_config_dir: Some("/cred".into()), sandbox_backend: "none".into() }
    }
    fn msg(role: Role, t: &str) -> ChatMessage {
        ChatMessage { role, content: Some(MessageContent::Text(t.into())), tool_call_id: None }
    }

    #[test]
    fn store_roundtrips() {
        let s = SessionStore::new();
        s.insert("k".into(), "sid-1".into());
        assert_eq!(s.get("k"), Some("sid-1".to_string()));
        assert_eq!(s.get("missing"), None);
    }

    #[test]
    fn lookup_prefix_excludes_final_user_turn() {
        // First turn: only one user message -> lookup prefix is empty.
        let m1 = vec![msg(Role::User, "first")];
        let k1 = lookup_key(&m1, &entry(), None, &rt());
        // After the turn, the stored key includes the user turn + assistant reply.
        let stored = stored_key_after(&m1, "answer one", &entry(), None, &rt());
        // Second turn resends history; its lookup prefix == the stored prefix -> SAME key.
        let m2 = vec![msg(Role::User, "first"), msg(Role::Assistant, "answer one"), msg(Role::User, "second")];
        let k2 = lookup_key(&m2, &entry(), None, &rt());
        assert_ne!(k1, k2);
        assert_eq!(stored, k2, "stored key must equal next turn's lookup key (resume hit)");
    }

    #[test]
    fn key_changes_with_system_prompt_and_runtime_and_tools() {
        let m = vec![msg(Role::User, "x")];
        let base = lookup_key(&m, &entry(), None, &rt());
        let with_sys = lookup_key(&m, &entry(), Some("sys"), &rt());
        let mut rt2 = rt(); rt2.sandbox_backend = "bubblewrap".into();
        let with_rt = lookup_key(&m, &entry(), None, &rt2);
        assert_ne!(base, with_sys);
        assert_ne!(base, with_rt);
    }
}
```

- [ ] **Step 3: Run, expect fail**

Run: `cargo test --lib session`
Expected: compile error.

- [ ] **Step 4: Write `src/session.rs`**

```rust
//! In-memory session index: maps a content-hash key to the native claude session id, so a
//! follow-up request can resume the agent's own on-disk session. Restart loses the index and
//! correctly degrades to the full-transcript miss path (spec §4.5). The key folds in the
//! conversation projection plus ModelEntry, system-prompt, tool-config, and runtime fingerprints.
use crate::config::ModelEntry;
use crate::openai::{ChatMessage, Role};
use crate::registry::model_entry_fingerprint;
use std::collections::HashMap;
use std::sync::Mutex;

/// Runtime context a native session id only makes sense within (cred dir + sandbox backend).
#[derive(Debug, Clone)]
pub struct RuntimeFingerprint {
    pub claude_config_dir: Option<String>,
    pub sandbox_backend: String,
}

impl RuntimeFingerprint {
    fn canon(&self) -> String {
        format!("cfg={:?};sandbox={}", self.claude_config_dir, self.sandbox_backend)
    }
}

#[derive(Default)]
pub struct SessionStore {
    map: Mutex<HashMap<String, String>>,
}

impl SessionStore {
    pub fn new() -> Self {
        SessionStore { map: Mutex::new(HashMap::new()) }
    }
    pub fn get(&self, key: &str) -> Option<String> {
        self.map.lock().unwrap().get(key).cloned()
    }
    pub fn insert(&self, key: String, session_id: String) {
        self.map.lock().unwrap().insert(key, session_id);
    }
}

/// Canonical projection of a message list: role + flattened content per message, newline-joined.
/// (Phase 2 has no client-supplied tool_calls; Phase 4 will extend this with tool_call ids.)
fn project(messages: &[ChatMessage]) -> String {
    messages
        .iter()
        .map(|m| format!("{:?}:{}", m.role, m.text()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn hash(parts: &[&str]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for p in parts {
        for &b in p.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h ^= 0xff; // separator so ["ab","c"] != ["a","bc"]
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{h:016x}")
}

fn key_for(prefix: &[ChatMessage], entry: &ModelEntry, system_prompt: Option<&str>, rt: &RuntimeFingerprint) -> String {
    hash(&[
        &project(prefix),
        &model_entry_fingerprint(entry),
        system_prompt.unwrap_or(""),
        // tool-config fingerprint: empty in Phase 2 (no client tools); reserved slot for Phase 4.
        "",
        &rt.canon(),
    ])
}

/// Index of the final user message in the list (the live instruction), if any.
fn final_user_idx(messages: &[ChatMessage]) -> Option<usize> {
    messages.iter().rposition(|m| m.role == Role::User)
}

/// Key to LOOK UP on an incoming request: hash of the prefix BEFORE the final user turn.
pub fn lookup_key(messages: &[ChatMessage], entry: &ModelEntry, system_prompt: Option<&str>, rt: &RuntimeFingerprint) -> String {
    let cut = final_user_idx(messages).unwrap_or(messages.len());
    key_for(&messages[..cut], entry, system_prompt, rt)
}

/// Key to STORE after a turn completes: the prefix the NEXT request will look up = the current
/// messages plus the assistant reply we just produced.
pub fn stored_key_after(messages: &[ChatMessage], assistant_text: &str, entry: &ModelEntry, system_prompt: Option<&str>, rt: &RuntimeFingerprint) -> String {
    let mut extended = messages.to_vec();
    extended.push(ChatMessage {
        role: Role::Assistant,
        content: Some(crate::openai::MessageContent::Text(assistant_text.to_string())),
        tool_call_id: None,
    });
    key_for(&extended, entry, system_prompt, rt)
}
```

- [ ] **Step 5: Run, expect pass**

Run: `cargo test --lib session`
Expected: `test result: ok. 3 passed`.

- [ ] **Step 6: Commit**

```bash
git add src/lib.rs src/session.rs
git commit -m "feat: in-memory SessionStore + content-hash key (projection + entry/system/tool/runtime fingerprints)"
```

---

## Task 5: Streaming TurnRunner + session-aware request execution (atomic switch — part 1 of 2)

> **Part 1 of the atomic switch (see Sequencing strategy).** This task rewrites `orchestrator.rs`
> and **intentionally leaves the lib not-compiling** (Phase-1 `http.rs` still references the old
> `TurnRunner::run`, which this task removes). **Do NOT run the full suite or commit at the end of
> this task** — proceed directly to Task 6, which completes the switch, removes legacy code,
> restores green, and commits both tasks together.

**Files:** Modify `src/orchestrator.rs`.

- [ ] **Step 1: Write failing tests** — replace the `#[cfg(test)] mod tests` in `src/orchestrator.rs` with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineKind, Mode, ModelEntry};
    use crate::engine::AgentEvent;
    use crate::openai::{ChatCompletionRequest, ChatMessage, MessageContent, Role};
    use crate::session::{RuntimeFingerprint, SessionStore};
    use futures::StreamExt;
    use std::sync::Arc;

    fn entry() -> ModelEntry {
        ModelEntry { id: "m".into(), engine: EngineKind::Claude, model: Some("opus".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false }
    }
    fn rt() -> RuntimeFingerprint {
        RuntimeFingerprint { claude_config_dir: None, sandbox_backend: "none".into() }
    }
    fn umsg(t: &str) -> ChatMessage {
        ChatMessage { role: Role::User, content: Some(MessageContent::Text(t.into())), tool_call_id: None }
    }
    fn req(msgs: Vec<ChatMessage>) -> ChatCompletionRequest {
        ChatCompletionRequest { model: "m".into(), messages: msgs, stream: Some(false), tools: None }
    }

    /// Fake runner: records the Turn it was given, returns canned events incl. a SessionId.
    struct FakeRunner { events: Vec<AgentEvent>, seen: Arc<std::sync::Mutex<Option<Turn>>> }
    impl TurnRunner for FakeRunner {
        fn run_stream(&self, turn: Turn) -> crate::engine::EventStream {
            *self.seen.lock().unwrap() = Some(turn);
            Box::pin(futures::stream::iter(self.events.clone()))
        }
    }

    fn aggregate(events: Vec<AgentEvent>) -> crate::openai::ChatCompletionResponse {
        response_from_events(events, "m").unwrap()
    }

    #[test]
    fn response_from_events_only_text_into_content() {
        let resp = aggregate(vec![
            AgentEvent::SessionId("s".into()),
            AgentEvent::Reasoning("thinking".into()),
            AgentEvent::ToolStart { name: "Edit".into(), args: "{}".into() },
            AgentEvent::AssistantText("pong".into()),
            AgentEvent::Done { finish_reason: "stop".into() },
        ]);
        assert_eq!(resp.choices[0].message.content, "pong"); // reasoning/tool dropped from content
        assert_eq!(resp.choices[0].finish_reason, "stop");
    }

    #[tokio::test]
    async fn miss_uses_full_transcript_fresh_then_stores_advanced_key() {
        let store = Arc::new(SessionStore::new());
        let seen = Arc::new(std::sync::Mutex::new(None));
        let runner: Arc<dyn TurnRunner> = Arc::new(FakeRunner {
            events: vec![AgentEvent::SessionId("sess-X".into()), AgentEvent::AssistantText("answer one".into()), AgentEvent::Done { finish_reason: "stop".into() }],
            seen: seen.clone(),
        });
        let messages = vec![umsg("first")];
        let stream = run_request(runner, store.clone(), &entry(), &req(messages.clone()), &rt());
        let collected: Vec<_> = stream.collect().await;
        assert!(collected.iter().any(|e| matches!(e, AgentEvent::AssistantText(t) if t == "answer one")));
        // miss => fresh (no resume), full transcript fed
        let turn = seen.lock().unwrap().clone().unwrap();
        assert!(turn.resume.is_none());
        assert!(turn.user_prompt.contains("first"));
        // index advanced: the next turn (history + assistant) must now hit with session id sess-X
        let m2 = vec![umsg("first"), ChatMessage { role: Role::Assistant, content: Some(MessageContent::Text("answer one".into())), tool_call_id: None }, umsg("second")];
        let k = crate::session::lookup_key(&m2, &entry(), None, &rt());
        assert_eq!(store.get(&k), Some("sess-X".to_string()));
    }

    #[tokio::test]
    async fn hit_resumes_with_last_turn_only() {
        let store = Arc::new(SessionStore::new());
        let seen = Arc::new(std::sync::Mutex::new(None));
        // Pre-seed the index so the second turn is a hit.
        let m_prev = vec![umsg("first"), ChatMessage { role: Role::Assistant, content: Some(MessageContent::Text("answer one".into())), tool_call_id: None }];
        let k = crate::session::lookup_key(&[umsg("first"), m_prev[1].clone(), umsg("second")], &entry(), None, &rt());
        store.insert(k, "sess-prev".into());
        let runner: Arc<dyn TurnRunner> = Arc::new(FakeRunner {
            events: vec![AgentEvent::AssistantText("answer two".into()), AgentEvent::Done { finish_reason: "stop".into() }],
            seen: seen.clone(),
        });
        let messages = vec![m_prev[0].clone(), m_prev[1].clone(), umsg("second")];
        let _ = run_request(runner, store, &entry(), &req(messages), &rt()).collect::<Vec<_>>().await;
        let turn = seen.lock().unwrap().clone().unwrap();
        assert_eq!(turn.resume.as_deref(), Some("sess-prev"));
        assert_eq!(turn.user_prompt, "second");          // only the new user turn
        assert!(turn.system_prompt.is_none());            // session already holds it
    }
}
```

- [ ] **Step 2: Run, expect fail**

Run: `cargo test --lib orchestrator`
Expected: compile errors (`run_stream`, `run_request` undefined; `TurnRunner` shape changed).

- [ ] **Step 3: Rewrite `src/orchestrator.rs`** — replace the whole file (above the test module) with:

```rust
//! Turn orchestration: the streaming `TurnRunner`, the production `ClaudeProcessRunner`, the
//! session-aware `run_request` (key resolution, resume vs fresh, capture + index advancement),
//! and the events->response aggregation used by the non-streaming path.
use crate::config::ModelEntry;
use crate::engine::claude::ClaudeAdapter;
use crate::engine::{AgentEvent, Engine, EngineError, EventStream, Turn};
use crate::openai::{
    ChatCompletionRequest, ChatCompletionResponse, ChatMessage, Choice, ResponseMessage, Role, Usage,
};
use crate::process::ProcessSupervisor;
use crate::session::{lookup_key, stored_key_after, RuntimeFingerprint, SessionStore};
use crate::transcript::render_turn;
use futures::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("engine error: {0}")]
    Engine(String),
}

/// Injectable streaming run layer. Production spawns claude; tests return a canned stream.
pub trait TurnRunner: Send + Sync {
    fn run_stream(&self, turn: Turn) -> EventStream;
}

/// Production runner: builds the claude command and streams its parsed events through the shared
/// supervisor. Spawn/timeout failures surface as an `AgentEvent::Error` in the stream.
pub struct ClaudeProcessRunner {
    pub supervisor: ProcessSupervisor,
    pub claude_config_dir: Option<PathBuf>,
    pub env_passthrough: Vec<String>,
    pub timeout: Duration,
}

impl TurnRunner for ClaudeProcessRunner {
    fn run_stream(&self, turn: Turn) -> EventStream {
        let engine = Engine::Claude(ClaudeAdapter::new("claude", self.claude_config_dir.clone()));
        let (cmd, stdin) = engine.build_stream_command(&turn, &self.env_passthrough);
        let lines = self.supervisor.spawn_streaming(cmd, stdin, self.timeout);
        Box::pin(async_stream::stream! {
            futures::pin_mut!(lines);
            while let Some(item) = lines.next().await {
                match item {
                    Ok(line) => {
                        for ev in engine.parse_stream_line(&line) {
                            yield ev;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                        yield AgentEvent::Error("claude timed out".to_string());
                        return;
                    }
                    Err(e) => {
                        yield AgentEvent::Error(format!("process error: {e}"));
                        return;
                    }
                }
            }
        })
    }
}

/// Runtime fingerprint for the current process config (feeds the session key).
pub fn runtime_fingerprint(claude_config_dir: &Option<PathBuf>, sandbox_backend: &str) -> RuntimeFingerprint {
    RuntimeFingerprint {
        claude_config_dir: claude_config_dir.as_ref().map(|p| p.display().to_string()),
        sandbox_backend: sandbox_backend.to_string(),
    }
}

/// The system/developer prompt the request would run under (for the key + fresh sessions).
fn system_prompt_of(req: &ChatCompletionRequest) -> Option<String> {
    render_turn(&req.messages).system_prompt
}

/// Text of the final user message (the live instruction fed on a resume hit).
fn final_user_text(messages: &[ChatMessage]) -> String {
    messages
        .iter()
        .rev()
        .find(|m| m.role == Role::User)
        .map(|m| m.text())
        .unwrap_or_default()
}

/// Resolve the request against the session store and return a live event stream. On a HIT, resume
/// the stored claude session feeding only the new user turn; on a MISS, start fresh with the full
/// read-only transcript. As the stream runs it captures the session id and, on completion, stores
/// the advanced key so the next turn resolves to a hit.
pub fn run_request(
    runner: Arc<dyn TurnRunner>,
    sessions: Arc<SessionStore>,
    entry: &ModelEntry,
    req: &ChatCompletionRequest,
    rt: &RuntimeFingerprint,
) -> EventStream {
    let sys = system_prompt_of(req);
    let key = lookup_key(&req.messages, entry, sys.as_deref(), rt);
    let hit = sessions.get(&key);

    let rendered = render_turn(&req.messages);
    let turn = match &hit {
        Some(sid) => Turn {
            system_prompt: None, // the resumed session already holds it
            user_prompt: final_user_text(&req.messages),
            model: entry.model.clone(),
            workspace: entry.workspace.clone(),
            mode: entry.mode,
            resume: Some(sid.clone()),
        },
        None => Turn {
            system_prompt: rendered.system_prompt.clone(),
            user_prompt: rendered.user_prompt.clone(),
            model: entry.model.clone(),
            workspace: entry.workspace.clone(),
            mode: entry.mode,
            resume: None,
        },
    };

    let inner = runner.run_stream(turn);
    // Owned copies for the post-stream store update.
    let messages = req.messages.clone();
    let entry = entry.clone();
    let sys_owned = sys.clone();
    let rt_owned = rt.clone();
    let resumed_sid = hit.clone();

    Box::pin(async_stream::stream! {
        futures::pin_mut!(inner);
        let mut session_id = resumed_sid;       // hit: keep the resumed id; miss: filled by init
        let mut assistant_text = String::new();
        while let Some(ev) = inner.next().await {
            if let AgentEvent::SessionId(s) = &ev {
                session_id = Some(s.clone());
            }
            if let AgentEvent::AssistantText(t) = &ev {
                assistant_text.push_str(t);
            }
            yield ev;
        }
        if let Some(sid) = session_id {
            let advanced = stored_key_after(&messages, &assistant_text, &entry, sys_owned.as_deref(), &rt_owned);
            sessions.insert(advanced, sid);
        }
    })
}

/// Aggregate a finished event stream into one OpenAI chat.completion. Only `AssistantText` goes
/// into `content`; reasoning/tool progress is dropped in the non-streaming representation.
pub fn response_from_events(
    events: Vec<AgentEvent>,
    model_id: &str,
) -> Result<ChatCompletionResponse, EngineError> {
    let mut content = String::new();
    let mut finish_reason = "stop".to_string();
    for ev in events {
        match ev {
            AgentEvent::AssistantText(t) => content.push_str(&t),
            AgentEvent::Done { finish_reason: fr } => finish_reason = fr,
            AgentEvent::Error(m) => return Err(EngineError::Reported(m)),
            AgentEvent::Reasoning(_)
            | AgentEvent::ToolStart { .. }
            | AgentEvent::ToolResult { .. }
            | AgentEvent::SessionId(_) => {}
        }
    }
    Ok(ChatCompletionResponse {
        id: format!("chatcmpl-{}", unix_now()),
        object: "chat.completion",
        created: unix_now(),
        model: model_id.to_string(),
        choices: vec![Choice {
            index: 0,
            message: ResponseMessage { role: "assistant", content },
            finish_reason,
        }],
        usage: Usage::default(),
    })
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
```

- [ ] **Step 4: Do NOT build/commit — proceed to Task 6.**

After rewriting `orchestrator.rs` the crate will **not compile** (`src/http.rs` still calls the old
`TurnRunner::run`, and `ClaudeProcessRunner::run`/`RunError::{Timeout,Spawn}` no longer exist). This
is expected. Do not run `cargo test` or commit here. Go straight to Task 6, which rewrites `http.rs`,
removes the dead legacy code, restores a green suite, and makes the single commit for both tasks.

(Quick sanity check that the *orchestrator file itself* is internally consistent before moving on:
`cargo build 2>&1 | grep -c "src/orchestrator.rs"` — expect `0` errors attributed to
`orchestrator.rs` itself; remaining errors should all point at `src/http.rs`.)

---

## Task 6: HTTP/SSE + remove legacy (atomic switch — part 2 of 2)

> **Part 2 of the atomic switch.** Completes the trait flip: rewrites `http.rs` for SSE + aggregation
> + the new `AppState`, rewrites the integration tests, deletes the now-dead Phase-1 legacy code,
> restores a **green full suite**, and makes **one commit covering Tasks 5 + 6**.

**Files:** Modify `src/openai.rs` (chunk types), `src/http.rs`, `src/process.rs` (remove legacy `run`); rewrite `tests/http_integration.rs`; clean dead methods in `src/engine/{mod.rs,claude.rs}`.

- [ ] **Step 1: Add streaming wire types to `src/openai.rs`** (append below the existing response types, above the test module)

```rust
// ---- Streaming chunk ----

#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub object: &'static str, // "chat.completion.chunk"
    pub created: u64,
    pub model: String,
    pub choices: Vec<ChunkChoice>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChunkChoice {
    pub index: u32,
    pub delta: Delta,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Delta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}
```

- [ ] **Step 2: Write failing integration tests** — replace `tests/http_integration.rs` entirely with:

```rust
use http_body_util::BodyExt;
use llm_bridge::config::{Defaults, EngineKind, Mode, ModelEntry, ProgressChannel};
use llm_bridge::engine::{AgentEvent, EventStream, Turn};
use llm_bridge::http::{build_router, AppState};
use llm_bridge::orchestrator::TurnRunner;
use llm_bridge::registry::Registry;
use llm_bridge::session::SessionStore;
use std::sync::Arc;
use tower::ServiceExt;

struct FakeRunner { events: Vec<AgentEvent> }
impl TurnRunner for FakeRunner {
    fn run_stream(&self, _turn: Turn) -> EventStream {
        Box::pin(futures::stream::iter(self.events.clone()))
    }
}

fn state_with(token: Option<&str>, runner: Arc<dyn TurnRunner>) -> AppState {
    let models = vec![ModelEntry {
        id: "claude-text".into(), engine: EngineKind::Claude, model: Some("sonnet".into()),
        workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false,
    }];
    AppState {
        registry: Arc::new(Registry::new(models)),
        bearer_token: token.map(String::from),
        runner,
        sessions: Arc::new(SessionStore::new()),
        defaults: Defaults::default(),
        progress_channel: ProgressChannel::ReasoningContent,
        claude_config_dir: None,
    }
}
fn fake(events: Vec<AgentEvent>) -> Arc<dyn TurnRunner> { Arc::new(FakeRunner { events }) }
fn state(token: Option<&str>) -> AppState { state_with(token, fake(vec![])) }

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn health_ok() {
    let app = build_router(state(None));
    let r = app.oneshot(axum::http::Request::get("/health").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert_eq!(r.status(), 200);
}

#[tokio::test]
async fn models_lists_registered() {
    let app = build_router(state(None));
    let r = app.oneshot(axum::http::Request::get("/v1/models").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert!(body_string(r).await.contains("claude-text"));
}

#[tokio::test]
async fn auth_enforced_on_v1() {
    let app = build_router(state(Some("sk-secret-strong-token-1234")));
    let r = app.oneshot(axum::http::Request::get("/v1/models").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert_eq!(r.status(), 401);
}

#[tokio::test]
async fn nonstreaming_success() {
    let runner = fake(vec![
        AgentEvent::SessionId("s".into()),
        AgentEvent::Reasoning("think".into()),
        AgentEvent::AssistantText("pong".into()),
        AgentEvent::Done { finish_reason: "stop".into() },
    ]);
    let app = build_router(state_with(None, runner));
    let body = r#"{"model":"claude-text","messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(r.status(), 200);
    let b = body_string(r).await;
    assert!(b.contains("\"pong\""), "{b}");
    assert!(b.contains("chat.completion"));
    assert!(!b.contains("think")); // reasoning not in non-streaming content
}

#[tokio::test]
async fn streaming_emits_sse_chunks_and_done() {
    let runner = fake(vec![
        AgentEvent::Reasoning("thinking".into()),
        AgentEvent::AssistantText("po".into()),
        AgentEvent::AssistantText("ng".into()),
        AgentEvent::Done { finish_reason: "stop".into() },
    ]);
    let app = build_router(state_with(None, runner));
    let body = r#"{"model":"claude-text","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("content-type").unwrap(), "text/event-stream");
    let b = body_string(r).await;
    assert!(b.contains("chat.completion.chunk"), "{b}");
    assert!(b.contains("\"content\":\"po\""), "{b}");
    assert!(b.contains("\"reasoning_content\":\"thinking\""), "{b}");
    assert!(b.contains("\"finish_reason\":\"stop\""), "{b}");
    assert!(b.trim_end().ends_with("data: [DONE]"), "{b}");
}

#[tokio::test]
async fn streaming_omit_profile_drops_reasoning() {
    let runner = fake(vec![AgentEvent::Reasoning("secret-thoughts".into()), AgentEvent::AssistantText("hi".into()), AgentEvent::Done { finish_reason: "stop".into() }]);
    let mut st = state_with(None, runner);
    st.progress_channel = ProgressChannel::Omit;
    let app = build_router(st);
    let body = r#"{"model":"claude-text","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    let b = body_string(r).await;
    assert!(!b.contains("secret-thoughts"), "{b}");
    assert!(b.contains("\"content\":\"hi\""), "{b}");
}

#[tokio::test]
async fn unknown_model_404_and_tools_400() {
    let app = build_router(state(None));
    let r1 = app.clone().oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(r#"{"model":"nope","messages":[]}"#)).unwrap()).await.unwrap();
    assert_eq!(r1.status(), 404);
    let r2 = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(r#"{"model":"claude-text","tools":[{"x":1}],"messages":[]}"#)).unwrap()).await.unwrap();
    assert_eq!(r2.status(), 400);
}
```

- [ ] **Step 3: Run, expect fail**

Run: `cargo test --test http_integration`
Expected: compile error (`AppState` fields changed; SSE not implemented).

- [ ] **Step 4: Rewrite `src/http.rs`**

```rust
//! HTTP layer: shared state, router, health/models, bearer auth, and the chat-completions handler
//! (SSE when `stream:true`, aggregated otherwise). Both paths consume the same event stream.
use crate::config::{Defaults, ProgressChannel};
use crate::engine::AgentEvent;
use crate::openai::{
    ApiError, ChatCompletionChunk, ChatCompletionRequest, ChunkChoice, Delta,
};
use crate::orchestrator::{response_from_events, run_request, runtime_fingerprint, TurnRunner};
use crate::registry::Registry;
use crate::session::SessionStore;
use axum::{
    extract::State,
    http::{header::AUTHORIZATION, Request, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::{sse::{Event, Sse}, IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use futures::{Stream, StreamExt};
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub bearer_token: Option<String>,
    pub runner: Arc<dyn TurnRunner>,
    pub sessions: Arc<SessionStore>,
    pub defaults: Defaults,                // used for sandbox_backend in the runtime fingerprint
    pub progress_channel: ProgressChannel, // from cfg.server.progress_channel (NOT on Defaults)
    pub claude_config_dir: Option<PathBuf>,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(chat_completions))
        .route_layer(from_fn_with_state(state.clone(), auth_middleware))
        .route("/health", get(health))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn models(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.registry.models_json())
}

async fn auth_middleware(State(state): State<AppState>, req: Request<axum::body::Body>, next: Next) -> Response {
    let Some(expected) = state.bearer_token.as_deref().filter(|t| !t.is_empty()) else {
        return next.run(req).await;
    };
    let presented = req.headers().get(AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    if presented == Some(expected) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, Json(ApiError::new("missing or invalid bearer token", "invalid_request_error"))).into_response()
    }
}

async fn chat_completions(State(state): State<AppState>, Json(req): Json<ChatCompletionRequest>) -> Response {
    if req.has_tools() {
        return err(StatusCode::BAD_REQUEST, "the `tools` field is not implemented yet (Phase 4)", "invalid_request_error");
    }
    let Some(entry) = state.registry.resolve(&req.model) else {
        return err(StatusCode::NOT_FOUND, &format!("unknown model '{}'", req.model), "model_not_found");
    };
    let entry = entry.clone();
    let model_id = req.model.clone();
    let streaming = req.is_streaming();
    let progress = state.progress_channel;

    let rt = runtime_fingerprint(&state.claude_config_dir, &format!("{:?}", state.defaults.sandbox_backend).to_lowercase());
    let events = run_request(state.runner.clone(), state.sessions.clone(), &entry, &req, &rt);

    if streaming {
        sse_response(events, model_id, progress).into_response()
    } else {
        let collected: Vec<AgentEvent> = events.collect().await;
        match response_from_events(collected, &model_id) {
            Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
            Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "engine_error"),
        }
    }
}

/// Map the event stream to OpenAI `chat.completion.chunk` SSE frames.
fn sse_response(
    events: crate::engine::EventStream,
    model_id: String,
    progress: ProgressChannel,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let id = format!("chatcmpl-{}", unix_now());
    let created = unix_now();
    let mut role_sent = false;

    let stream = async_stream::stream! {
        futures::pin_mut!(events);
        while let Some(ev) = events.next().await {
            let (delta, finish) = match ev {
                AgentEvent::AssistantText(t) => {
                    let role = (!role_sent).then(|| { role_sent = true; "assistant" });
                    (Some(Delta { role, content: Some(t), reasoning_content: None }), None)
                }
                AgentEvent::Reasoning(t) | AgentEvent::ToolStart { name: t, .. } | AgentEvent::ToolResult { summary: t } => {
                    match progress {
                        ProgressChannel::ReasoningContent => (Some(Delta { role: None, content: None, reasoning_content: Some(t) }), None),
                        ProgressChannel::Omit => (None, None),
                    }
                }
                AgentEvent::Done { finish_reason } => (Some(Delta::default()), Some(finish_reason)),
                AgentEvent::Error(m) => {
                    // Surface as a final assistant note + stop; SSE has no error frame.
                    (Some(Delta { role: None, content: Some(format!("[error: {m}]")), reasoning_content: None }), Some("stop".to_string()))
                }
                AgentEvent::SessionId(_) => (None, None),
            };
            if let Some(delta) = delta {
                let chunk = ChatCompletionChunk {
                    id: id.clone(), object: "chat.completion.chunk", created, model: model_id.clone(),
                    choices: vec![ChunkChoice { index: 0, delta, finish_reason: finish.clone() }],
                };
                yield Ok(Event::default().data(serde_json::to_string(&chunk).unwrap()));
            }
        }
        yield Ok(Event::default().data("[DONE]"));
    };
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(15)))
}

fn err(status: StatusCode, message: &str, kind: &str) -> Response {
    (status, Json(ApiError::new(message, kind))).into_response()
}

fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}
```

> **Note on the `ToolStart` arm:** binding `name: t` discards `args` intentionally (progress shows the tool name; arg detail is out of Phase-2 scope). The combined match arm reuses one string binding for reasoning/tool-name/tool-summary.

> **`[DONE]` framing:** `Event::default().data("[DONE]")` serializes to `data: [DONE]\n\n`. The test asserts the body ends with `data: [DONE]` (after trim). Confirm the assertion matches actual axum SSE framing; if axum appends a trailing newline that breaks `ends_with`, adjust the test to `assert!(b.contains("data: [DONE]"))` rather than changing the handler.

- [ ] **Step 5: Run, expect pass**

Run: `cargo test --test http_integration`
Expected: `test result: ok. 7 passed`. If the `[DONE]` `ends_with` assertion fails due to trailing-newline framing, switch that one assertion to `contains` (per the note) and re-run.

- [ ] **Step 6: Remove the now-dead Phase-1 legacy code.** Nothing should reference these anymore
(the streaming path replaced them). Verify, then delete:

Run first: `grep -rn "\.run(\|parse_output\|build_command\|ProcessError" src/`
Expected references: only the *definitions* (no call sites outside their own files/tests). Then delete:
- `src/process.rs`: the `run` method, the `ProcessError` enum, the 3 `run`-based tests
  (`runs_command_and_captures_stdout`, `times_out_long_commands`, `surfaces_nonzero_exit`), and now-unused
  imports (`std::process::Output`; `thiserror`'s `Error` import if unused). Keep `spawn_streaming` + its 3
  tests + `is_cloneable_and_shares_the_permit_pool`.
- `src/engine/claude.rs`: the `build_command` and `parse_output` methods (keep `build_stream_command`,
  `parse_stream_line`, `allowlisted_env`, `normalize_finish`, `BLOCKED_TOOLS`). Drop the json
  `claude_result.json` fixture only if no test references it (the Phase-1 json parse test was already
  replaced in Task 2).
- `src/engine/mod.rs`: the `Engine::build_command` and `Engine::parse_output` dispatch methods.

If `grep` shows any *call site* (not a definition) still using these, STOP and report — the switch is
incomplete.

- [ ] **Step 7: Run the FULL suite — expect green (the switch is complete)**

Run: `cargo test && cargo clippy --all-targets 2>&1 | grep -E "warning|error" || echo CLEAN`
Expected: all tests pass (lib + http_integration); clippy CLEAN. If clippy flags an unused import or
dead-code from the legacy removal, fix it.

- [ ] **Step 8: Commit (covers the whole atomic switch, Tasks 5 + 6)**

```bash
git add src/orchestrator.rs src/openai.rs src/http.rs src/process.rs src/engine/mod.rs src/engine/claude.rs tests/http_integration.rs
git commit -m "feat: switch to streaming TurnRunner + SSE/aggregation + session resume; drop legacy json path"
```

---

## Task 7: main wiring + e2e streaming smoke + README

**Files:** Modify `src/main.rs`, `README.md`; add a test to `tests/e2e_smoke.rs`.

- [ ] **Step 1: Update `src/main.rs`** — build the `SessionStore` and pass the new `AppState` fields. Replace the `let state = AppState { ... }` block and ensure the runner is built as before:

```rust
    use llm_bridge::session::SessionStore;

    let supervisor = ProcessSupervisor::new(cfg.defaults.max_concurrency);
    let runner = Arc::new(ClaudeProcessRunner {
        supervisor,
        claude_config_dir: cfg.credentials.claude_config_dir.clone(),
        env_passthrough: cfg.defaults.env_passthrough.clone(),
        timeout: Duration::from_secs(cfg.defaults.timeout_s),
    });

    let bind = cfg.server.bind.clone();
    let state = AppState {
        registry: Arc::new(Registry::new(cfg.models.clone())),
        bearer_token: cfg.server.bearer_token.clone(),
        runner,
        sessions: Arc::new(SessionStore::new()),
        defaults: cfg.defaults.clone(),
        progress_channel: cfg.server.progress_channel,
        claude_config_dir: cfg.credentials.claude_config_dir.clone(),
    };
```

Also add `use llm_bridge::session::SessionStore;` to the imports at the top (or keep the inline `use` shown above). Keep the existing `validate_config` bail and `axum::serve`.

- [ ] **Step 2: Verify build + full suite**

Run: `cargo build && cargo test`
Expected: clean build; all tests pass (lib + http_integration). No e2e (feature off).

- [ ] **Step 3: Add a streaming e2e smoke** — append to `tests/e2e_smoke.rs` (inside the `#![cfg(feature = "e2e_smoke")]` module):

```rust
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
```

- [ ] **Step 4: Confirm e2e compiles** (do NOT run it)

Run: `cargo build --features e2e_smoke --tests`
Expected: compiles.

- [ ] **Step 5: Update `README.md` status section** — replace the "## Status (Phase 1)" block with:

````markdown
## Status (Phase 2)
- ✅ `GET /health`, `GET /v1/models`, `POST /v1/chat/completions` — **streaming (SSE) and non-streaming**
- ✅ claude engine via `--output-format stream-json`; text + agentic (`trusted_caller_only`) modes
- ✅ **Session resume** (content-hash key → `--resume`); in-memory index (restart degrades to full-transcript replay)
- ✅ progress profiles: `reasoning_content` (OpenWebUI) / `omit` (strict clients); global `max_concurrency`
- ⛔ `tools` → 400 (Phase 4); non-claude engines / `sandbox_backend` → refused at startup (Phase 3)
````

- [ ] **Step 6: Final commit**

```bash
git add src/main.rs tests/e2e_smoke.rs README.md
git commit -m "feat: wire SessionStore into main, streaming e2e smoke, README (Phase 2 complete)"
```

---

## Phase 2 Done — Definition of Done
- `cargo build` clean; `cargo test` green; `cargo clippy --all-targets` clean.
- `stream:true` returns `text/event-stream` with `chat.completion.chunk` frames (content deltas; reasoning/tool → `reasoning_content` under that profile, suppressed under `omit`) ending in `data: [DONE]`.
- Non-streaming returns one aggregated `chat.completion` (answer only).
- A multi-turn conversation **resumes** the claude session (verified: the 2nd turn's lookup key hits the index stored after turn 1; resume feeds only the new turn; miss feeds the full transcript). Restart loses the in-memory index and falls back to full-transcript replay.
- `tools` still → 400; non-claude / `sandbox_backend != none` still refused at startup.

**Next:** Phase 3 (Codex + Agy adapters, `sandbox_backend` + write/read canary probes, agy session-id/keyring spike), then Phase 4 (MCP tool bridge).
