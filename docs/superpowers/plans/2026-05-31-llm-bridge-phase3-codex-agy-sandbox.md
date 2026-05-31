# llm-bridge Phase 3 — Codex + Agy Adapters + Bubblewrap Sandbox + Canary Probes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make all three engines run behind the one Chat Completions endpoint — add the **Codex** (`codex exec --json`) and **Agy** (`agy --print`) adapters beside the existing Claude one — and add the spec's **security posture**: an external **`bubblewrap` sandbox_backend** with **write-denial + read-denial startup canary probes**, plus the **API-key-env restriction** for claude/agy agentic models.

**Architecture:** The `Engine` enum gains `Codex` and `Agy` variants; `Turn` gains an `engine` field so one generic `EngineProcessRunner` dispatches per request. Codex parses `exec --json` JSONL events (session id, reasoning, tool, agent message, completion) and resumes via `codex exec resume <id>`; Agy parses `--print` plain text (one `AssistantText` per line), is **non-streaming with resume gated OFF** (stateless full-transcript replay, single-tenant — the spec's agy-spike default). The runner appends a synthetic `Done` at clean EOF for engines that emit no terminal event, and **gates resume on `caps().resume_by_id`** so agy never resumes. A new `sandbox` module wraps any engine command in `bwrap` when `sandbox_backend: bubblewrap`, and runs the two canary probes at startup; the pure refuse/allow decision is unit-tested, the real bwrap mechanics are feature-gated. Startup validation lifts the Phase-1/2 ceiling (codex/agy now allowed; bubblewrap allowed; **`container` refused as not-yet-implemented**) and adds the env-auth restriction.

**Tech Stack:** No new crates (serde_json for codex JSONL; std for the sandbox/probe subprocess + planted-canary file). Builds on Phase 2 (tokio, axum 0.8, futures, async-stream, serde). Spec: `docs/superpowers/specs/2026-05-31-llm-bridge-design.md` (§3.2 engine caps, §4.4 adapters/events, §4.5(e) runtime fingerprint, §4.8 safety/sandbox/canary, §6 config, §9 steps 7 & 9).

**Scope note — Phase 3 of 4 (spec build-sequence steps 7 + 9).** In scope: Codex + Agy adapters, the generic per-engine runner, `sandbox_backend: bubblewrap` + write/read canary probes, env-auth restriction, validation lift, wiring, docs. **Deliberately deferred (refused or absent, with a clear message — never silently allowed):**
- **MCP `tools` bridge → Phase 4.** Request `tools` still returns `400` (unchanged from Phase 2). `caps().mcp_tools_phase1` stays `false` on every engine.
- **`sandbox_backend: container` → not implemented.** The enum value stays valid in config, but startup validation **refuses** it with "not yet implemented; use `bubblewrap` or `none`". Only `bubblewrap` is the real external backend in this phase.
- **agy native resume (`--conversation`) + agy credential isolation → gated OFF** pending the spec's agy spike (§3.2, §9, §10). Agy runs stateless full-transcript replay under the operator's own login (single-tenant). `caps(Agy).resume_by_id == false`.

---

## ⚠️ Sequencing strategy (ADDITIVE → one cross-cutting task — read before executing)

Like Phase 2, keep **every task ending on a green full `cargo test`**:

- **Task 1 is additive + a `Turn` field add.** It adds `Turn.engine`, the `Codex`/`Agy` enum variants with **stub adapters** (compile, return nothing), `caps_for`, and updates every existing `Turn` construction to set `engine: EngineKind::Claude`. Claude behaviour is unchanged; codex/agy are unreachable (validation still refuses them until Task 6).
- **Tasks 2–3 fill the stub adapters** (Codex, then Agy). Purely additive; each ends green.
- **Task 4 is the one cross-cutting change:** rename `ClaudeProcessRunner` → `EngineProcessRunner` (per-engine dispatch), add the synthetic-`Done` wrapper + resume gating, **generalize `RuntimeFingerprint.claude_config_dir` → `engine_home`**, and switch `AppState.claude_config_dir` → `AppState.credentials`. This ripples through `session.rs`, `orchestrator.rs`, `http.rs`, `main.rs`, and `tests/http_integration.rs` — all updated in this task. Treat it as one unit; end green.
- **Task 5 adds `src/sandbox.rs`** (bwrap wrapping + canary decision + real probe) and wires wrapping into the runner. Default `none` ⇒ no-op, so green throughout.
- **Task 6 rewrites validation** (lift ceiling, env-auth restriction, refuse `container`). Green.
- **Task 7 wires `main`** (per-engine runner, startup probes), updates `config.example.yaml`, README, and adds feature-gated codex/agy e2e smokes. Green.

Where a task says "replace", it means replace that specific item; only Task 4 renames shared types.

---

## Starting state (end of Phase 2)
`cargo test` → 49 passing on `master` (42 lib + 7 http_integration), clean tree. Relevant current shapes:
- `engine/mod.rs`: `pub type EventStream = Pin<Box<dyn Stream<Item=AgentEvent>+Send>>`; `Turn { system_prompt, user_prompt, model, workspace, mode, resume }` (NO `engine` field yet); `AgentEvent { AssistantText, Reasoning, ToolStart{name,args}, ToolResult{summary}, SessionId, Error, Done{finish_reason} }`; `Caps { streaming, resume_by_id, mcp_tools_phase1 }`; `enum Engine { Claude(ClaudeAdapter) }` with `caps()`, `build_stream_command(&Turn,&[String])->(Command,Option<String>)`, `parse_stream_line(&str)->Vec<AgentEvent>`.
- `engine/claude.rs`: `ClaudeAdapter::{new(bin,config_dir), build_stream_command, parse_stream_line}`, `pub(crate) fn allowlisted_env`, `fn normalize_finish`, `const BLOCKED_TOOLS`.
- `process.rs`: `ProcessSupervisor { sem }`, `spawn_streaming(cmd, stdin, timeout) -> impl Stream<Item=io::Result<String>>` (stdout lines; permit+child owned; timeout→`ErrorKind::TimedOut`). Does NOT surface exit status / stderr.
- `session.rs`: `RuntimeFingerprint { claude_config_dir: Option<String>, sandbox_backend: String }` with `fn canon`; `SessionStore { get, insert }`; `project`, `hash`, `key_for`, `lookup_key`, `stored_key_after`.
- `orchestrator.rs`: `trait TurnRunner { fn run_stream(&self, Turn) -> EventStream }`; `struct ClaudeProcessRunner { supervisor, claude_config_dir, env_passthrough, timeout }`; `fn runtime_fingerprint(&Option<PathBuf>, &str) -> RuntimeFingerprint`; `fn run_request(runner, sessions, entry, req, rt) -> EventStream`; `fn response_from_events`.
- `http.rs`: `AppState { registry, bearer_token, runner, sessions, defaults, progress_channel, claude_config_dir: Option<PathBuf> }`; `build_router`, `auth_middleware`, `chat_completions` (rejects `tools`→400, unknown model→404, SSE vs aggregate), `sse_response`.
- `config.rs`: `Credentials { claude_config_dir, codex_home, agy_config_dir }`; `EngineKind { Claude, Codex, Agy }`; `Mode { Agentic, Text }`; `SandboxBackend { None, Bubblewrap, Container }`; `ModelEntry { id, engine, model, workspace, mode, permissions, trusted_caller_only }`.
- `validate.rs`: refuses non-claude engine ("only the claude engine"), `sandbox_backend != none` ("not implemented in Phase 1"), agentic-without-trusted, agentic-without-workspace, bind/auth, workspace↔cred overlap.
- `main.rs`: builds `ClaudeProcessRunner` + `AppState` (with `claude_config_dir`).

---

## File Structure (Phase 3 changes)
```
src/lib.rs                 # + pub mod sandbox;
src/engine/mod.rs          # +pub mod codex; +pub mod agy; Turn.engine; Engine::{Codex,Agy}; caps_for(); kind(); dispatch
src/engine/codex.rs (NEW)  # CodexAdapter: build_stream_command (exec --json, resume, -s, --cd, CODEX_HOME, scrub) + tolerant JSONL parser
src/engine/agy.rs   (NEW)  # AgyAdapter: build_stream_command (--print, --add-dir, --sandbox, scrub) + plain-text parser; resume gated off
src/session.rs             # RuntimeFingerprint.claude_config_dir -> engine_home (canon + tests)
src/orchestrator.rs        # ClaudeProcessRunner -> EngineProcessRunner (per-engine dispatch); synthetic-Done wrapper; resume gating; runtime_fingerprint sig
src/sandbox.rs      (NEW)  # bwrap command wrapping (maybe_wrap) + canary_decision (pure) + run_probes (real) + ProbeOutcome
src/http.rs                # AppState.claude_config_dir -> credentials; per-engine engine_home for the fingerprint
src/validate.rs            # lift ceiling (codex/agy allowed; bubblewrap allowed; container refused); env-auth restriction
src/main.rs                # EngineProcessRunner (all cred dirs + sandbox_backend); startup canary probes when sandbox_backend != none
config.example.yaml        # + codex/agy model examples; sandbox_backend bubblewrap note; provisioning + env-auth comments
README.md                  # Status (Phase 3)
tests/fixtures/codex_stream_text.jsonl  (NEW)
tests/fixtures/codex_stream_error.jsonl (NEW)
tests/fixtures/agy_print.txt            (NEW)
tests/http_integration.rs  # AppState field rename (claude_config_dir -> credentials)
tests/e2e_smoke.rs         # + feature-gated codex & agy text smokes
```

Each `src/*.rs` keeps unit tests in `#[cfg(test)] mod tests` at the bottom. Modules filled in dependency order: engine generalization → codex → agy → runner/session → sandbox → validate → wiring.

---

## Task 1: Engine generalization — `Turn.engine`, `Codex`/`Agy` variants (stubs), `caps_for`

Adds the engine field to `Turn`, the two new enum variants backed by **stub adapters**, a per-kind `caps_for`, and dispatch. Claude is untouched. Every existing `Turn` construction gains `engine: EngineKind::Claude`.

**Files:**
- Modify: `src/engine/mod.rs`
- Create: `src/engine/codex.rs` (stub), `src/engine/agy.rs` (stub)
- Modify (Turn constructions): `src/orchestrator.rs`, `src/engine/claude.rs` (test helper), `tests/e2e_smoke.rs`

- [ ] **Step 1: Write the failing test** — append to `#[cfg(test)] mod tests` in `src/engine/mod.rs`:

```rust
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
        assert_eq!(e.caps().streaming, true);
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
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib engine`
Expected: compile error (`codex` module / `caps_for` / `kind` / `Turn.engine` not found).

- [ ] **Step 3: Create the stub `src/engine/codex.rs`** (real impl in Task 2)

```rust
//! CodexAdapter (full impl in Task 3 of the spec build sequence; Task 2 here).
use super::{AgentEvent, Turn};
use std::path::PathBuf;
use tokio::process::Command;

pub struct CodexAdapter {
    pub bin: String,
    pub codex_home: Option<PathBuf>,
}

impl CodexAdapter {
    pub fn new(bin: &str, codex_home: Option<PathBuf>) -> Self {
        CodexAdapter { bin: bin.into(), codex_home }
    }
    pub fn build_stream_command(&self, _turn: &Turn, _env_passthrough: &[String]) -> (Command, Option<String>) {
        (Command::new(&self.bin), None)
    }
    pub fn parse_stream_line(&self, _line: &str) -> Vec<AgentEvent> {
        Vec::new()
    }
}
```

- [ ] **Step 4: Create the stub `src/engine/agy.rs`** (real impl in Task 3)

```rust
//! AgyAdapter (full impl in Task 3).
use super::{AgentEvent, Turn};
use std::path::PathBuf;
use tokio::process::Command;

pub struct AgyAdapter {
    pub bin: String,
    pub config_dir: Option<PathBuf>,
}

impl AgyAdapter {
    pub fn new(bin: &str, config_dir: Option<PathBuf>) -> Self {
        AgyAdapter { bin: bin.into(), config_dir }
    }
    pub fn build_stream_command(&self, _turn: &Turn, _env_passthrough: &[String]) -> (Command, Option<String>) {
        (Command::new(&self.bin), None)
    }
    pub fn parse_stream_line(&self, _line: &str) -> Vec<AgentEvent> {
        Vec::new()
    }
}
```

- [ ] **Step 5: Edit `src/engine/mod.rs`** — declare the modules, add `Turn.engine`, the variants, `caps_for`, `kind`, and dispatch.

Replace the module declaration line `pub mod claude;` with:

```rust
pub mod agy;
pub mod claude;
pub mod codex;
```

Change the `use` line `use crate::config::Mode;` to:

```rust
use crate::config::{EngineKind, Mode};
```

Add `engine` and `permissions` to `Turn` (after `resume`):

```rust
#[derive(Debug, Clone)]
pub struct Turn {
    pub system_prompt: Option<String>,
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
```

Add the free `caps_for` function (above the `Engine` enum) and replace the `Engine` enum + its `impl` block with the three-variant version:

```rust
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
```

Leave the existing `claude_caps` test unchanged — it constructs `Engine::Claude(...)` and calls `.caps()`, which still resolves through `caps_for`.

- [ ] **Step 6: Update every existing `Turn` construction** to set `engine` and `permissions`.

In `src/engine/claude.rs` test helper `turn(...)`, add `engine: crate::config::EngineKind::Claude, permissions: None,` to the `Turn { ... }` literal.

In `src/orchestrator.rs` `run_request`, BOTH `Turn { ... }` literals (the `Some(sid)` resume arm and the `None` fresh arm) gain `engine: entry.engine, permissions: entry.permissions.clone(),`.

In `src/engine/mod.rs` test `turn_has_resume_field`, add `engine: crate::config::EngineKind::Claude, permissions: None,` to its `Turn { ... }` literal.

In `tests/e2e_smoke.rs`, BOTH `Turn { ... }` literals gain `engine: llm_bridge::config::EngineKind::Claude, permissions: None,`.

- [ ] **Step 7: Run the full suite + confirm e2e still compiles**

Run: `cargo test`
Expected: all pass (49 prior + 3 new engine tests = 52).
Run: `cargo build --features e2e_smoke --tests`
Expected: compiles.

- [ ] **Step 8: Commit**

```bash
git add src/engine/ src/orchestrator.rs tests/e2e_smoke.rs
git commit -m "feat: Turn.engine + Codex/Agy enum variants (stubs) + caps_for dispatch"
```

---

## Task 2: CodexAdapter (real) — `exec --json`, resume, sandbox levels, env scrub

Replaces the Task 1 stub. Builds `codex exec [resume <id>] --json --ignore-user-config -c shell_environment_policy.inherit=core [--model M] [-s LEVEL] [--cd WS] [--ephemeral] "<prompt>"`, scrubs the env to the allowlist, points codex at the dedicated `CODEX_HOME`, and parses the JSONL event stream.

**Fixture/parser note:** the parser is intentionally **tolerant** — it reads the event `type` at the top level *or* nested under `msg` (older codex builds wrap events as `{"id":..,"msg":{..}}`), and accepts several type aliases. The fixtures below mirror `codex exec --json` top-level typed events. If the installed codex emits token deltas instead of one final `agent_message`, re-record the fixture and add the `agent_message_delta` type to the assistant-text arm.

**Files:**
- Replace: `src/engine/codex.rs`
- Create: `tests/fixtures/codex_stream_text.jsonl`, `tests/fixtures/codex_stream_error.jsonl`

- [ ] **Step 1: Create `tests/fixtures/codex_stream_text.jsonl`**

```jsonl
{"type":"session_configured","session_id":"codex-sess-1"}
{"type":"agent_reasoning","text":"the user wants pong"}
{"type":"exec_command_begin","command":["bash","-lc","echo hi"]}
{"type":"agent_message","message":"pong"}
{"type":"task_complete"}
```

- [ ] **Step 2: Create `tests/fixtures/codex_stream_error.jsonl`**

```jsonl
{"type":"session_configured","session_id":"codex-sess-2"}
{"type":"error","message":"context window exceeded"}
```

- [ ] **Step 3: Write the failing tests** — `#[cfg(test)] mod tests` in `src/engine/codex.rs`:

```rust
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
```

- [ ] **Step 4: Run to confirm failure**

Run: `cargo test --lib codex`
Expected: assertion failures (stub returns empty / `Command::new(bin)` with no args).

- [ ] **Step 5: Write the implementation** (top of `src/engine/codex.rs`, above the test module)

```rust
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
/// for request-driven runs. Defaults to `workspace-write` (the agentic default).
fn sandbox_level(permissions: Option<&str>) -> &'static str {
    match permissions {
        Some("read-only") => "read-only",
        Some("workspace-write") | None => "workspace-write",
        Some(_) => "workspace-write",
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
                let args = ev.get("command").map(|c| c.to_string()).unwrap_or_default();
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
```

`allowlisted_env` is already `pub(crate)` in `claude.rs`, so `use crate::engine::claude::allowlisted_env;` resolves. `Turn.permissions` already exists (added in Task 1), so no struct change is needed here.

- [ ] **Step 6: Run to verify pass + full suite**

Run: `cargo test --lib codex`
Expected: 6 passed.
Run: `cargo test`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/codex.rs tests/fixtures/codex_stream_text.jsonl tests/fixtures/codex_stream_error.jsonl
git commit -m "feat: CodexAdapter (exec --json, resume, sandbox levels, env scrub) + JSONL parser"
```

---

## Task 3: AgyAdapter (real) — `--print` plain text, no resume, stateless replay

Replaces the Task 1 stub. agy is the most constrained engine: `--print` emits **plain text** (non-streaming), exposes **no config-dir / MCP / profile flag**, and its session-id capture + credential isolation are **unproven** (spec §3.2, §9). So in v1 agy: ignores `turn.resume` (always fresh, full-transcript replay — the runner gates this via `caps(Agy).resume_by_id == false`), runs under the operator's own login, and `tools` is already gated off (`caps.mcp_tools_phase1 == false`). The parser emits one `AssistantText` per output line; the runner appends the synthetic `Done` at EOF (Task 4).

**Files:**
- Replace: `src/engine/agy.rs`
- Create: `tests/fixtures/agy_print.txt`

- [ ] **Step 1: Create `tests/fixtures/agy_print.txt`**

```text
pong
second line
```

- [ ] **Step 2: Write the failing tests** — `#[cfg(test)] mod tests` in `src/engine/agy.rs`:

```rust
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
```

- [ ] **Step 3: Run to confirm failure**

Run: `cargo test --lib agy`
Expected: assertion failures (stub).

- [ ] **Step 4: Write the implementation** (top of `src/engine/agy.rs`, above the test module)

```rust
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

    /// Each non-trailing output line is a chunk of the answer. We re-append the newline that
    /// `BufReader::lines` strips, so multi-line answers keep their formatting once aggregated.
    pub fn parse_stream_line(&self, line: &str) -> Vec<AgentEvent> {
        vec![AgentEvent::AssistantText(format!("{line}\n"))]
    }
}
```

- [ ] **Step 5: Run to verify pass + full suite**

Run: `cargo test --lib agy`
Expected: 5 passed.
Run: `cargo test`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/engine/agy.rs tests/fixtures/agy_print.txt
git commit -m "feat: AgyAdapter (--print plain text, sandbox flag, resume gated off) + line parser"
```

---

## Task 4: Generic `EngineProcessRunner` — per-engine dispatch, synthetic `Done`, resume gating, generalized fingerprint

The single cross-cutting task. Renames `ClaudeProcessRunner` → `EngineProcessRunner` (dispatches per `turn.engine`), appends a synthetic `Done` at clean EOF for engines that emit no terminal event (agy), **gates resume on `caps().resume_by_id`** (so agy never resumes/stores), generalizes `RuntimeFingerprint.claude_config_dir` → `engine_home`, and switches `AppState.claude_config_dir` → `AppState.credentials`. Touches `session.rs`, `orchestrator.rs`, `http.rs`, `main.rs`, `tests/http_integration.rs`. (Sandbox wrapping is added in Task 5; this runner has no `sandbox_backend` field yet.)

**Files:** Modify `src/session.rs`, `src/orchestrator.rs`, `src/http.rs`, `src/main.rs`, `tests/http_integration.rs`.

- [ ] **Step 1: Generalize `RuntimeFingerprint` in `src/session.rs`** — rename the field and update `canon`:

```rust
/// Runtime context a native session id only makes sense within (the engine's cred/home dir +
/// sandbox backend). `engine_home` is `CLAUDE_CONFIG_DIR` / `CODEX_HOME` / agy's config context.
#[derive(Debug, Clone)]
pub struct RuntimeFingerprint {
    pub engine_home: Option<String>,
    pub sandbox_backend: String,
}

impl RuntimeFingerprint {
    fn canon(&self) -> String {
        format!("home={:?};sandbox={}", self.engine_home, self.sandbox_backend)
    }
}
```

In `src/session.rs` tests, update the `rt()` helper:

```rust
    fn rt() -> RuntimeFingerprint {
        RuntimeFingerprint { engine_home: Some("/cred".into()), sandbox_backend: "none".into() }
    }
```

- [ ] **Step 2: Run to confirm the rename breaks callers**

Run: `cargo build`
Expected: errors in `orchestrator.rs`/`http.rs` referencing `claude_config_dir` on `RuntimeFingerprint`.

- [ ] **Step 3: Rewrite the runner + helpers in `src/orchestrator.rs`.**

Update the imports block to add the engine kinds/adapters and `Credentials`:

```rust
use crate::config::{Credentials, EngineKind, ModelEntry};
use crate::engine::agy::AgyAdapter;
use crate::engine::claude::ClaudeAdapter;
use crate::engine::codex::CodexAdapter;
use crate::engine::{caps_for, AgentEvent, Engine, EngineError, EventStream, Turn};
```

Replace the `ClaudeProcessRunner` struct + its `impl TurnRunner` with `EngineProcessRunner`:

```rust
/// Production runner: dispatches per `turn.engine`, builds that engine's command, and streams its
/// parsed events through the shared supervisor. Spawn/timeout failures surface as `AgentEvent::Error`.
/// At clean EOF, if the engine emitted no terminal event (agy's plain-text path), a synthetic
/// `Done { "stop" }` is appended so the aggregator/SSE layer always sees a finish.
pub struct EngineProcessRunner {
    pub supervisor: ProcessSupervisor,
    pub credentials: Credentials,
    pub env_passthrough: Vec<String>,
    pub timeout: Duration,
}

fn engine_label(k: EngineKind) -> &'static str {
    match k {
        EngineKind::Claude => "claude",
        EngineKind::Codex => "codex",
        EngineKind::Agy => "agy",
    }
}

impl EngineProcessRunner {
    fn build_engine(&self, kind: EngineKind) -> Engine {
        match kind {
            EngineKind::Claude => Engine::Claude(ClaudeAdapter::new("claude", self.credentials.claude_config_dir.clone())),
            EngineKind::Codex => Engine::Codex(CodexAdapter::new("codex", self.credentials.codex_home.clone())),
            EngineKind::Agy => Engine::Agy(AgyAdapter::new("agy", self.credentials.agy_config_dir.clone())),
        }
    }
}

impl TurnRunner for EngineProcessRunner {
    fn run_stream(&self, turn: Turn) -> EventStream {
        let engine = self.build_engine(turn.engine);
        let (cmd, stdin) = engine.build_stream_command(&turn, &self.env_passthrough);
        let lines = self.supervisor.spawn_streaming(cmd, stdin, self.timeout);
        let kind = turn.engine;
        Box::pin(async_stream::stream! {
            futures::pin_mut!(lines);
            let mut saw_terminal = false;
            while let Some(item) = lines.next().await {
                match item {
                    Ok(line) => {
                        for ev in engine.parse_stream_line(&line) {
                            if matches!(ev, AgentEvent::Done { .. } | AgentEvent::Error(_)) {
                                saw_terminal = true;
                            }
                            yield ev;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                        yield AgentEvent::Error(format!("{} timed out", engine_label(kind)));
                        return;
                    }
                    Err(e) => {
                        yield AgentEvent::Error(format!("process error: {e}"));
                        return;
                    }
                }
            }
            if !saw_terminal {
                yield AgentEvent::Done { finish_reason: "stop".to_string() };
            }
        })
    }
}
```

Replace `runtime_fingerprint` to build the renamed field (signature unchanged, arg renamed for clarity):

```rust
/// Runtime fingerprint for the resolved engine's home dir + the active sandbox backend.
pub fn runtime_fingerprint(engine_home: &Option<PathBuf>, sandbox_backend: &str) -> RuntimeFingerprint {
    RuntimeFingerprint {
        engine_home: engine_home.as_ref().map(|p| p.display().to_string()),
        sandbox_backend: sandbox_backend.to_string(),
    }
}
```

- [ ] **Step 4: Gate resume on `caps().resume_by_id` in `run_request`.**

At the top of `run_request`, after computing `sys` and `key`, replace the `let hit = sessions.get(&key);` line with:

```rust
    let can_resume = caps_for(entry.engine).resume_by_id;
    let hit = if can_resume { sessions.get(&key) } else { None };
```

Add `let can_resume_owned = can_resume;` alongside the other owned copies (`let messages = ...` etc.), and wrap the post-stream store in the gate. Replace the trailing store block:

```rust
        if can_resume_owned {
            if let Some(sid) = session_id {
                let advanced = stored_key_after(&messages, &assistant_text, &entry, sys_owned.as_deref(), &rt_owned);
                sessions.insert(advanced, sid);
            }
        }
```

(Both `run_request` `Turn { ... }` arms already set `engine`/`permissions` from Task 1/2.)

- [ ] **Step 5: Update the `orchestrator.rs` tests** — rename the `rt()` field and add the agy gating test.

Update the test `rt()` helper:

```rust
    fn rt() -> RuntimeFingerprint {
        RuntimeFingerprint { engine_home: None, sandbox_backend: "none".into() }
    }
```

Append the agy test (the `FakeRunner` ignores the engine, so this isolates the orchestrator gating):

```rust
    #[tokio::test]
    async fn agy_never_resumes_or_stores() {
        let store = Arc::new(SessionStore::new());
        let seen = Arc::new(std::sync::Mutex::new(None));
        let agy = ModelEntry { id: "m".into(), engine: EngineKind::Agy, model: None,
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false };
        let runner: Arc<dyn TurnRunner> = Arc::new(FakeRunner {
            events: vec![AgentEvent::SessionId("ignored".into()), AgentEvent::AssistantText("a".into()), AgentEvent::Done { finish_reason: "stop".into() }],
            seen: seen.clone(),
        });
        let messages = vec![umsg("first")];
        let _ = run_request(runner, store.clone(), &agy, &req(messages), &rt()).collect::<Vec<_>>().await;
        assert!(seen.lock().unwrap().clone().unwrap().resume.is_none());
        // Even after a SessionId event, a non-resume engine stores nothing.
        let m2 = vec![umsg("first"), ChatMessage { role: Role::Assistant, content: Some(MessageContent::Text("a".into())), tool_call_id: None }, umsg("second")];
        assert_eq!(store.get(&crate::session::lookup_key(&m2, &agy, None, &rt())), None);
    }
```

Add `EngineKind` to the test module's `use crate::config::{...}` line (it currently imports `EngineKind, Mode, ModelEntry` — confirm `EngineKind` is present; add it if not).

- [ ] **Step 6: Update `src/http.rs`** — swap `claude_config_dir` for `credentials` and pick the per-engine home.

In the `use` block add the config types:

```rust
use crate::config::{Credentials, Defaults, EngineKind, ProgressChannel};
```

In `AppState`, replace the `claude_config_dir` field:

```rust
    pub credentials: Credentials, // per-engine home dirs feed the runtime fingerprint
```

In `chat_completions`, replace the `let rt = runtime_fingerprint(&state.claude_config_dir, ...)` line with a per-engine selection (place it after `let entry = entry.clone();`):

```rust
    let engine_home = match entry.engine {
        EngineKind::Claude => state.credentials.claude_config_dir.clone(),
        EngineKind::Codex => state.credentials.codex_home.clone(),
        EngineKind::Agy => state.credentials.agy_config_dir.clone(),
    };
    let rt = runtime_fingerprint(&engine_home, &format!("{:?}", state.defaults.sandbox_backend).to_lowercase());
```

- [ ] **Step 7: Update `src/main.rs`** — build `EngineProcessRunner` and pass `credentials` to `AppState`.

Change the import `use llm_bridge::orchestrator::ClaudeProcessRunner;` to `use llm_bridge::orchestrator::EngineProcessRunner;`.

Replace the runner construction:

```rust
    let runner = Arc::new(EngineProcessRunner {
        supervisor,
        credentials: cfg.credentials.clone(),
        env_passthrough: cfg.defaults.env_passthrough.clone(),
        timeout: Duration::from_secs(cfg.defaults.timeout_s),
    });
```

In the `AppState { ... }` literal, replace `claude_config_dir: cfg.credentials.claude_config_dir.clone(),` with `credentials: cfg.credentials.clone(),`.

- [ ] **Step 8: Update `tests/http_integration.rs`** — the `AppState` field rename.

Add `Credentials` to the import: `use llm_bridge::config::{Credentials, Defaults, EngineKind, Mode, ModelEntry, ProgressChannel};`. In `state_with`, replace `claude_config_dir: None,` with `credentials: Credentials::default(),`.

- [ ] **Step 9: Run the full suite + e2e compile**

Run: `cargo build && cargo test`
Expected: all pass (lib incl. the new `agy_never_resumes_or_stores`; http_integration unchanged behaviour).
Run: `cargo build --features e2e_smoke --tests`
Expected: compiles.

- [ ] **Step 10: Commit**

```bash
git add src/session.rs src/orchestrator.rs src/http.rs src/main.rs tests/http_integration.rs
git commit -m "feat: generic EngineProcessRunner (per-engine dispatch, synthetic Done, resume gating, engine_home fingerprint)"
```

---

## Task 5: `sandbox` module — bubblewrap wrapping + write/read canary probes

A new module that (a) wraps an engine command in `bwrap` so the **workspace is the only writable root** and **only the engine's own cred/home dir is readable**, and (b) runs the two startup probes. The argv builder + the refuse/allow `canary_decision` are **pure and unit-tested**; the real bwrap-executing probe is **feature-gated** (needs `bwrap` on the host). The runner gains a `sandbox_backend` field and wraps the command when it is `bubblewrap` (no-op for `none`).

**Why bind only the engine's own home RO:** the engine must read its cred dir to authenticate, but everything else — including secrets *beside* the cred dir — must stay invisible. The sandbox mounts a fresh tmpfs over `/tmp`, so a canary planted in the host temp dir is shadowed (invisible) inside; that is what the read-denial probe asserts.

**Files:**
- Create: `src/sandbox.rs`
- Modify: `src/lib.rs` (declare the module), `src/orchestrator.rs` (runner `sandbox_backend` field + wrap call), `src/main.rs` (set the field)

- [ ] **Step 1: Declare the module** — add to `src/lib.rs` (keep alphabetical):

```rust
pub mod sandbox;
```

- [ ] **Step 2: Write the failing tests** — `#[cfg(test)] mod tests` in `src/sandbox.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::path::PathBuf;

    #[test]
    fn decision_ok_only_when_both_denied() {
        assert!(canary_decision(&ProbeOutcome { write_denied: true, read_denied: true }).is_ok());
        let e = canary_decision(&ProbeOutcome { write_denied: false, read_denied: true }).unwrap_err();
        assert!(e.contains("write"), "{e}");
        let e = canary_decision(&ProbeOutcome { write_denied: true, read_denied: false }).unwrap_err();
        assert!(e.contains("read"), "{e}");
    }

    #[test]
    fn argv_binds_workspace_rw_cred_ro_and_ends_with_inner() {
        let ws = PathBuf::from("/work/repoB");
        let cred = PathBuf::from("/cred/codex");
        let program = OsString::from("codex");
        let argv = bwrap_argv(
            Some(ws.as_path()),
            &[cred.clone()],
            program.as_os_str(),
            &[OsString::from("exec"), OsString::from("--json")],
            Some(ws.as_path()),
        );
        let s: Vec<String> = argv.iter().map(|a| a.to_string_lossy().into_owned()).collect();
        // workspace is the only writable root
        assert!(s.windows(3).any(|w| w == ["--bind", "/work/repoB", "/work/repoB"]));
        // cred dir is read-only
        assert!(s.windows(3).any(|w| w == ["--ro-bind", "/cred/codex", "/cred/codex"]));
        // tmpfs shadows /tmp (read-denial)
        assert!(s.windows(2).any(|w| w == ["--tmpfs", "/tmp"]));
        // the inner command follows the `--` separator, in order
        let dd = s.iter().position(|a| a == "--").expect("-- present");
        assert_eq!(&s[dd + 1..], &["codex", "exec", "--json"]);
    }

    #[test]
    fn maybe_wrap_is_noop_for_none() {
        let inner = tokio::process::Command::new("codex");
        let wrapped = maybe_wrap(crate::config::SandboxBackend::None, None, &[], inner);
        assert_eq!(wrapped.as_std().get_program().to_string_lossy(), "codex");
    }

    #[test]
    fn maybe_wrap_uses_bwrap_for_bubblewrap() {
        let inner = tokio::process::Command::new("codex");
        let ws = PathBuf::from("/work/x");
        let wrapped = maybe_wrap(crate::config::SandboxBackend::Bubblewrap, Some(ws.as_path()), &[], inner);
        assert_eq!(wrapped.as_std().get_program().to_string_lossy(), "bwrap");
    }
}
```

- [ ] **Step 3: Run to confirm failure**

Run: `cargo test --lib sandbox`
Expected: compile error (module empty).

- [ ] **Step 4: Write the implementation** (top of `src/sandbox.rs`)

```rust
//! External sandbox backend (`bubblewrap`) and the startup canary probes (spec §4.8). Wrapping
//! makes the workspace the only writable root and exposes ONLY the engine's own cred/home dir
//! read-only; a fresh tmpfs over `/tmp` shadows any planted secret. The pure argv builder +
//! `canary_decision` are unit-tested; `run_probes` shells out to real `bwrap` (feature-gated test).
use crate::config::SandboxBackend;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

/// Result of the two startup probes against a model's effective sandbox.
#[derive(Debug, Clone, Copy)]
pub struct ProbeOutcome {
    pub write_denied: bool,
    pub read_denied: bool,
}

/// Refuse unless BOTH the write- and read-denial probes passed (the path is denied). Pure.
pub fn canary_decision(o: &ProbeOutcome) -> Result<(), String> {
    let mut bad = Vec::new();
    if !o.write_denied {
        bad.push("write-denial probe FAILED (a write outside the workspace succeeded)");
    }
    if !o.read_denied {
        bad.push("read-denial probe FAILED (the sandboxed process read a planted secret)");
    }
    if bad.is_empty() { Ok(()) } else { Err(bad.join("; ")) }
}

/// Build the argv that follows `bwrap` (system RO binds, tmpfs /tmp, workspace RW, cred dirs RO,
/// then `-- <program> <args...>`). Pure so it is unit-testable without executing anything.
pub(crate) fn bwrap_argv(
    workspace: Option<&Path>,
    ro_paths: &[PathBuf],
    inner_program: &OsStr,
    inner_args: &[OsString],
    cwd: Option<&Path>,
) -> Vec<OsString> {
    let mut a: Vec<OsString> = Vec::new();
    let mut push = |s: &str| a.push(OsString::from(s));
    push("--die-with-parent");
    push("--unshare-all");
    push("--share-net"); // engines need network to reach the model vendor
    push("--proc");
    push("/proc");
    push("--dev");
    push("/dev");
    push("--tmpfs");
    push("/tmp"); // shadow host /tmp -> read-denial of anything planted there
    for sys in ["/usr", "/bin", "/sbin", "/lib", "/lib64", "/etc"] {
        if Path::new(sys).exists() {
            a.push(OsString::from("--ro-bind"));
            a.push(OsString::from(sys));
            a.push(OsString::from(sys));
        }
    }
    if let Some(ws) = workspace {
        a.push(OsString::from("--bind"));
        a.push(ws.as_os_str().to_os_string());
        a.push(ws.as_os_str().to_os_string());
    }
    for ro in ro_paths {
        if ro.exists() {
            a.push(OsString::from("--ro-bind"));
            a.push(ro.as_os_str().to_os_string());
            a.push(ro.as_os_str().to_os_string());
        }
    }
    if let Some(dir) = cwd {
        a.push(OsString::from("--chdir"));
        a.push(dir.as_os_str().to_os_string());
    }
    a.push(OsString::from("--"));
    a.push(inner_program.to_os_string());
    a.extend(inner_args.iter().cloned());
    a
}

/// Wrap a built engine command in `bwrap`, preserving its program/args/cwd/env.
fn wrap_bubblewrap(inner: tokio::process::Command, workspace: Option<&Path>, ro_paths: &[PathBuf]) -> tokio::process::Command {
    let std_inner = inner.as_std();
    let program = std_inner.get_program().to_os_string();
    let inner_args: Vec<OsString> = std_inner.get_args().map(|a| a.to_os_string()).collect();
    let cwd = std_inner.get_current_dir().map(|p| p.to_path_buf());
    let envs: Vec<(OsString, Option<OsString>)> =
        std_inner.get_envs().map(|(k, v)| (k.to_os_string(), v.map(|v| v.to_os_string()))).collect();

    let argv = bwrap_argv(workspace, ro_paths, &program, &inner_args, cwd.as_deref());

    let mut bw = tokio::process::Command::new("bwrap");
    bw.args(argv);
    bw.env_clear();
    for (k, v) in envs {
        if let Some(v) = v {
            bw.env(k, v);
        }
    }
    bw
}

/// Wrap the command for the configured backend. `none` (and the not-yet-implemented `container`,
/// which startup validation refuses) return the command unchanged.
pub fn maybe_wrap(
    backend: SandboxBackend,
    workspace: Option<&Path>,
    ro_paths: &[PathBuf],
    cmd: tokio::process::Command,
) -> tokio::process::Command {
    match backend {
        SandboxBackend::Bubblewrap => wrap_bubblewrap(cmd, workspace, ro_paths),
        SandboxBackend::None | SandboxBackend::Container => cmd,
    }
}

/// Run the two real bwrap canary probes against a model's effective sandbox. Called at startup
/// (main) when `sandbox_backend != none`. A bwrap spawn failure surfaces as `Err` so startup refuses.
pub fn run_probes(workspace: Option<&Path>, ro_paths: &[PathBuf]) -> std::io::Result<ProbeOutcome> {
    // write-denial: writing into a RO-bound system path must fail.
    let write_denied = !run_wrapped(workspace, ro_paths, "echo probe > /etc/llm-bridge-write-probe 2>/dev/null")?;

    // read-denial: plant a secret in the host temp dir (shadowed by the sandbox tmpfs), then try
    // to read it from inside the sandbox; it must be invisible.
    let secret = std::env::temp_dir().join(format!("llm-bridge-canary-{}", std::process::id()));
    std::fs::write(&secret, b"TOP-SECRET-CANARY")?;
    let script = format!("cat '{}' 2>/dev/null", secret.display());
    let read_result = run_wrapped(workspace, ro_paths, &script);
    let _ = std::fs::remove_file(&secret);
    let read_denied = !read_result?;

    Ok(ProbeOutcome { write_denied, read_denied })
}

/// Run `sh -c <script>` inside the bubblewrap sandbox; return whether it SUCCEEDED (exit 0).
fn run_wrapped(workspace: Option<&Path>, ro_paths: &[PathBuf], script: &str) -> std::io::Result<bool> {
    let argv = bwrap_argv(
        workspace,
        ro_paths,
        OsStr::new("sh"),
        &[OsString::from("-c"), OsString::from(script)],
        None,
    );
    let status = std::process::Command::new("bwrap")
        .args(argv)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .status()?;
    Ok(status.success())
}
```

- [ ] **Step 5: Run to verify the pure tests pass**

Run: `cargo test --lib sandbox`
Expected: 4 passed (the real-probe test is feature-gated and not built here).

- [ ] **Step 6: Wire wrapping into the runner** — `src/orchestrator.rs`.

Add `SandboxBackend` to the config import: `use crate::config::{Credentials, EngineKind, ModelEntry, SandboxBackend};`.

Add the field to `EngineProcessRunner`:

```rust
    pub sandbox_backend: SandboxBackend,
```

In `run_stream`, after `let (cmd, stdin) = engine.build_stream_command(&turn, &self.env_passthrough);`, insert the per-engine RO home + wrap:

```rust
        // Expose ONLY the resolved engine's own home dir read-only inside the sandbox.
        let engine_home = match turn.engine {
            EngineKind::Claude => self.credentials.claude_config_dir.clone(),
            EngineKind::Codex => self.credentials.codex_home.clone(),
            EngineKind::Agy => self.credentials.agy_config_dir.clone(),
        };
        let ro_paths: Vec<PathBuf> = engine_home.into_iter().collect();
        let cmd = crate::sandbox::maybe_wrap(self.sandbox_backend, turn.workspace.as_deref(), &ro_paths, cmd);
```

- [ ] **Step 7: Set the field in `src/main.rs`** — add to the `EngineProcessRunner { ... }` construction:

```rust
        sandbox_backend: cfg.defaults.sandbox_backend,
```

- [ ] **Step 8: Run the full suite + e2e compile**

Run: `cargo build && cargo test`
Expected: all pass.
Run: `cargo build --features e2e_smoke --tests`
Expected: compiles.

- [ ] **Step 9: Add the feature-gated real-probe test** — append to `#[cfg(test)] mod tests` in `src/sandbox.rs`:

```rust
    // Real bwrap probe — needs `bwrap` on the host. Run with:
    //   cargo test --features e2e_smoke --lib sandbox::tests::real_bwrap_denies_write_and_read -- --nocapture
    #[cfg(feature = "e2e_smoke")]
    #[test]
    fn real_bwrap_denies_write_and_read() {
        let ws = std::env::temp_dir().join(format!("llm-bridge-ws-{}", std::process::id()));
        std::fs::create_dir_all(&ws).unwrap();
        let outcome = run_probes(Some(&ws), &[]).expect("bwrap ran");
        let _ = std::fs::remove_dir_all(&ws);
        assert!(outcome.write_denied, "write must be denied outside the workspace");
        assert!(outcome.read_denied, "planted secret must be unreadable inside the sandbox");
        assert!(canary_decision(&outcome).is_ok());
    }
```

Run: `cargo build --features e2e_smoke --lib` (compiles; do not run the gated test in CI).

- [ ] **Step 10: Commit**

```bash
git add src/sandbox.rs src/lib.rs src/orchestrator.rs src/main.rs
git commit -m "feat: bubblewrap sandbox wrapping + write/read canary probes (pure decision + real probe)"
```

---

## Task 6: Validation rewrite — lift the ceiling, env-auth restriction, refuse `container`

Replaces the Phase-1/2 ceiling. Codex and Agy engines are now allowed; `bubblewrap` is allowed (the real canary probes run at startup in `main`, Task 7); `container` is **refused as not-yet-implemented**. Adds the **API-key-env restriction**: a claude/agy *agentic* model with no `sandbox_backend` must not pass an API-key var through `env_passthrough` (it would leak into model-run tool shells; only codex scrubs). When `sandbox_backend` is set, `trusted_caller_only` is no longer required (the sandbox provides confinement, verified by the startup probes).

**Files:** Modify `src/validate.rs`.

- [ ] **Step 1: Write the failing tests** — update `#[cfg(test)] mod tests` in `src/validate.rs`.

Replace `sandbox_backend_other_than_none_is_refused_in_phase1` and `non_claude_engine_is_refused_in_phase1` with these, and add the new ones:

```rust
    fn codex_agentic(ws: &str, trusted: bool) -> ModelEntry {
        ModelEntry { id: "c".into(), engine: EngineKind::Codex, model: Some("gpt-5".into()),
            workspace: Some(PathBuf::from(ws)), mode: Mode::Agentic, permissions: Some("workspace-write".into()),
            trusted_caller_only: trusted }
    }

    #[test]
    fn codex_and_agy_engines_are_allowed() {
        let mut cfg = base();
        cfg.models = vec![
            ModelEntry { id: "ct".into(), engine: EngineKind::Codex, model: None, workspace: None,
                mode: Mode::Text, permissions: None, trusted_caller_only: false },
            ModelEntry { id: "at".into(), engine: EngineKind::Agy, model: None, workspace: None,
                mode: Mode::Text, permissions: None, trusted_caller_only: false },
        ];
        assert!(validate_config(&cfg).is_ok(), "{:?}", validate_config(&cfg));
    }

    #[test]
    fn codex_agentic_without_trusted_and_no_sandbox_is_refused() {
        let mut cfg = base();
        cfg.models = vec![codex_agentic("/work/repoB", false)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("trusted_caller_only")), "{err:?}");
    }

    #[test]
    fn bubblewrap_backend_is_allowed_and_drops_trusted_requirement() {
        let mut cfg = base();
        cfg.defaults.sandbox_backend = SandboxBackend::Bubblewrap;
        cfg.models = vec![agentic("/work/repoA", false)]; // not trusted, but sandboxed -> OK
        assert!(validate_config(&cfg).is_ok(), "{:?}", validate_config(&cfg));
    }

    #[test]
    fn container_backend_is_refused_as_not_implemented() {
        let mut cfg = base();
        cfg.defaults.sandbox_backend = SandboxBackend::Container;
        cfg.models = vec![agentic("/work/repoA", true)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("container") && m.contains("not yet implemented")), "{err:?}");
    }

    #[test]
    fn claude_agentic_with_api_key_env_and_no_sandbox_is_refused() {
        let mut cfg = base();
        cfg.defaults.env_passthrough = vec!["PATH".into(), "ANTHROPIC_API_KEY".into()];
        cfg.models = vec![agentic("/work/repoA", true)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("API-key env")), "{err:?}");
    }

    #[test]
    fn codex_agentic_with_api_key_env_is_allowed() {
        // codex scrubs via shell_environment_policy, so env-key auth is fine for it.
        let mut cfg = base();
        cfg.defaults.env_passthrough = vec!["PATH".into(), "OPENAI_API_KEY".into()];
        cfg.models = vec![codex_agentic("/work/repoB", true)];
        assert!(validate_config(&cfg).is_ok(), "{:?}", validate_config(&cfg));
    }

    #[test]
    fn claude_api_key_env_with_bubblewrap_is_allowed() {
        let mut cfg = base();
        cfg.defaults.sandbox_backend = SandboxBackend::Bubblewrap;
        cfg.defaults.env_passthrough = vec!["PATH".into(), "ANTHROPIC_API_KEY".into()];
        cfg.models = vec![agentic("/work/repoA", false)];
        assert!(validate_config(&cfg).is_ok(), "{:?}", validate_config(&cfg));
    }
```

Keep the existing `agentic_without_trusted_is_refused`, `agentic_with_trusted_is_ok`, bind/auth, and overlap tests unchanged.

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib validate`
Expected: failures (old ceiling still rejects codex/bubblewrap; new messages absent).

- [ ] **Step 3: Rewrite the validation body in `src/validate.rs`.**

Update the module doc comment first line to:

```rust
//! Startup validation. Returns Err(Vec<message>) listing every problem at once. Phase 3 allows all
//! three engines + the bubblewrap sandbox; the real write/read canary probes run at startup in main.
```

Replace the `// Phase-1 ceiling` block with the container-only refusal:

```rust
    // bubblewrap is implemented; container is not yet (the enum value stays valid in config).
    if cfg.defaults.sandbox_backend == SandboxBackend::Container {
        errs.push(
            "defaults.sandbox_backend=container is not yet implemented; use `bubblewrap` or `none`".to_string(),
        );
    }
    let sandboxed = cfg.defaults.sandbox_backend != SandboxBackend::None;
```

Replace the per-model loop body (the `for m in &cfg.models { ... }` interior) with:

```rust
    for m in &cfg.models {
        // (All three engines are supported in Phase 3 — no engine refusal here.)

        if m.mode == Mode::Agentic {
            if !sandboxed && !m.trusted_caller_only {
                errs.push(format!(
                    "model '{}': agentic models require trusted_caller_only: true when sandbox_backend is none (no native sandbox passes the read-denial probe)",
                    m.id
                ));
            }
            if m.workspace.is_none() {
                errs.push(format!("model '{}': agentic models require a workspace", m.id));
            }
            // API-key env auth leaks into model-run tool shells for claude/agy (only codex scrubs);
            // disallow it for those engines unless an external sandbox contains the leak.
            if !sandboxed && matches!(m.engine, EngineKind::Claude | EngineKind::Agy) {
                let leaked: Vec<&str> = api_key_vars(m.engine)
                    .iter()
                    .copied()
                    .filter(|k| cfg.defaults.env_passthrough.iter().any(|p| p == k))
                    .collect();
                if !leaked.is_empty() {
                    errs.push(format!(
                        "model '{}': API-key env auth ({}) is disallowed for a {:?} agentic model without a sandbox_backend (it leaks to model-run tool shells); use file-based login or set sandbox_backend",
                        m.id, leaked.join(", "), m.engine
                    ));
                }
            }
        }

        if let Some(ws) = &m.workspace {
            let ws_r = resolve_path(ws);
            for cred in &cred_dirs {
                if paths_overlap(&ws_r, &resolve_path(cred)) {
                    errs.push(format!(
                        "model '{}': workspace {} overlaps credential dir {}",
                        m.id, ws.display(), cred.display()
                    ));
                }
            }
        }
    }
```

Add the helper (below `validate_config`, near `is_loopback_bind`):

```rust
/// API-key env vars that leak into model-run tool shells. codex scrubs via shell_environment_policy
/// (so it is exempt); claude/agy expose no equivalent.
fn api_key_vars(engine: EngineKind) -> &'static [&'static str] {
    match engine {
        EngineKind::Claude => &["ANTHROPIC_API_KEY"],
        EngineKind::Agy => &["GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_APPLICATION_CREDENTIALS"],
        EngineKind::Codex => &[],
    }
}
```

(`EngineKind` is already imported at the top of `validate.rs`.)

- [ ] **Step 4: Run to verify pass + full suite**

Run: `cargo test --lib validate`
Expected: all validate tests pass.
Run: `cargo test`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/validate.rs
git commit -m "feat: lift engine/sandbox ceiling; refuse container; API-key-env restriction for claude/agy agentic"
```

---

## Task 7: main startup probes + config example + README + e2e smokes

Wires the real canary probes into startup (after `validate_config`, before serving), refreshes the config example and README, and adds feature-gated codex/agy text smokes.

**Files:** Modify `src/main.rs`, `config.example.yaml`, `README.md`; add to `tests/e2e_smoke.rs`.

- [ ] **Step 1: Add the startup canary probes to `src/main.rs`.**

Change the config import to bring in the kinds used below:

```rust
use llm_bridge::config::{load_config, EngineKind, SandboxBackend};
```

Add `use std::path::PathBuf;` to the imports.

Immediately AFTER the `validate_config` bail block and BEFORE building the supervisor, insert:

```rust
    // For an external sandbox, prove the posture before serving: each agentic model's effective
    // sandbox must DENY both a write outside the workspace and a read of a planted secret (spec §4.8).
    if cfg.defaults.sandbox_backend != SandboxBackend::None {
        for m in cfg.models.iter().filter(|m| m.mode == llm_bridge::config::Mode::Agentic) {
            let home = match m.engine {
                EngineKind::Claude => cfg.credentials.claude_config_dir.clone(),
                EngineKind::Codex => cfg.credentials.codex_home.clone(),
                EngineKind::Agy => cfg.credentials.agy_config_dir.clone(),
            };
            let ro: Vec<PathBuf> = home.into_iter().collect();
            let outcome = llm_bridge::sandbox::run_probes(m.workspace.as_deref(), &ro)
                .map_err(|e| anyhow::anyhow!("model '{}': sandbox canary probe failed to run ({e}); is bwrap installed?", m.id))?;
            if let Err(why) = llm_bridge::sandbox::canary_decision(&outcome) {
                anyhow::bail!("model '{}': sandbox_backend posture rejected — {}", m.id, why);
            }
            tracing::info!("model '{}': sandbox canary probes passed (write + read denied)", m.id);
        }
    }
```

- [ ] **Step 2: Verify build + full suite**

Run: `cargo build && cargo test`
Expected: clean build; all pass.

- [ ] **Step 3: Update `config.example.yaml`** — replace the whole file with the three-engine example:

```yaml
server:
  bind: "127.0.0.1:8088"          # non-loopback bind requires a strong, non-default bearer_token
  bearer_token: "sk-change-me"    # CHANGE THIS for any non-loopback bind (validation rejects placeholders)
  progress_channel: reasoning_content
defaults:
  timeout_s: 600
  max_concurrency: 4
  sandbox_backend: none           # none | bubblewrap (implemented) | container (NOT yet implemented).
                                  # `bubblewrap` requires `bwrap` on the host and is validated at startup
                                  # by write- + read-denial canary probes; it lets agentic models run
                                  # WITHOUT trusted_caller_only.
  env_passthrough: ["PATH", "HOME", "LANG", "LC_ALL", "TERM"]   # all other env vars are scrubbed.
                                  # Do NOT add ANTHROPIC_API_KEY / GEMINI_API_KEY for a claude/agy
                                  # AGENTIC model unless sandbox_backend is set (it leaks to tool shells).
credentials:                      # dedicated, persistent, operator-provisioned per-engine dirs
  claude_config_dir: ~/.llm-bridge/cred/claude   # CLAUDE_CONFIG_DIR; provision: CLAUDE_CONFIG_DIR=… claude login
  codex_home: ~/.llm-bridge/cred/codex           # CODEX_HOME (persistent → enables resume); CODEX_HOME=… codex login
  agy_config_dir: ~/.llm-bridge/cred/gemini      # UNPROVEN: agy has no config-dir flag + uses the OS keyring;
                                                 # in v1 agy runs under the operator's own login (single-tenant)
models:
  - id: "claude-opus-repoA"
    engine: claude
    model: opus
    workspace: /work/repoA
    mode: agentic
    permissions: workspace-write
    trusted_caller_only: true     # REQUIRED for agentic when sandbox_backend: none
  - id: "codex-gpt5-repoB"
    engine: codex
    model: gpt-5
    workspace: /work/repoB
    mode: agentic
    permissions: workspace-write  # codex confines WRITES natively, but still reads the cred dir,
    trusted_caller_only: true     # so it also needs trusted_caller_only (or a sandbox_backend)
  - id: "agy-gemini-repoC"
    engine: agy
    model: gemini-2.5-pro
    workspace: /work/repoC
    mode: agentic
    permissions: workspace-write  # agy: coarse boolean sandbox; stateless replay (no resume) in v1
    trusted_caller_only: true
  - id: "claude-sonnet-text"
    engine: claude
    model: sonnet
    mode: text                    # PoC-style pure generator: all tools blocked
```

- [ ] **Step 4: Update the `README.md` Status section** — replace the "## Status (Phase 2)" block with:

````markdown
## Status (Phase 3)
- ✅ `GET /health`, `GET /v1/models`, `POST /v1/chat/completions` — streaming (SSE) and non-streaming
- ✅ **Three engines**: `claude` (stream-json), `codex` (`exec --json`, sandbox levels, persistent `CODEX_HOME`), `agy` (`--print`, stateless replay)
- ✅ **Session resume** for claude + codex (content-hash key → engine resume); **agy is stateless** (resume gated off pending the agy spike — single-tenant, operator login)
- ✅ **`sandbox_backend: bubblewrap`** — workspace-only writes + cred-dir-only reads, validated at startup by **write- + read-denial canary probes**; agentic models then run without `trusted_caller_only`
- ✅ API-key-env restriction (claude/agy agentic without a sandbox refuse a passed API key); progress profiles; global `max_concurrency`
- ⛔ `tools` → 400 (Phase 4); `sandbox_backend: container` → refused at startup (not yet implemented)
````

Also update the opening line (`OpenAI Chat Completions–compatible HTTP shim over the \`claude\` CLI (Phase 1).`) to:

```markdown
OpenAI Chat Completions–compatible HTTP shim over the `claude`, `codex`, and `agy` agentic CLIs.
```

- [ ] **Step 5: Add feature-gated codex + agy smokes** — append to `tests/e2e_smoke.rs` (inside the `#![cfg(feature = "e2e_smoke")]` module):

```rust
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
        engine: EngineKind::Codex, permissions: None,
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
        engine: EngineKind::Agy, permissions: None,
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

- [ ] **Step 6: Confirm e2e + sandbox feature compile** (do NOT run them)

Run: `cargo build --features e2e_smoke --tests --lib`
Expected: compiles.

- [ ] **Step 7: Clippy + full suite**

Run: `cargo clippy --all-targets && cargo test`
Expected: clippy clean; all tests pass.

- [ ] **Step 8: Final commit**

```bash
git add src/main.rs config.example.yaml README.md tests/e2e_smoke.rs
git commit -m "feat: startup canary probes + 3-engine config example, README, codex/agy e2e smokes (Phase 3 complete)"
```

---

## Phase 3 Done — Definition of Done
- `cargo build` clean; `cargo test` green; `cargo clippy --all-targets` clean.
- **All three engines run** behind `/v1/chat/completions`: claude (stream-json), codex (`exec --json`), agy (`--print`), selected by the model id; streaming and non-streaming both work.
- **Codex resumes** (content-hash key → `codex exec resume <id>`); **claude resumes** (unchanged); **agy never resumes** — `caps(Agy).resume_by_id == false`, so it always replays the full transcript and stores no key (verified by `agy_never_resumes_or_stores`).
- **`sandbox_backend: bubblewrap`** wraps every engine command (workspace = only writable root; only the engine's own home dir readable); startup runs the **write- + read-denial canary probes** and **refuses to start** if either is not denied. `container` is refused as not-yet-implemented.
- Agentic models with `sandbox_backend: none` still require `trusted_caller_only`; with a sandbox they do not. **API-key env auth** is refused for claude/agy agentic without a sandbox.
- `tools` still → `400` (Phase 4 owns the MCP bridge). MCP injection / suspended tool-call groups are NOT in this phase.
- The **agy spike** (`--conversation` resume, keyring/`~/.gemini` credential isolation) remains an open, documented investigation; agy stays stateless-replay + single-tenant until it is proven.

**Next:** Phase 4 (MCP tool bridge: per-process injection for claude `--mcp-config --strict-mcp-config` / codex `-c mcp_servers.*`, turn-level suspended `tool_call` groups, `tool_result_timeout_s` / `max_suspended_sessions`).
