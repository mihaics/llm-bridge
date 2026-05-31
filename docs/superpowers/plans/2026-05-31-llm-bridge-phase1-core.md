# llm-bridge Phase 1 — Core + Claude Non-Streaming Shim — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A running, config-validated, bearer-authed **OpenAI Chat Completions–compatible** HTTP server that answers `POST /v1/chat/completions` (non-streaming) by shelling out to the `claude` CLI, plus `GET /v1/models` and `GET /health`. This is the direct successor to the Python PoC (`autoid/autoid-ai-poc/scripts/claude_shim.py`), usable from OpenWebUI/LiteLLM.

**Architecture:** Single Rust binary. A YAML config defines a model registry; startup validation refuses unsafe/inconsistent model postures. Each request resolves `model` → a `ModelEntry`, renders the conversation into a read-only transcript prompt, and runs `claude -p --output-format json` through a shared `TurnRunner` (one global `ProcessSupervisor` enforcing `max_concurrency`). The spawned `claude` gets a **scrubbed environment** (`env_clear` + an allowlist) so model-run tools can't read service secrets; **text mode** blocks all tools (pure generator), **agentic mode** runs in the workspace with bypassed permissions, gated by `trusted_caller_only`. The single JSON result is parsed and returned as one OpenAI `chat.completion`. Sessions are **stateless full-transcript replay** in this phase.

**Tech Stack:** Rust (1.93), tokio, axum 0.8, serde/serde_json, serde_yaml, tower (test), http-body-util (test), tracing. Spec: `docs/superpowers/specs/2026-05-31-llm-bridge-design.md`.

**Scope note — this is Phase 1 of 4.** Later plans: (2) SSE streaming + session store/resume; (3) Codex + Agy adapters + `sandbox_backend` + canary probes; (4) MCP tool bridge. Phase 1 deliberately implements only what makes a working claude shim; it **refuses** (does not silently allow) anything those phases own — `sandbox_backend != none`, `stream: true`, non-claude engines, and request `tools` all return a clear error.

---

## File Structure

```
Cargo.toml
src/
  main.rs          # entrypoint: load config, validate, build runner+router, serve
  lib.rs           # module declarations (created complete in Task 0)
  openai.rs        # OpenAI Chat Completions wire types (serde)
  config.rs        # Config schema + YAML loader (+ tilde expansion)
  registry.rs      # Registry: resolve model id -> ModelEntry; /v1/models JSON; fingerprint
  validate.rs      # startup validation (bind/auth, agentic safety, path overlap)
  transcript.rs    # render conversation -> read-only transcript prompt
  process.rs       # ProcessSupervisor: spawn + stdin + timeout + global concurrency
  engine/
    mod.rs         # AgentEvent, Turn, Caps, EngineError, Engine enum dispatch
    claude.rs      # ClaudeAdapter: build_command (+ env scrub) + parse_output
  orchestrator.rs  # TurnRunner trait, RunError, ClaudeProcessRunner, turn_from_request, response_from_events
  http.rs          # AppState, router, /health, /v1/models, bearer auth, chat_completions handler
tests/
  fixtures/claude_result.json   # recorded `claude --output-format json` object
  http_integration.rs           # end-to-end router tests with a FakeRunner (hermetic)
  e2e_smoke.rs                  # feature-gated real-claude smoke test
config.example.yaml
README.md
```

Each `src/*.rs` keeps its unit tests in a `#[cfg(test)] mod tests` at the bottom of the same file. Cross-file/router tests live in `tests/`. Modules are **filled in dependency order** (openai → config → registry → validate → engine → transcript → process → claude → orchestrator → http → main); Task 0 creates them all as empty stubs so the crate compiles after every task.

---

## Task 0: Project scaffold (all module stubs)

**Files:**
- Create: `Cargo.toml`, `src/main.rs`, `src/lib.rs`, and **empty** stub files for every module.

- [ ] **Step 1: Create `Cargo.toml`**

```toml
[package]
name = "llm-bridge"
version = "0.1.0"
edition = "2021"
rust-version = "1.93"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "process", "time", "io-util", "sync", "net", "signal"] }
axum = "0.8"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde_yaml = "0.9"            # NOTE: archived but functional on 1.93; swap to serde_yml later if needed
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[dev-dependencies]
tower = { version = "0.5", features = ["util"] }   # ServiceExt::oneshot for router tests
http-body-util = "0.1"                              # collect response bodies in tests

[features]
# e2e_smoke shells out to a real installed `claude`; off by default so CI/`cargo test` stays hermetic
e2e_smoke = []
```

- [ ] **Step 2: Create `src/lib.rs`** (complete; all modules declared)

```rust
pub mod config;
pub mod engine;
pub mod http;
pub mod openai;
pub mod orchestrator;
pub mod process;
pub mod registry;
pub mod transcript;
pub mod validate;
```

- [ ] **Step 3: Create empty stub files so the crate compiles**

Create these as **empty files** (zero bytes), except `engine/mod.rs` which must declare its submodule:

```bash
mkdir -p src/engine tests/fixtures
: > src/openai.rs
: > src/config.rs
: > src/registry.rs
: > src/validate.rs
: > src/transcript.rs
: > src/process.rs
: > src/orchestrator.rs
: > src/http.rs
: > src/engine/claude.rs
printf 'pub mod claude;\n' > src/engine/mod.rs
```

- [ ] **Step 4: Create a placeholder `src/main.rs`** (real wiring lands in Task 11)

```rust
fn main() {
    println!("llm-bridge: see Task 11 for the real entrypoint");
}
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo build`
Expected: `Compiling llm-bridge`, `Finished`. (Empty modules compile fine.)

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src/ tests/
git commit -m "chore: scaffold llm-bridge crate with empty module stubs"
```

---

## Task 1: OpenAI wire types

**Files:**
- Fill: `src/openai.rs`

- [ ] **Step 1: Write failing tests** (append `#[cfg(test)] mod tests` at bottom of `src/openai.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_string_content() {
        let json = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model, "m");
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].text(), "hi");
        assert!(!req.is_streaming());
    }

    #[test]
    fn deserializes_content_parts() {
        let json = r#"{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]}]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.messages[0].text(), "a b");
    }

    #[test]
    fn detects_stream_and_tools() {
        let json = r#"{"model":"m","stream":true,"tools":[{"x":1}],"messages":[]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert!(req.is_streaming());
        assert!(req.has_tools());
    }

    #[test]
    fn serializes_error_in_openai_shape() {
        let e = ApiError::new("nope", "invalid_request_error");
        let s = serde_json::to_string(&e).unwrap();
        assert_eq!(s, r#"{"error":{"message":"nope","type":"invalid_request_error"}}"#);
    }
}
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `cargo test --lib openai`
Expected: compile error (types not defined).

- [ ] **Step 3: Write the implementation** (top of `src/openai.rs`, above the test module)

```rust
//! OpenAI Chat Completions wire types (request, response, error).
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    Developer,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContentPart {
    #[serde(default)]
    pub text: String,
}

/// OpenAI message `content` is either a plain string or an array of parts.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Parts(Vec<ContentPart>),
}

impl MessageContent {
    pub fn flatten(&self) -> String {
        match self {
            MessageContent::Text(s) => s.clone(),
            MessageContent::Parts(parts) => parts
                .iter()
                .map(|p| p.text.as_str())
                .collect::<Vec<_>>()
                .join(" "),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatMessage {
    pub role: Role,
    #[serde(default)]
    pub content: Option<MessageContent>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
}

impl ChatMessage {
    /// Flattened text content ("" if none).
    pub fn text(&self) -> String {
        self.content.as_ref().map(|c| c.flatten()).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: Option<bool>,
    /// Accepted but unused in Phase 1 (MCP bridge is Phase 4).
    #[serde(default)]
    pub tools: Option<serde_json::Value>,
}

impl ChatCompletionRequest {
    pub fn is_streaming(&self) -> bool {
        self.stream.unwrap_or(false)
    }
    pub fn has_tools(&self) -> bool {
        matches!(&self.tools, Some(serde_json::Value::Array(a)) if !a.is_empty())
    }
}

// ---- Response ----

#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: &'static str, // "chat.completion"
    pub created: u64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Usage,
}

#[derive(Debug, Clone, Serialize)]
pub struct Choice {
    pub index: u32,
    pub message: ResponseMessage,
    pub finish_reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponseMessage {
    pub role: &'static str, // "assistant"
    pub content: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

// ---- Error (OpenAI shape) ----

#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    pub error: ApiErrorBody,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiErrorBody {
    pub message: String,
    #[serde(rename = "type")]
    pub kind: String,
}

impl ApiError {
    pub fn new(message: impl Into<String>, kind: impl Into<String>) -> Self {
        ApiError { error: ApiErrorBody { message: message.into(), kind: kind.into() } }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib openai`
Expected: `test result: ok. 4 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/openai.rs
git commit -m "feat: OpenAI Chat Completions wire types"
```

---

## Task 2: Config schema + YAML loader (+ tilde expansion)

**Files:**
- Fill: `src/config.rs`
- Create: `config.example.yaml`

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `src/config.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const YAML: &str = r#"
server:
  bind: "127.0.0.1:8088"
  bearer_token: "sk-test"
  progress_channel: reasoning_content
defaults:
  timeout_s: 300
  max_concurrency: 2
  sandbox_backend: none
  env_passthrough: ["PATH", "HOME"]
credentials:
  claude_config_dir: /home/u/.llm-bridge/cred/claude
models:
  - id: "claude-text"
    engine: claude
    model: sonnet
    mode: text
  - id: "claude-agentic"
    engine: claude
    model: opus
    workspace: /work/repoA
    mode: agentic
    permissions: workspace-write
    trusted_caller_only: true
"#;

    #[test]
    fn parses_full_config() {
        let cfg = parse_config(YAML).unwrap();
        assert_eq!(cfg.server.bind, "127.0.0.1:8088");
        assert_eq!(cfg.server.bearer_token.as_deref(), Some("sk-test"));
        assert_eq!(cfg.defaults.timeout_s, 300);
        assert_eq!(cfg.defaults.sandbox_backend, SandboxBackend::None);
        assert_eq!(cfg.defaults.env_passthrough, vec!["PATH".to_string(), "HOME".to_string()]);
        assert_eq!(cfg.models.len(), 2);
        assert_eq!(cfg.models[0].mode, Mode::Text);
        assert_eq!(cfg.models[1].engine, EngineKind::Claude);
        assert!(cfg.models[1].trusted_caller_only);
    }

    #[test]
    fn applies_defaults_for_optional_fields() {
        let yaml = r#"
server: { bind: "127.0.0.1:9000" }
models:
  - { id: "m", engine: claude, mode: text }
"#;
        let cfg = parse_config(yaml).unwrap();
        assert_eq!(cfg.defaults.timeout_s, 600);
        assert_eq!(cfg.defaults.max_concurrency, 4);
        assert_eq!(cfg.defaults.env_passthrough, default_env_passthrough());
        assert!(!cfg.models[0].trusted_caller_only);
        assert_eq!(cfg.server.progress_channel, ProgressChannel::ReasoningContent);
    }

    #[test]
    fn expands_leading_tilde() {
        std::env::set_var("HOME", "/home/test");
        assert_eq!(expand_tilde(&PathBuf::from("~/.llm-bridge/x")), PathBuf::from("/home/test/.llm-bridge/x"));
        assert_eq!(expand_tilde(&PathBuf::from("/abs/path")), PathBuf::from("/abs/path"));
    }
}
```

- [ ] **Step 2: Run tests to confirm failure**

Run: `cargo test --lib config`
Expected: compile error.

- [ ] **Step 3: Write the implementation** (top of `src/config.rs`)

```rust
//! Configuration schema, YAML loader, and leading-tilde path expansion.
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub credentials: Credentials,
    #[serde(default)]
    pub models: Vec<ModelEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub progress_channel: ProgressChannel,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Defaults {
    #[serde(default = "default_timeout_s")]
    pub timeout_s: u64,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    #[serde(default)]
    pub sandbox_backend: SandboxBackend,
    #[serde(default = "default_env_passthrough")]
    pub env_passthrough: Vec<String>,
}

impl Default for Defaults {
    fn default() -> Self {
        Defaults {
            timeout_s: default_timeout_s(),
            max_concurrency: default_max_concurrency(),
            sandbox_backend: SandboxBackend::default(),
            env_passthrough: default_env_passthrough(),
        }
    }
}

fn default_timeout_s() -> u64 { 600 }
fn default_max_concurrency() -> usize { 4 }
/// Non-secret vars the spawned CLI needs to run. Everything else is scrubbed (env_clear).
pub fn default_env_passthrough() -> Vec<String> {
    ["PATH", "HOME", "LANG", "LC_ALL", "TERM"].iter().map(|s| s.to_string()).collect()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Credentials {
    #[serde(default)]
    pub claude_config_dir: Option<PathBuf>,
    #[serde(default)]
    pub codex_home: Option<PathBuf>,
    #[serde(default)]
    pub agy_config_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelEntry {
    pub id: String,
    pub engine: EngineKind,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub workspace: Option<PathBuf>,
    pub mode: Mode,
    #[serde(default)]
    pub permissions: Option<String>,
    #[serde(default)]
    pub trusted_caller_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EngineKind { Claude, Codex, Agy }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode { Agentic, Text }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxBackend { #[default] None, Bubblewrap, Container }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressChannel { #[default] ReasoningContent, Omit }

/// Expand a leading `~/` to `$HOME`. Other paths returned unchanged.
pub fn expand_tilde(p: &Path) -> PathBuf {
    if let Ok(rest) = p.strip_prefix("~") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    p.to_path_buf()
}

fn expand_paths(cfg: &mut Config) {
    for slot in [
        &mut cfg.credentials.claude_config_dir,
        &mut cfg.credentials.codex_home,
        &mut cfg.credentials.agy_config_dir,
    ] {
        if let Some(p) = slot.as_ref() {
            *slot = Some(expand_tilde(p));
        }
    }
    for m in &mut cfg.models {
        if let Some(ws) = m.workspace.as_ref() {
            m.workspace = Some(expand_tilde(ws));
        }
    }
}

/// Parse config from a YAML string (no tilde expansion — used by tests).
pub fn parse_config(yaml: &str) -> anyhow::Result<Config> {
    Ok(serde_yaml::from_str(yaml)?)
}

/// Load config from a file path, expanding leading `~/` in credential and workspace paths.
pub fn load_config(path: &Path) -> anyhow::Result<Config> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("reading config {}: {e}", path.display()))?;
    let mut cfg = parse_config(&text)?;
    expand_paths(&mut cfg);
    Ok(cfg)
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cargo test --lib config`
Expected: `test result: ok. 3 passed`.

- [ ] **Step 5: Create `config.example.yaml`**

```yaml
server:
  bind: "127.0.0.1:8088"          # non-loopback bind requires a strong, non-default bearer_token
  bearer_token: "sk-change-me"    # CHANGE THIS for any non-loopback bind (validation rejects placeholders)
  progress_channel: reasoning_content
defaults:
  timeout_s: 600
  max_concurrency: 4
  sandbox_backend: none           # Phase 1 supports only `none`; others are Phase 3
  env_passthrough: ["PATH", "HOME", "LANG", "LC_ALL", "TERM"]   # all other env vars are scrubbed from claude
credentials:
  claude_config_dir: ~/.llm-bridge/cred/claude   # CLAUDE_CONFIG_DIR (leading ~ is expanded). Provision once:
                                                 #   CLAUDE_CONFIG_DIR=~/.llm-bridge/cred/claude claude login
models:
  - id: "claude-sonnet-text"
    engine: claude
    model: sonnet
    mode: text                    # PoC-style pure generator: all tools blocked
  - id: "claude-opus-repoA"
    engine: claude
    model: opus
    workspace: /work/repoA
    mode: agentic
    permissions: workspace-write
    trusted_caller_only: true     # REQUIRED for agentic when sandbox_backend: none
```

- [ ] **Step 6: Commit**

```bash
git add src/config.rs config.example.yaml
git commit -m "feat: config schema, YAML loader, tilde expansion, env_passthrough"
```

---

## Task 3: Model registry + fingerprint

**Files:**
- Fill: `src/registry.rs`

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `src/registry.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineKind, Mode, ModelEntry};

    fn entry(id: &str) -> ModelEntry {
        ModelEntry {
            id: id.into(), engine: EngineKind::Claude, model: Some("opus".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false,
        }
    }

    #[test]
    fn resolves_by_id() {
        let reg = Registry::new(vec![entry("a"), entry("b")]);
        assert_eq!(reg.resolve("b").unwrap().id, "b");
        assert!(reg.resolve("missing").is_none());
    }

    #[test]
    fn models_json_has_openai_list_shape() {
        let reg = Registry::new(vec![entry("a")]);
        let v = reg.models_json();
        assert_eq!(v["object"], "list");
        assert_eq!(v["data"][0]["id"], "a");
        assert_eq!(v["data"][0]["object"], "model");
    }

    #[test]
    fn fingerprint_changes_with_workspace() {
        let mut a = entry("x");
        let mut b = entry("x");
        b.workspace = Some("/different".into());
        assert_ne!(model_entry_fingerprint(&a), model_entry_fingerprint(&b));
        a.workspace = Some("/different".into());
        assert_eq!(model_entry_fingerprint(&a), model_entry_fingerprint(&b));
    }
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib registry`
Expected: compile error.

- [ ] **Step 3: Write the implementation** (top of `src/registry.rs`)

```rust
//! Model registry: resolve a request `model` id to its `ModelEntry`, serve `/v1/models`,
//! and compute the ModelEntry fingerprint that (in Phase 2) feeds the session key.
use crate::config::ModelEntry;
use serde_json::json;

pub struct Registry {
    models: Vec<ModelEntry>,
}

impl Registry {
    pub fn new(models: Vec<ModelEntry>) -> Self {
        Registry { models }
    }

    pub fn resolve(&self, id: &str) -> Option<&ModelEntry> {
        self.models.iter().find(|m| m.id == id)
    }

    pub fn models(&self) -> &[ModelEntry] {
        &self.models
    }

    /// OpenAI `/v1/models` body: `{ "object": "list", "data": [ { id, object, owned_by } ] }`.
    pub fn models_json(&self) -> serde_json::Value {
        let data: Vec<_> = self
            .models
            .iter()
            .map(|m| json!({ "id": m.id, "object": "model", "owned_by": "llm-bridge" }))
            .collect();
        json!({ "object": "list", "data": data })
    }
}

/// Stable fingerprint over the resolution-relevant fields of a ModelEntry. Defined and tested
/// here so Phase 2's session key can rely on it.
pub fn model_entry_fingerprint(m: &ModelEntry) -> String {
    let canon = json!({
        "engine": format!("{:?}", m.engine),
        "model": m.model,
        "workspace": m.workspace,
        "mode": format!("{:?}", m.mode),
        "permissions": m.permissions,
    });
    let s = serde_json::to_string(&canon).expect("fingerprint serialize");
    format!("{:016x}", fnv1a(s.as_bytes()))
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --lib registry`
Expected: `test result: ok. 3 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/registry.rs
git commit -m "feat: model registry, /v1/models body, entry fingerprint"
```

---

## Task 4: Startup validation

Implements the spec §4.8 startup rules Phase 1 owns: bind/auth (incl. **non-default** token for non-loopback), agentic safety (`trusted_caller_only`), the Phase-1 ceiling (`sandbox_backend == none`; claude only), and workspace↔credential path-overlap.

**Files:**
- Fill: `src/validate.rs`

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `src/validate.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use std::path::PathBuf;

    fn base() -> Config {
        Config {
            server: ServerConfig { bind: "127.0.0.1:8088".into(), bearer_token: None,
                                   progress_channel: ProgressChannel::ReasoningContent },
            defaults: Defaults::default(),
            credentials: Credentials::default(),
            models: vec![],
        }
    }

    fn agentic(ws: &str, trusted: bool) -> ModelEntry {
        ModelEntry { id: "a".into(), engine: EngineKind::Claude, model: Some("opus".into()),
            workspace: Some(PathBuf::from(ws)), mode: Mode::Agentic, permissions: None,
            trusted_caller_only: trusted }
    }

    #[test]
    fn loopback_without_token_is_ok() {
        assert!(validate_config(&base()).is_ok());
    }

    #[test]
    fn non_loopback_without_token_is_refused() {
        let mut cfg = base();
        cfg.server.bind = "0.0.0.0:8088".into();
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("non-loopback")), "{err:?}");
    }

    #[test]
    fn non_loopback_with_placeholder_token_is_refused() {
        let mut cfg = base();
        cfg.server.bind = "0.0.0.0:8088".into();
        cfg.server.bearer_token = Some("sk-change-me".into());
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("non-default")), "{err:?}");
    }

    #[test]
    fn non_loopback_with_strong_token_is_ok() {
        let mut cfg = base();
        cfg.server.bind = "0.0.0.0:8088".into();
        cfg.server.bearer_token = Some("k7Qe2vR9mZ1pX4nL8wTciuY3".into());
        assert!(validate_config(&cfg).is_ok());
    }

    #[test]
    fn agentic_without_trusted_is_refused() {
        let mut cfg = base();
        cfg.models = vec![agentic("/work/repoA", false)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("trusted_caller_only")), "{err:?}");
    }

    #[test]
    fn agentic_with_trusted_is_ok() {
        let mut cfg = base();
        cfg.models = vec![agentic("/work/repoA", true)];
        assert!(validate_config(&cfg).is_ok());
    }

    #[test]
    fn sandbox_backend_other_than_none_is_refused_in_phase1() {
        let mut cfg = base();
        cfg.defaults.sandbox_backend = SandboxBackend::Bubblewrap;
        cfg.models = vec![agentic("/work/repoA", true)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("sandbox_backend")), "{err:?}");
    }

    #[test]
    fn non_claude_engine_is_refused_in_phase1() {
        let mut cfg = base();
        cfg.models = vec![ModelEntry { id: "c".into(), engine: EngineKind::Codex,
            model: None, workspace: None, mode: Mode::Text, permissions: None,
            trusted_caller_only: false }];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("only the claude engine")), "{err:?}");
    }

    #[test]
    fn workspace_containing_credential_dir_is_refused() {
        let mut cfg = base();
        cfg.credentials.claude_config_dir = Some(PathBuf::from("/home/u/cred/claude"));
        cfg.models = vec![agentic("/home/u", true)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("overlaps")), "{err:?}");
    }
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib validate`
Expected: compile error.

- [ ] **Step 3: Write the implementation** (top of `src/validate.rs`)

```rust
//! Startup validation. Returns Err(Vec<message>) listing every problem (so the operator sees all
//! issues at once). Phase 1 enforces the rules it owns; Phase 3 adds the sandbox canary probes.
use crate::config::{Config, EngineKind, Mode, SandboxBackend};
use std::path::{Path, PathBuf};

/// Tokens we refuse for a non-loopback bind even though they are non-empty.
const PLACEHOLDER_TOKENS: &[&str] = &["sk-change-me", "sk-noauth", "changeme", "secret", "token"];
const MIN_TOKEN_LEN: usize = 16;

pub fn validate_config(cfg: &Config) -> Result<(), Vec<String>> {
    let mut errs = Vec::new();

    // Bind / auth.
    if !is_loopback_bind(&cfg.server.bind) {
        let token = cfg.server.bearer_token.as_deref().unwrap_or("");
        if token.is_empty() {
            errs.push(format!(
                "server.bind {} is non-loopback but no bearer_token is set; refusing to expose unauthenticated",
                cfg.server.bind
            ));
        } else if PLACEHOLDER_TOKENS.contains(&token) || token.len() < MIN_TOKEN_LEN {
            errs.push(format!(
                "server.bind {} is non-loopback but bearer_token is a placeholder/too short; set a strong, non-default token (>= {} chars)",
                cfg.server.bind, MIN_TOKEN_LEN
            ));
        }
    }

    // Phase-1 ceiling: only `none` sandbox backend is implemented.
    if cfg.defaults.sandbox_backend != SandboxBackend::None {
        errs.push(format!(
            "defaults.sandbox_backend={:?} is not implemented in Phase 1 (use `none`; sandbox backends arrive in Phase 3)",
            cfg.defaults.sandbox_backend
        ));
    }

    let cred_dirs: Vec<PathBuf> = [
        cfg.credentials.claude_config_dir.clone(),
        cfg.credentials.codex_home.clone(),
        cfg.credentials.agy_config_dir.clone(),
    ]
    .into_iter()
    .flatten()
    .collect();

    for m in &cfg.models {
        if m.engine != EngineKind::Claude {
            errs.push(format!(
                "model '{}': only the claude engine is implemented in Phase 1 (got {:?})",
                m.id, m.engine
            ));
        }

        if m.mode == Mode::Agentic {
            if !m.trusted_caller_only {
                errs.push(format!(
                    "model '{}': agentic models require trusted_caller_only: true when sandbox_backend is none",
                    m.id
                ));
            }
            if m.workspace.is_none() {
                errs.push(format!("model '{}': agentic models require a workspace", m.id));
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

    if errs.is_empty() { Ok(()) } else { Err(errs) }
}

fn is_loopback_bind(bind: &str) -> bool {
    let host = bind.rsplit_once(':').map(|(h, _)| h).unwrap_or(bind);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>().map(|ip| ip.is_loopback()).unwrap_or(false)
}

/// Canonicalize if the path exists; otherwise make it lexically absolute.
fn resolve_path(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf()))
}

fn paths_overlap(a: &Path, b: &Path) -> bool {
    a == b || a.starts_with(b) || b.starts_with(a)
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --lib validate`
Expected: `test result: ok. 9 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/validate.rs
git commit -m "feat: startup validation (bind/auth non-default token, agentic safety, path overlap)"
```

---

## Task 5: Engine core types + Engine enum

**Files:**
- Fill: `src/engine/mod.rs`, `src/engine/claude.rs` (stub — real impl in Task 8)

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `src/engine/mod.rs`)

```rust
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
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib engine`
Expected: compile error.

- [ ] **Step 3: Write `src/engine/mod.rs`** (replace the Task-0 `pub mod claude;` line; keep it as the first line)

```rust
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
```

- [ ] **Step 4: Write the `src/engine/claude.rs` stub** (full impl in Task 8)

```rust
//! ClaudeAdapter (full impl in Task 8).
use super::{AgentEvent, EngineError, Turn};
use std::path::PathBuf;

pub struct ClaudeAdapter { pub bin: String, pub config_dir: Option<PathBuf> }

impl ClaudeAdapter {
    pub fn new(bin: &str, config_dir: Option<PathBuf>) -> Self {
        ClaudeAdapter { bin: bin.into(), config_dir }
    }
    pub fn build_command(&self, _turn: &Turn, _env_passthrough: &[String]) -> (tokio::process::Command, Option<String>) {
        (tokio::process::Command::new(&self.bin), None)
    }
    pub fn parse_output(&self, _stdout: &str) -> Result<Vec<AgentEvent>, EngineError> {
        Ok(vec![])
    }
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test --lib engine`
Expected: `test result: ok. 2 passed`.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mod.rs src/engine/claude.rs
git commit -m "feat: engine core types and Engine dispatch enum (claude)"
```

---

## Task 6: Transcript renderer

**Files:**
- Fill: `src/transcript.rs`

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `src/transcript.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::openai::{ChatMessage, MessageContent, Role};

    fn msg(role: Role, text: &str) -> ChatMessage {
        ChatMessage { role, content: Some(MessageContent::Text(text.into())), tool_call_id: None }
    }

    #[test]
    fn single_user_turn_has_no_transcript_preamble() {
        let r = render_turn(&[msg(Role::User, "hello")]);
        assert_eq!(r.system_prompt, None);
        assert_eq!(r.user_prompt, "hello");
    }

    #[test]
    fn system_messages_become_system_prompt() {
        let r = render_turn(&[msg(Role::System, "be terse"), msg(Role::User, "hi")]);
        assert_eq!(r.system_prompt.as_deref(), Some("be terse"));
        assert_eq!(r.user_prompt, "hi");
    }

    #[test]
    fn multi_turn_renders_readonly_transcript_then_final_user_turn() {
        let msgs = vec![
            msg(Role::User, "first"),
            msg(Role::Assistant, "answer one"),
            msg(Role::User, "second"),
        ];
        let r = render_turn(&msgs);
        assert!(r.user_prompt.contains("reference only"), "{}", r.user_prompt);
        assert!(r.user_prompt.contains("### User"));
        assert!(r.user_prompt.contains("### Assistant"));
        assert!(r.user_prompt.contains("answer one"));
        assert!(r.user_prompt.trim_end().ends_with("second"), "{}", r.user_prompt);
    }
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib transcript`
Expected: compile error.

- [ ] **Step 3: Write the implementation** (top of `src/transcript.rs`)

```rust
//! Render an OpenAI message list into a single prompt for a fresh CLI session, with a guard so
//! historical instructions are treated as read-only context (spec §4.5, miss-path replay).
use crate::openai::{ChatMessage, Role};

pub struct RenderedTurn {
    pub system_prompt: Option<String>,
    pub user_prompt: String,
}

const GUARD: &str = "The following is the prior conversation, provided as context for reference \
only. Do NOT re-execute past instructions; respond only to the final user message below.";

pub fn render_turn(messages: &[ChatMessage]) -> RenderedTurn {
    let system_parts: Vec<String> = messages
        .iter()
        .filter(|m| matches!(m.role, Role::System | Role::Developer))
        .map(|m| m.text())
        .filter(|s| !s.is_empty())
        .collect();
    let system_prompt = if system_parts.is_empty() { None } else { Some(system_parts.join("\n\n")) };

    let convo: Vec<&ChatMessage> = messages
        .iter()
        .filter(|m| !matches!(m.role, Role::System | Role::Developer))
        .collect();

    let last_user_idx = convo.iter().rposition(|m| m.role == Role::User);

    match last_user_idx {
        Some(idx) if idx == 0 => RenderedTurn { system_prompt, user_prompt: convo[0].text() },
        Some(idx) => {
            let mut out = String::new();
            out.push_str(GUARD);
            out.push_str("\n\n--- BEGIN PRIOR CONVERSATION ---\n");
            for m in &convo[..idx] {
                let label = match m.role {
                    Role::User => "### User",
                    Role::Assistant => "### Assistant",
                    Role::Tool => "### Tool result",
                    _ => "### Other",
                };
                out.push_str(label);
                if m.role == Role::Tool {
                    if let Some(id) = &m.tool_call_id {
                        out.push_str(&format!(" for {id}"));
                    }
                }
                out.push('\n');
                out.push_str(&m.text());
                out.push_str("\n\n");
            }
            out.push_str("--- END PRIOR CONVERSATION ---\n\n");
            out.push_str(&convo[idx].text());
            RenderedTurn { system_prompt, user_prompt: out }
        }
        None => {
            let joined = convo.iter().map(|m| m.text()).collect::<Vec<_>>().join("\n\n");
            RenderedTurn { system_prompt, user_prompt: joined }
        }
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --lib transcript`
Expected: `test result: ok. 3 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/transcript.rs
git commit -m "feat: read-only transcript renderer for fresh-session replay"
```

---

## Task 7: ProcessSupervisor

**Files:**
- Fill: `src/process.rs`

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `src/process.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::process::Command;

    #[tokio::test]
    async fn runs_command_and_captures_stdout() {
        let sup = ProcessSupervisor::new(2);
        let cmd = Command::new("cat"); // echoes stdin to stdout
        let out = sup.run(cmd, Some("hello\n".into()), Duration::from_secs(5)).await.unwrap();
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "hello");
    }

    #[tokio::test]
    async fn times_out_long_commands() {
        let sup = ProcessSupervisor::new(2);
        let mut cmd = Command::new("sleep");
        cmd.arg("5");
        let err = sup.run(cmd, None, Duration::from_millis(200)).await.unwrap_err();
        assert!(matches!(err, ProcessError::Timeout));
    }

    #[tokio::test]
    async fn surfaces_nonzero_exit() {
        let sup = ProcessSupervisor::new(2);
        let mut cmd = Command::new("bash");
        cmd.arg("-c").arg("echo oops >&2; exit 3");
        let out = sup.run(cmd, None, Duration::from_secs(5)).await.unwrap();
        assert_eq!(out.status.code(), Some(3));
        assert!(String::from_utf8_lossy(&out.stderr).contains("oops"));
    }

    #[tokio::test]
    async fn is_cloneable_and_shares_the_permit_pool() {
        // Cloning shares the same Arc<Semaphore> so the cap is global, not per-clone.
        let sup = ProcessSupervisor::new(1);
        let sup2 = sup.clone();
        assert!(std::sync::Arc::ptr_eq(&sup.sem, &sup2.sem));
    }
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib process`
Expected: compile error.

- [ ] **Step 3: Write the implementation** (top of `src/process.rs`)

```rust
//! Spawn child CLIs with stdin, a per-turn timeout, and a GLOBAL concurrency cap. One instance is
//! shared across all requests (held in AppState via the runner), so cloning shares the semaphore.
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::Semaphore;

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("the child timed out")]
    Timeout,
    #[error("spawn/io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
pub struct ProcessSupervisor {
    pub(crate) sem: Arc<Semaphore>,
}

impl ProcessSupervisor {
    pub fn new(max_concurrency: usize) -> Self {
        ProcessSupervisor { sem: Arc::new(Semaphore::new(max_concurrency.max(1))) }
    }

    /// Run `cmd`, optionally writing `stdin`, killing it after `timeout`. Returns the full Output.
    pub async fn run(
        &self,
        mut cmd: Command,
        stdin: Option<String>,
        timeout: Duration,
    ) -> Result<Output, ProcessError> {
        let _permit = self.sem.acquire().await.expect("semaphore not closed");

        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);

        let mut child = cmd.spawn()?;

        if let Some(payload) = stdin {
            let mut sink = child.stdin.take().expect("stdin piped");
            sink.write_all(payload.as_bytes()).await?;
            sink.shutdown().await?; // close stdin so the child sees EOF
        } else {
            drop(child.stdin.take());
        }

        match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(result) => Ok(result?),
            Err(_) => Err(ProcessError::Timeout), // child is killed on drop
        }
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --lib process`
Expected: `test result: ok. 4 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/process.rs
git commit -m "feat: ProcessSupervisor (stdin, timeout, shared global concurrency cap)"
```

---

## Task 8: ClaudeAdapter (real implementation, with env scrub)

Replaces the Task 5 stub. Builds `claude -p --output-format json` (text vs agentic), **scrubs the environment** to an allowlist so model-run tools can't read service secrets, and parses the single JSON result (verified shape: keys include `result`, `session_id`, `is_error`, `stop_reason`).

**Files:**
- Replace: `src/engine/claude.rs`
- Create: `tests/fixtures/claude_result.json`

- [ ] **Step 1: Create the recorded fixture** `tests/fixtures/claude_result.json`

```json
{"type":"result","subtype":"success","is_error":false,"result":"pong","session_id":"391b532e-a8ee-4bbf-9689-ab6891d09e90","stop_reason":"end_turn","num_turns":1}
```

- [ ] **Step 2: Write failing tests** (`#[cfg(test)] mod tests` in `src/engine/claude.rs`)

```rust
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
```

- [ ] **Step 3: Run to confirm failure**

Run: `cargo test --lib engine::claude`
Expected: failures (stub returns empty / bare command).

- [ ] **Step 4: Write the implementation** (replace all of `src/engine/claude.rs`)

```rust
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
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test --lib engine::claude`
Expected: `test result: ok. 5 passed`.

- [ ] **Step 6: Commit**

```bash
git add src/engine/claude.rs tests/fixtures/claude_result.json
git commit -m "feat: ClaudeAdapter build_command (env scrub) + JSON result parsing"
```

---

## Task 9: Orchestrator — TurnRunner, ClaudeProcessRunner, pure helpers

The injectable run layer + pure request/response mapping. No axum here (the handler lives in `http.rs`, Task 10), so there's no module cycle.

**Files:**
- Fill: `src/orchestrator.rs`

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `src/orchestrator.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineKind, Mode, ModelEntry};
    use crate::engine::AgentEvent;
    use crate::openai::{ChatCompletionRequest, ChatMessage, MessageContent, Role};

    fn entry() -> ModelEntry {
        ModelEntry { id: "m".into(), engine: EngineKind::Claude, model: Some("opus".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false }
    }

    fn req() -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: "m".into(),
            messages: vec![
                ChatMessage { role: Role::System, content: Some(MessageContent::Text("sys".into())), tool_call_id: None },
                ChatMessage { role: Role::User, content: Some(MessageContent::Text("hello".into())), tool_call_id: None },
            ],
            stream: Some(false),
            tools: None,
        }
    }

    #[test]
    fn turn_from_request_maps_system_and_prompt() {
        let t = turn_from_request(&req(), &entry());
        assert_eq!(t.system_prompt.as_deref(), Some("sys"));
        assert_eq!(t.user_prompt, "hello");
        assert_eq!(t.model.as_deref(), Some("opus"));
        assert_eq!(t.mode, Mode::Text);
    }

    #[test]
    fn response_from_events_concatenates_text_and_sets_finish() {
        let events = vec![
            AgentEvent::SessionId("s".into()),
            AgentEvent::AssistantText("pong".into()),
            AgentEvent::Done { finish_reason: "stop".into() },
        ];
        let resp = response_from_events(events, "m").unwrap();
        assert_eq!(resp.choices[0].message.content, "pong");
        assert_eq!(resp.choices[0].finish_reason, "stop");
        assert_eq!(resp.model, "m");
        assert_eq!(resp.object, "chat.completion");
    }

    #[test]
    fn response_from_events_error_becomes_err() {
        let events = vec![AgentEvent::Error("boom".into())];
        assert!(response_from_events(events, "m").is_err());
    }
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib orchestrator`
Expected: compile error.

- [ ] **Step 3: Write the implementation** (top of `src/orchestrator.rs`)

```rust
//! Turn orchestration: the injectable `TurnRunner`, the production `ClaudeProcessRunner`, and the
//! pure request->Turn and events->response mappings. The HTTP handler (http.rs) calls these.
use crate::config::ModelEntry;
use crate::engine::claude::ClaudeAdapter;
use crate::engine::{AgentEvent, Engine, EngineError, Turn};
use crate::openai::{
    ChatCompletionRequest, ChatCompletionResponse, Choice, ResponseMessage, Usage,
};
use crate::process::{ProcessError, ProcessSupervisor};
use crate::transcript::render_turn;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("timed out")]
    Timeout,
    #[error("spawn failed: {0}")]
    Spawn(String),
    #[error("engine error: {0}")]
    Engine(String),
}

pub type RunFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<AgentEvent>, RunError>> + Send + 'a>>;

/// Injectable: runs a Turn and returns normalized events. Production spawns claude; tests fake it.
pub trait TurnRunner: Send + Sync {
    fn run<'a>(&'a self, entry: &'a ModelEntry, turn: Turn) -> RunFuture<'a>;
}

/// Production runner: builds the claude command, runs it through the shared supervisor, parses.
pub struct ClaudeProcessRunner {
    pub supervisor: ProcessSupervisor,
    pub claude_config_dir: Option<PathBuf>,
    pub env_passthrough: Vec<String>,
    pub timeout: Duration,
}

impl TurnRunner for ClaudeProcessRunner {
    fn run<'a>(&'a self, _entry: &'a ModelEntry, turn: Turn) -> RunFuture<'a> {
        Box::pin(async move {
            let engine = Engine::Claude(ClaudeAdapter::new("claude", self.claude_config_dir.clone()));
            let (cmd, stdin) = engine.build_command(&turn, &self.env_passthrough);
            let output = self
                .supervisor
                .run(cmd, stdin, self.timeout)
                .await
                .map_err(|e| match e {
                    ProcessError::Timeout => RunError::Timeout,
                    ProcessError::Io(io) => RunError::Spawn(io.to_string()),
                })?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(RunError::Engine(format!(
                    "claude exited {}: {}",
                    output.status,
                    stderr.trim()
                )));
            }
            let stdout = String::from_utf8_lossy(&output.stdout);
            engine.parse_output(&stdout).map_err(|e| RunError::Engine(e.to_string()))
        })
    }
}

/// Map an incoming request + resolved entry into an engine Turn (fresh-session replay).
pub fn turn_from_request(req: &ChatCompletionRequest, entry: &ModelEntry) -> Turn {
    let rendered = render_turn(&req.messages);
    Turn {
        system_prompt: rendered.system_prompt,
        user_prompt: rendered.user_prompt,
        model: entry.model.clone(),
        workspace: entry.workspace.clone(),
        mode: entry.mode,
    }
}

/// Assemble normalized events into an OpenAI chat.completion (or an error if the engine failed).
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
            AgentEvent::SessionId(_) => {} // captured by the session store in Phase 2
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
        usage: Usage::default(), // token accounting is Phase 2+
    })
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --lib orchestrator`
Expected: `test result: ok. 3 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/orchestrator.rs
git commit -m "feat: TurnRunner trait, ClaudeProcessRunner, request/response mapping"
```

---

## Task 10: HTTP layer — AppState, router, auth, endpoints, handler (+ hermetic tests)

**Files:**
- Fill: `src/http.rs`
- Create: `tests/http_integration.rs`

- [ ] **Step 1: Write failing integration tests** (`tests/http_integration.rs`)

```rust
use http_body_util::BodyExt;
use llm_bridge::config::{EngineKind, Mode, ModelEntry};
use llm_bridge::engine::{AgentEvent, Turn};
use llm_bridge::http::{build_router, AppState};
use llm_bridge::orchestrator::{RunFuture, TurnRunner};
use llm_bridge::registry::Registry;
use std::sync::Arc;
use tower::ServiceExt; // oneshot

/// A canned runner so handler tests are hermetic (no real claude).
struct FakeRunner {
    events: Vec<AgentEvent>,
}
impl TurnRunner for FakeRunner {
    fn run<'a>(&'a self, _entry: &'a ModelEntry, _turn: Turn) -> RunFuture<'a> {
        let events = self.events.clone();
        Box::pin(async move { Ok(events) })
    }
}

fn state_with(token: Option<&str>, runner: Arc<dyn TurnRunner>) -> AppState {
    let models = vec![ModelEntry {
        id: "claude-text".into(), engine: EngineKind::Claude, model: Some("sonnet".into()),
        workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false,
    }];
    AppState { registry: Arc::new(Registry::new(models)), bearer_token: token.map(String::from), runner }
}

fn state(token: Option<&str>) -> AppState {
    state_with(token, Arc::new(FakeRunner { events: vec![] }))
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn health_ok() {
    let app = build_router(state(None));
    let resp = app.oneshot(axum::http::Request::get("/health").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn models_lists_registered_models() {
    let app = build_router(state(None));
    let resp = app.oneshot(axum::http::Request::get("/v1/models").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert!(body_string(resp).await.contains("\"claude-text\""));
}

#[tokio::test]
async fn missing_bearer_token_is_401() {
    let app = build_router(state(Some("sk-secret-strong-token-1234")));
    let resp = app.oneshot(axum::http::Request::get("/v1/models").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn correct_bearer_token_passes() {
    let app = build_router(state(Some("sk-secret-strong-token-1234")));
    let resp = app.oneshot(
        axum::http::Request::get("/v1/models")
            .header("authorization", "Bearer sk-secret-strong-token-1234")
            .body(axum::body::Body::empty()).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn chat_success_returns_completion() {
    let runner = Arc::new(FakeRunner { events: vec![
        AgentEvent::SessionId("s".into()),
        AgentEvent::AssistantText("pong".into()),
        AgentEvent::Done { finish_reason: "stop".into() },
    ]});
    let app = build_router(state_with(None, runner));
    let body = r#"{"model":"claude-text","messages":[{"role":"user","content":"hi"}]}"#;
    let resp = app.oneshot(
        axum::http::Request::post("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body)).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), 200);
    let b = body_string(resp).await;
    assert!(b.contains("\"pong\""), "{b}");
    assert!(b.contains("chat.completion"), "{b}");
}

#[tokio::test]
async fn chat_unknown_model_is_404() {
    let app = build_router(state(None));
    let body = r#"{"model":"nope","messages":[{"role":"user","content":"hi"}]}"#;
    let resp = app.oneshot(
        axum::http::Request::post("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body)).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), 404);
    assert!(body_string(resp).await.contains("unknown model"));
}

#[tokio::test]
async fn chat_streaming_is_rejected_in_phase1() {
    let app = build_router(state(None));
    let body = r#"{"model":"claude-text","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let resp = app.oneshot(
        axum::http::Request::post("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body)).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), 400);
    assert!(body_string(resp).await.contains("streaming"));
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --test http_integration`
Expected: compile error (`build_router`/`AppState` not defined).

- [ ] **Step 3: Write the implementation** (top of `src/http.rs`)

```rust
//! HTTP layer: shared state, router, health/models endpoints, bearer-auth middleware, and the
//! non-streaming chat-completions handler.
use crate::openai::{ApiError, ChatCompletionRequest};
use crate::orchestrator::{response_from_events, turn_from_request, RunError, TurnRunner};
use crate::engine::EngineError;
use crate::registry::Registry;
use axum::{
    extract::State,
    http::{header::AUTHORIZATION, Request, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub bearer_token: Option<String>,
    pub runner: Arc<dyn TurnRunner>,
}

pub fn build_router(state: AppState) -> Router {
    // `route_layer` applies auth ONLY to routes declared before it, so `/v1/*` require a token
    // while `/health` (declared after) stays open.
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

async fn auth_middleware(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(expected) = state.bearer_token.as_deref().filter(|t| !t.is_empty()) else {
        return next.run(req).await; // no token configured -> auth disabled (loopback/trusted default)
    };
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if presented == Some(expected) {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(ApiError::new("missing or invalid bearer token", "invalid_request_error")),
        )
            .into_response()
    }
}

/// POST /v1/chat/completions (non-streaming only in Phase 1).
async fn chat_completions(
    State(state): State<AppState>,
    Json(req): Json<ChatCompletionRequest>,
) -> Response {
    if req.is_streaming() {
        return err(StatusCode::BAD_REQUEST, "streaming is not implemented yet (Phase 2)", "invalid_request_error");
    }
    if req.has_tools() {
        return err(StatusCode::BAD_REQUEST, "the `tools` field is not implemented yet (Phase 4)", "invalid_request_error");
    }
    let Some(entry) = state.registry.resolve(&req.model) else {
        return err(StatusCode::NOT_FOUND, &format!("unknown model '{}'", req.model), "model_not_found");
    };
    let entry = entry.clone();
    let turn = turn_from_request(&req, &entry);

    match state.runner.run(&entry, turn).await {
        Ok(events) => match response_from_events(events, &req.model) {
            Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
            Err(EngineError::Reported(m)) => err(StatusCode::INTERNAL_SERVER_ERROR, &m, "engine_error"),
            Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "engine_error"),
        },
        Err(RunError::Timeout) => err(StatusCode::GATEWAY_TIMEOUT, "claude timed out", "timeout"),
        Err(RunError::Spawn(m)) => err(StatusCode::INTERNAL_SERVER_ERROR, &format!("spawn failed: {m}"), "engine_error"),
        Err(RunError::Engine(m)) => err(StatusCode::INTERNAL_SERVER_ERROR, &m, "engine_error"),
    }
}

fn err(status: StatusCode, message: &str, kind: &str) -> Response {
    (status, Json(ApiError::new(message, kind))).into_response()
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test http_integration`
Expected: `test result: ok. 7 passed`.

- [ ] **Step 5: Run the whole suite**

Run: `cargo test`
Expected: all unit + integration tests pass; no `e2e_smoke` (feature off).

- [ ] **Step 6: Commit**

```bash
git add src/http.rs tests/http_integration.rs
git commit -m "feat: HTTP router, auth, and non-streaming chat-completions handler (hermetic tests)"
```

---

## Task 11: main entrypoint, e2e smoke, run docs

**Files:**
- Replace: `src/main.rs`
- Create: `tests/e2e_smoke.rs`, `README.md`

- [ ] **Step 1: Write the real `src/main.rs`**

```rust
//! llm-bridge entrypoint: load config, validate, build the shared runner + router, serve.
use llm_bridge::config::load_config;
use llm_bridge::http::{build_router, AppState};
use llm_bridge::orchestrator::ClaudeProcessRunner;
use llm_bridge::process::ProcessSupervisor;
use llm_bridge::registry::Registry;
use llm_bridge::validate::validate_config;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let config_path = std::env::args().nth(1).unwrap_or_else(|| "config.yaml".to_string());
    let cfg = load_config(std::path::Path::new(&config_path))?;

    if let Err(problems) = validate_config(&cfg) {
        for p in &problems {
            tracing::error!("config: {p}");
        }
        anyhow::bail!("{} config validation error(s); refusing to start", problems.len());
    }

    // One shared supervisor -> a GLOBAL concurrency cap across all requests.
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
    };

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("llm-bridge listening on {bind} ({} models)", cfg.models.len());
    axum::serve(listener, app).await?;
    Ok(())
}
```

- [ ] **Step 2: Verify the full build and tests**

Run: `cargo build && cargo test`
Expected: clean build; all tests pass.

- [ ] **Step 3: Write the feature-gated e2e smoke** (`tests/e2e_smoke.rs`)

```rust
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
```

- [ ] **Step 4: Run the smoke test manually** (not part of `cargo test`)

Run: `cargo test --features e2e_smoke --test e2e_smoke -- --nocapture`
Expected: PASS (model reply contains "pong"). If `claude` isn't logged in, or the env allowlist is missing something claude needs, it will fail — expand `passthrough` for your environment. The test is opt-in.

- [ ] **Step 5: Write `README.md`**

````markdown
# llm-bridge

OpenAI Chat Completions–compatible HTTP shim over the `claude` CLI (Phase 1).
Successor to the Python PoC; see `docs/superpowers/specs/2026-05-31-llm-bridge-design.md`.

## Run

```bash
cp config.example.yaml config.yaml   # edit models / bearer_token
# Provision a DEDICATED claude login (keeps the service off your personal config):
CLAUDE_CONFIG_DIR=~/.llm-bridge/cred/claude claude login
cargo run --release -- config.yaml
```

Point OpenWebUI/LiteLLM at `http://127.0.0.1:8088/v1` (api key = your `bearer_token`).

## Security posture (Phase 1, single-user/trusted)
- The spawned `claude` runs with a **scrubbed environment** (only `env_passthrough` vars survive),
  so secrets like `ANTHROPIC_API_KEY` are NOT visible to model-run tools. Auth is **file-based**
  via `CLAUDE_CONFIG_DIR`.
- Agentic models require `trusted_caller_only: true` (no OS sandbox in Phase 1). Bind localhost;
  front with a trusted proxy if remote. Non-loopback binds require a strong, non-default token.

## Status (Phase 1)
- ✅ `GET /health`, `GET /v1/models`, `POST /v1/chat/completions` (non-streaming)
- ✅ claude engine, text + agentic (`trusted_caller_only`) modes; global `max_concurrency`
- ⛔ `stream: true` → 400 (Phase 2), `tools` → 400 (Phase 4), non-claude/`sandbox_backend` → refused at startup (Phase 3)

## Test
```bash
cargo test                                                        # hermetic unit + router tests
cargo test --features e2e_smoke --test e2e_smoke -- --nocapture   # real claude (opt-in)
```
````

- [ ] **Step 6: Final commit**

```bash
git add src/main.rs tests/e2e_smoke.rs README.md
git commit -m "feat: main entrypoint, e2e smoke test, README (Phase 1 complete)"
```

---

## Phase 1 Done — Definition of Done

- `cargo build` clean; `cargo test` green (hermetic) — including the **handler success path** via `FakeRunner`.
- `cargo run -- config.yaml` serves `/health`, `/v1/models`, and answers `/v1/chat/completions` from claude.
- `max_concurrency` is a single global cap (one shared `ProcessSupervisor`); the spawned claude has a scrubbed env.
- Unsafe/unsupported config is refused at startup with clear messages (incl. non-default token, non-claude engine, `sandbox_backend != none`, workspace/cred overlap); `stream`/`tools` requests return clear 400s.

**Next:** Phase 2 plan (SSE streaming + session store/resume) — switch ClaudeAdapter to `--output-format stream-json`, add the `SessionStore` + content-hash key (ModelEntry + tool-config + system-prompt + runtime fingerprints) with index advancement after every turn, and the `reasoning_content`/`omit` progress profiles.
