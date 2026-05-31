# llm-bridge — Design

**Date:** 2026-05-31
**Status:** Approved for planning (revised after spec review)

## 1. Purpose

`llm-bridge` is a Rust service that exposes an **OpenAI Chat Completions–compatible HTTP API**
in front of local **agentic coding CLIs** — `claude` (Claude Code / Anthropic), `codex`
(OpenAI Codex), and `agy` (Google Antigravity / Gemini). It lets any OpenAI-compatible client
(OpenWebUI, LiteLLM, scripts) drive these agents — including their real powers: reading and
editing files, running commands, and holding multi-turn sessions — by selecting a "model".

It implements the **Chat Completions** and **Models** endpoints (the surface OpenWebUI and
LiteLLM actually use); it is not a full reimplementation of the OpenAI API (no embeddings,
logprobs, assistants, etc.).

It supersedes the Python proof-of-concept
(`autoid/autoid-ai-poc/scripts/claude_shim.py`), which wrapped only `claude -p` as a
**pure text generator** with all tools disabled. `llm-bridge` instead does **full agentic
passthrough**, supports all three engines behind one uniform interface, streams the agent's
work as it happens (where the engine supports it), and bridges OpenAI tool-calling to MCP.

### Goals
- One Chat Completions endpoint fronting three different agentic CLIs.
- Pick engine + underlying model + workspace + mode + permissions by selecting a model id.
- Real agentic behavior (file edits, command execution) under a sandboxed posture.
- Multi-turn continuity that survives a stateless-looking HTTP API.
- Stream the agent's tool activity to the client as visible-but-non-persisted progress.
- Honor OpenAI `tools` (function calling) by bridging to MCP.

### Non-goals
- Reimplementing the agents or talking to model vendor APIs directly. We always shell out to
  the installed CLI and rely on its existing local login/auth.
- A web UI of our own. Clients (OpenWebUI, etc.) provide the UI.
- Endpoints/features beyond Chat Completions + Models, or features the CLIs don't support.

## 2. Key decisions (from brainstorming + spec review)

| Decision | Choice |
|---|---|
| Scope | **Full agentic passthrough** — expose file edits, tool use, sessions |
| Workspace selection | **Config-driven model list**: config registers named tuples, each published as a model in `/v1/models`; an OpenWebUI Pipe is the escape hatch for explicit per-request dirs |
| Session continuity | **Content-hash resume**, keyed on the conversation projection **+ a ModelEntry fingerprint** |
| Progress output | Tool activity → **`reasoning_content`** (visible, NOT persisted/resent); final answer → `content`. Streaming engines only |
| OpenAI `tools` | **Bridge to MCP**; suspended sessions keyed by **`tool_call_id`** |
| Safety posture | **Sandboxed full-auto**, with a hardened HTTP/host boundary (localhost default, env/HOME isolation, path canonicalization) |
| Process model | **Re-spawn with native resume** (map session-key → CLI session id); the MCP suspended-session is the one stateful exception |
| Build scope | **All in from v1** — MCP bridge is core, not deferred (build is ordered, not phased) |

## 3. Two constraints that shape everything

### 3.1 The consumer constraint (OpenWebUI strips metadata)
OpenWebUI and most OpenAI clients **strip all non-standard fields** (`chat_id`, `session_id`,
`user`, custom `metadata`) before forwarding to an OpenAI-compatible endpoint
(open-webui issue #11231, discussion #6999). What reliably reaches the shim is only:

- **`model`** — fully user-controlled via the dropdown we populate from `/v1/models`. The
  richest control channel.
- **`messages`** — system prompt + the **full conversation thread**, resent every turn.
- **`stream`**, plus sampling params (`temperature`, `max_tokens`, …) the agents largely ignore.

Therefore: workspace selection rides on the **`model` id** (config-driven registry; an
OpenWebUI Pipe is the escape hatch for an explicit per-request dir), and session continuity is
derived from the **conversation content** (content-hash) since no stable chat id arrives. It
also means anything we stream into **`content`** is persisted by the client and **resent** on
the next turn — which is why progress must not go there (§4.4).

### 3.2 The engine-capability constraint (the three CLIs are NOT uniform)
Verified against the installed binaries:

| | streaming events | resume by id | tool events | sandbox | final answer separable |
|---|---|---|---|---|---|
| `claude` | ✓ `--output-format stream-json` | ✓ `--continue`/`--resume <id>` | ✓ (`tool_use`) | dir confinement + tool allow/deny policy | result event distinct from tool events |
| `codex` | ✓ `exec --json` (JSONL) | ✓ `exec resume <id>` / `--last` | ✓ | ✓ 3 levels: `read-only` / `workspace-write` / `danger-full-access` | ✓ `--output-last-message <file>` |
| `agy` | ✗ **plain text only** (`--print`) | ✓ `--conversation <id>` / `--continue` | ✗ | ✗ boolean only (`--sandbox`) | only the final text exists |

Consequences baked into the design:
- **agy is a non-streaming, final-text-only adapter** in v1: no progress, no tool events,
  coarse boolean sandbox. Resume works via `--conversation <id>`.
- **claude and codex are streaming adapters** with distinguishable tool events and a separable
  final answer — which is what makes the `reasoning_content` split (§4.4) clean.
- `codex` has the richest controls (`-s` sandbox levels, `--cd`/`--add-dir`,
  `--ignore-user-config`, `--ephemeral`, `shell_environment_policy`, `--output-last-message`,
  `--output-schema`); the adapter uses them for isolation and clean output capture.

## 4. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ HTTP/API (axum)  /v1/chat/completions /v1/models /health      │
│   bearer auth · localhost default · SSE streaming · OpenAI    │
│   Chat Completions request/response types                     │
├─────────────────────────────────────────────────────────────┤
│ Model Registry   config → ModelEntry{engine,model,workspace,  │
│                  mode,permissions}; serves /v1/models          │
├─────────────────────────────────────────────────────────────┤
│ Turn Orchestrator  messages→invocation · session resolve ·    │
│                    event→OpenAI-chunk mapping · cancellation   │
├──────────────┬───────────────────────┬───────────────────────┤
│ Session Store│ Engine (enum dispatch) │ MCP Tool Bridge        │
│ key→cli sid  │ Claude · Codex · Agy   │ rmcp server + suspend  │
│ (sled/disk)  │ spawn/flags/parse      │ keyed by tool_call_id  │
├──────────────┴───────────────────────┴───────────────────────┤
│ Process Supervisor  tokio spawn · timeouts · concurrency cap  │
│ Observability       tracing, request ids, per-turn logs       │
└─────────────────────────────────────────────────────────────┘
```

Tech stack: **tokio**, **axum** (HTTP/SSE), **serde**/`serde_json` (OpenAI types),
**tokio::process** (child CLIs), **rmcp** (official Rust MCP SDK, tool bridge), **sled**
(optional session persistence), **tracing**, and `serde_yaml`/`figment` (config).

### 4.1 HTTP/API layer
- `POST /v1/chat/completions` — streaming (SSE) and non-streaming.
- `GET /v1/models` — the registry, OpenAI `{object:"list", data:[...]}` shape.
- `GET /health` — liveness.
- **Binds `127.0.0.1` by default.** Binding any non-loopback address **requires** a
  non-default bearer token to be configured (refuse to start otherwise). Bearer-token auth on
  all `/v1/*` routes.
- Maps OpenAI Chat Completions request/response/error schemas to/from internal types. Errors
  use OpenAI-shaped bodies (`{"error":{"message","type"}}`).

### 4.2 Model Registry
Loads config (section 6) into `ModelEntry { id, engine, model, workspace, mode, permissions }`
and serves them via `/v1/models`. A model id resolves to exactly one entry, which fully
determines how the turn is run. A **fingerprint** of the resolved entry (engine, model,
workspace, mode, permissions) feeds the session key (§4.5).

### 4.3 Turn Orchestrator
For each request:
1. Resolve `model` → `ModelEntry`. Unknown model → OpenAI error.
2. **If the request's messages reference `tool_call_id`s of a live suspended session
   (§4.6), route there** (tool-result follow-up). Otherwise continue.
3. Split `messages` into system prompt + conversation; compute the **session key** (§4.5).
4. Resolve session, select the engine, build the spawn command.
5. If the request carries `tools`, stand up the MCP bridge (§4.6) and wire the CLI to it.
6. Spawn via the Process Supervisor, consume normalized `AgentEvent`s, map events → OpenAI
   chunks (§4.4), stream (SSE) or accumulate into one completion.
7. On client disconnect, cancel and kill the child.

### 4.4 Engine adapters (enum dispatch — object-safe by construction)
Only three engines, so we use an **enum**, not `dyn` trait objects (avoids the object-safety
problems of `impl AsyncRead` args / `impl Stream` returns, and removes dynamic-dispatch cost):

```rust
enum Engine { Claude(ClaudeAdapter), Codex(CodexAdapter), Agy(AgyAdapter) }

struct Caps { streaming: bool, tool_events: bool, resume_by_id: bool, sandbox: SandboxKind }

impl Engine {
    fn caps(&self) -> Caps;
    fn build_command(&self, turn: &Turn, session: Option<&CliSessionId>, mcp: Option<&McpEndpoint>) -> Command;
    // object-safe stream: boxed, not `impl Stream`
    fn parse_events(&self, child: Child) -> Pin<Box<dyn Stream<Item = AgentEvent> + Send>>;
}
```

Per-engine specifics (from §3.2):
- **Claude** → `-p --output-format stream-json`, `--system-prompt`, `--model`,
  `--continue`/`--resume <id>`; sandbox via confined workdir + tool allow/deny policy.
- **Codex** → `codex exec --json` (JSONL events), `--output-last-message <file>` for the clean
  final answer, `exec resume <id>` / `--last`, `-s <level>` sandbox, `--cd`/`--add-dir`,
  isolation via `--ignore-user-config` / `--ephemeral` / `-c shell_environment_policy=...`;
  MCP via `codex mcp` config.
- **Agy** → `--print` (plain text, non-streaming), `--conversation <id>` / `--continue`,
  `--sandbox` (boolean), `--add-dir <workspace>`; MCP via `~/.gemini/config/mcp_config.json`.
  Emits a single `AssistantText` + `Done`; `caps().streaming == false`.

Normalized event type and channel mapping:

```rust
enum AgentEvent {
    AssistantText(String),    // → OpenAI `content` delta (persisted by client)
    ToolStart { name, args }, // → `reasoning_content` delta ("🔧 Editing src/foo.rs")
    ToolResult { summary },    // → `reasoning_content` delta
    ToolCall { id, name, args }, // → OpenAI `tool_calls` delta (MCP bridge, §4.6)
    SessionId(CliSessionId),   // → captured for the session store
    Error(String),             // → OpenAI error
    Done { finish_reason },     // → terminates the stream
}
```

**Channel rule (Finding 2):** only the agent's **final answer** goes to `content`; all tool
activity goes to **`reasoning_content`**, which OpenWebUI renders (collapsible) but does **not**
persist into the resent history. This keeps the conversation OpenWebUI replays — and the
content-hash computed from it — free of synthetic progress text. For non-streaming `agy`, only
the final answer exists, so it maps straight to `content`.

### 4.5 Session continuity (content-hash resume)
- **Key** = hash of (a) the conversation **projection** — roles + `content` of each message,
  excluding `reasoning_content` and progress, for the **prefix** (all messages except the final
  user turn) — **plus** (b) the **ModelEntry fingerprint** (engine, model, workspace, mode,
  permissions). The fingerprint prevents a config that reuses an id with a changed
  workspace/model/permissions from resuming the wrong CLI session.
- Lookup `key → CliSessionId`. **Hit:** spawn with the engine's resume flag, feed only the new
  user turn. **Miss:** fresh session; capture the `SessionId` event, store `key′ → sid` where
  `key′` includes the just-completed turn, so the next request (whose prefix now includes this
  turn) resolves to it.
- Store: in-memory map with optional `sled` persistence + idle-TTL GC. The server holds no
  agent state itself — the real session lives in the CLI's own on-disk store; a server restart
  loses only the key→sid index (worst case: a fresh session next turn).

### 4.6 MCP Tool Bridge (for OpenAI `tools`)
When a request carries `tools`:
1. The orchestrator starts an in-process **`rmcp` MCP server** exposing those tool definitions
   and points the spawned CLI at it (per-engine MCP config).
2. When the agent calls one, the bridge emits an OpenAI `tool_call` (with a generated
   `tool_call_id`) and **ends the turn** with `finish_reason: "tool_calls"`.
3. OpenAI's protocol means the client executes the tool and returns the result in a **follow-up
   request** containing the assistant message with that `tool_calls` entry **and** a
   `role:"tool"` message carrying the matching **`tool_call_id`** — *not* a new user turn.

**Suspended-session mechanism, keyed by `tool_call_id` (Finding 1):** the process that hit a
client tool call is **held open** (with its parked MCP call) in a `SuspendedSessions` registry
keyed by the emitted **`tool_call_id`(s)**. The orchestrator (§4.3 step 2) detects a follow-up
by matching the request's `tool` messages' `tool_call_id`s against this registry — *not* by the
conversation prefix hash, which would not match because the follow-up adds tool/assistant
messages rather than a user turn. On match, it delivers the `tool` result into the waiting MCP
call and the same process continues. A timeout reaps orphaned suspensions (client never returns
the result, or retries instead) by killing the child; the next turn then starts fresh.

This is the only place a process outlives a single HTTP request: bounded (only with `tools`),
keyed by `tool_call_id`, and time-limited.

### 4.7 Process Supervisor
Spawns child CLIs via `tokio::process`; per-turn timeouts (→ `504`); a global concurrency cap
with a bounded queue; cancellation/kill on client disconnect or shutdown.

### 4.8 Safety / permissions (hardened — Finding 4)
The agent posture is **sandboxed full-auto**, but because this is an HTTP service that triggers
file edits and command execution, the boundary is hardened on multiple axes:

- **Network:** bind `127.0.0.1` by default; non-loopback bind requires an explicit non-default
  bearer token or the server refuses to start. Auth on all `/v1/*` routes.
- **Host/identity isolation:** spawn each CLI with an isolated env — scrub/whitelist env vars,
  set a per-engine `HOME`/`CODEX_HOME`/config dir so a request can't read the operator's
  unrelated secrets; codex uses `--ignore-user-config` + `--ephemeral` +
  `-c shell_environment_policy.inherit=...`.
- **Workspace confinement:** canonicalize the configured workspace path, reject symlinks that
  escape it, and pass it as the engine's only writable root (`--cd`/`--add-dir`/confined dir).
- **Per-engine sandbox is documented, not assumed uniform:** codex `read-only` /
  `workspace-write` / `danger-full-access`; claude tool allow/deny + confined dir; agy coarse
  boolean `--sandbox`. The config's `permissions` maps to the strongest available control per
  engine, and the docs state each engine's actual guarantee (the sandbox strength is the
  *engine's*, not ours).
- **`mode: text`** replicates the PoC: empty temp dir, all tools blocked — a safe pure-generator.

## 5. Data flow

**Non-streaming chat completion** — resolve model → entry; compute session key; resolve session;
spawn; consume `AgentEvent`s to completion; assemble (final text → `content`, tool activity →
`reasoning_content`); return one `chat.completion`.

**Streaming chat completion** — same setup, respond `text/event-stream`; each `AgentEvent` → a
`chat.completion.chunk` (`delta.content` for final text, `delta.reasoning_content` for progress,
`delta.tool_calls` when bridging); `[DONE]` at `Done`; client disconnect cancels the child.
(`agy`: a single final-text chunk, no progress.)

**Tool-call round trip (MCP bridge)** — request with `tools` → MCP server up, CLI wired in;
agent calls a tool → emit `tool_calls` (with `tool_call_id`), finish `tool_calls`, **suspend**
the process keyed by that id; client runs the tool, sends a follow-up with the matching
`role:"tool"` result; orchestrator matches the `tool_call_id` to the suspension, delivers the
result into the parked MCP call, the agent continues, the response resumes.

## 6. Configuration

```yaml
server:
  bind: "127.0.0.1:8088"        # non-loopback bind requires a real bearer_token
  bearer_token: "sk-..."        # required if bind is non-loopback
defaults:
  timeout_s: 600
  max_concurrency: 4
  session_store: { backend: sled, path: ~/.llm-bridge/sessions, ttl_h: 24 }
  isolation: { ephemeral_home: true, env_passthrough: ["PATH", "LANG"] }
models:
  - id: "claude-opus-repoA"
    engine: claude               # claude | codex | agy
    model: opus                  # passed to the engine's --model
    workspace: /work/repoA       # canonicalized; symlink-escapes rejected
    mode: agentic                # agentic | text
    permissions: workspace-write # mapped to the strongest equivalent per engine
  - id: "codex-gpt5-repoB"
    engine: codex
    model: gpt-5
    workspace: /work/repoB
    mode: agentic
    permissions: workspace-write
  - id: "claude-sonnet-textgen"
    engine: claude
    model: sonnet
    mode: text                   # PoC-style pure generator, no workspace
```

Model ids are plain ASCII (e.g. `claude-opus-repoA`) for clean rendering in the OpenWebUI
dropdown. The model id is the entire control surface for OpenWebUI; the registry maps it to
everything else.

## 7. Error handling, concurrency, observability
- OpenAI-shaped error bodies; CLI nonzero exit → `500` with stderr; timeout → `504`; unknown
  model → `404`; bad request → `400`; missing/invalid token → `401`.
- Concurrency cap with a bounded queue; reject or queue past the limit.
- `tracing` spans per turn with a request id; structured logs of the spawn command (secrets
  redacted), engine, model, session hit/miss, duration, exit status.
- Graceful shutdown drains/kills children and any suspended sessions.

## 8. Testing strategy
- **Engine parsers** tested against **recorded CLI fixtures** (captured claude `stream-json`,
  `codex exec --json` JSONL, `agy --print` text) — no live logins in CI.
- **Orchestrator** tested with a `FakeEngine` producing scripted `AgentEvent`s (session resume,
  streaming channel mapping, `reasoning_content` vs `content` split, tool-call
  suspension/resume by `tool_call_id`, cancellation).
- **MCP bridge** tested with an in-process `rmcp` client driving a scripted suspension round
  trip (including the `tool_call_id` routing and the orphan timeout).
- **Session key** tested for fingerprint sensitivity (same id + changed workspace ⇒ different
  key) and prefix-rotation correctness.
- **End-to-end smoke** behind a feature flag that shells out to a real installed CLI (local,
  not CI).

## 9. Build sequence (ordered, all in v1)
1. Scaffold, config loader, model registry, `/health` + `/v1/models`, localhost bind + auth.
2. `Engine` enum + `FakeEngine`; Turn Orchestrator; non-streaming `/v1/chat/completions`.
3. Process Supervisor (spawn, isolation/env scrub, timeout, concurrency, cancellation).
4. `ClaudeAdapter` against recorded fixtures; real non-streaming end to end.
5. SSE streaming + event→chunk mapping (final → `content`, progress → `reasoning_content`).
6. Session Store + content-hash resume (projection + ModelEntry fingerprint).
7. `CodexAdapter` (JSONL + `--output-last-message`, sandbox levels) and `AgyAdapter`
   (non-streaming, `--conversation` resume).
8. MCP Tool Bridge + suspended sessions keyed by `tool_call_id` (OpenAI `tools` round trip).
9. Security/sandbox hardening pass + per-engine sandbox docs, observability polish, e2e smoke.

## 10. Open questions / risks
- **MCP wiring per engine** differs (config file vs flag); each adapter owns its setup/teardown.
  Verify each CLI can point at an ad-hoc local MCP server per invocation, especially agy's
  `mcp_config.json` path and whether it honors per-invocation MCP servers in `--print` mode.
- **Suspended-session vs client retry:** if OpenWebUI retries instead of returning the tool
  result, the suspension times out and the next turn starts fresh (acceptable degradation).
- **agy depth:** confirmed non-streaming with `--conversation` resume and boolean sandbox;
  validate that `--conversation <id>` reliably resumes in `--print` mode and that the session id
  is discoverable (it isn't in stdout like claude/codex — may require reading agy's session dir).
- **Sandbox strength is the engine's, not ours:** "full-auto" trusts each CLI's sandbox; agy's
  coarse boolean sandbox is the weakest and should be documented as such.
