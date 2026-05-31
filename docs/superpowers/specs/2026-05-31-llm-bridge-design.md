# llm-bridge — Design

**Date:** 2026-05-31
**Status:** Approved for planning

## 1. Purpose

`llm-bridge` is a Rust service that exposes a **full OpenAI-compatible HTTP API** in front of
local **agentic coding CLIs** — `claude` (Claude Code / Anthropic), `codex` (OpenAI Codex),
and `agy` (Google Antigravity / Gemini). It lets any OpenAI-compatible client (OpenWebUI,
LiteLLM, scripts) drive these agents — including their real powers: reading and editing files,
running commands, and holding multi-turn sessions — by selecting a "model".

It supersedes the Python proof-of-concept
(`autoid/autoid-ai-poc/scripts/claude_shim.py`), which wrapped only `claude -p` as a
**pure text generator** with all tools disabled. `llm-bridge` instead does **full agentic
passthrough**, supports all three engines behind one uniform interface, streams the agent's
work as it happens, and bridges OpenAI tool-calling to MCP.

### Goals
- One OpenAI-compatible endpoint fronting three different agentic CLIs.
- Pick engine + underlying model + workspace + mode + permissions by selecting a model id.
- Real agentic behavior (file edits, command execution) under a uniform safety posture.
- Multi-turn continuity that survives a stateless-looking HTTP API.
- Stream the agent's tool activity to the client as readable progress.
- Honor OpenAI `tools` (function calling) by bridging to MCP.

### Non-goals
- Reimplementing the agents or talking to model vendor APIs directly. We always shell out to
  the installed CLI and rely on its existing local login/auth.
- A web UI of our own. Clients (OpenWebUI, etc.) provide the UI.
- Provider features the CLIs don't support (e.g. logprobs, embeddings).

## 2. Key decisions (from brainstorming)

| Decision | Choice |
|---|---|
| Scope | **Full agentic passthrough** — expose file edits, tool use, sessions |
| Workspace selection | **Config-driven model list**: a config registers named tuples, each published as a model in `/v1/models`; OpenWebUI Pipe is the escape hatch for explicit per-request dirs |
| Session continuity | **Content-hash resume**: hash the conversation prefix → resume the CLI's own on-disk session, else start fresh |
| Streaming output | **Progress + final answer**: map tool activity to readable content deltas, interleaved with the answer |
| OpenAI `tools` | **Bridge to MCP**: host client tools as an MCP server the agent connects to; round-trip `tool_calls` back to the client |
| Safety posture | **Always sandboxed full-auto**: every agentic model runs auto-approve inside each CLI's native sandbox |
| Process model | **Re-spawn with native resume**: map conversation-hash → CLI session id; spawn fresh each turn with the engine's resume flag |
| Build scope | **All in from v1** — MCP bridge is core, not deferred (build is ordered, not phased) |

## 3. The consumer constraint that shapes everything

OpenWebUI and most OpenAI clients **strip all non-standard fields** (`chat_id`, `session_id`,
`user`, custom `metadata`) before forwarding to an OpenAI-compatible endpoint
(open-webui issue #11231, discussion #6999). What reliably reaches the shim is only:

- **`model`** — fully user-controlled via the dropdown we populate from `/v1/models`. The
  richest control channel.
- **`messages`** — system prompt + the **full conversation thread**, resent every turn.
- **`stream`**, plus sampling params (`temperature`, `max_tokens`, …) the agents largely ignore.

Therefore:
- **Workspace selection rides on the `model` id** (config-driven registry). Power users can
  still inject an explicit per-request workspace via an OpenWebUI **Pipe** (documented escape
  hatch); that path may set a non-standard field the shim reads when present.
- **Session continuity is derived from the conversation content** (content-hash), since no
  stable chat id arrives.

## 4. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ HTTP/API (axum)  /v1/chat/completions /v1/models /health      │
│   bearer auth · SSE streaming · OpenAI request/response types │
├─────────────────────────────────────────────────────────────┤
│ Model Registry   config → ModelEntry{engine,model,workspace,  │
│                  mode,permissions}; serves /v1/models          │
├─────────────────────────────────────────────────────────────┤
│ Turn Orchestrator  messages→invocation · session resolve ·    │
│                    event→OpenAI-chunk mapping · cancellation   │
├──────────────┬───────────────────────┬───────────────────────┤
│ Session Store│ Agent Adapters (trait) │ MCP Tool Bridge        │
│ hash→cli sid │ Claude · Codex · Agy   │ rmcp server + suspend  │
│ (sled/disk)  │ spawn/flags/parse      │ pending tool_calls      │
├──────────────┴───────────────────────┴───────────────────────┤
│ Process Supervisor  tokio spawn · timeouts · concurrency cap  │
│ Observability       tracing, request ids, per-turn logs       │
└─────────────────────────────────────────────────────────────┘
```

Tech stack: **tokio** (async runtime), **axum** (HTTP/SSE), **serde**/`serde_json` (OpenAI
types), **tokio::process** (child CLIs), **rmcp** (official Rust MCP SDK, for the tool bridge),
**sled** (optional session persistence), **tracing** (observability), **figment** or
`serde_yaml` (config).

### 4.1 HTTP/API layer
- `POST /v1/chat/completions` — streaming (SSE, `text/event-stream`) and non-streaming.
- `GET /v1/models` — the registry, OpenAI `{object:"list", data:[...]}` shape.
- `GET /health` — liveness.
- Bearer-token auth (configurable; mirrors the PoC's `sk-noauth` default for trusted networks).
- Maps OpenAI request/response/error schemas to/from internal types. Errors use OpenAI-shaped
  bodies (`{"error":{"message","type"}}`).

### 4.2 Model Registry
Loads the config (section 6) into `ModelEntry { id, engine, model, workspace, mode,
permissions }` and serves them via `/v1/models`. A model id resolves to exactly one entry,
which fully determines how the turn is run.

### 4.3 Turn Orchestrator
The center of the system. For each request:
1. Resolve `model` → `ModelEntry`. Reject unknown models with an OpenAI error.
2. Split `messages` into system prompt + conversation; compute the **prefix hash** (all but
   the last user turn) for session lookup.
3. Resolve session (§4.5), select the `Adapter`, build the spawn command.
4. If the request carries `tools`, stand up the MCP bridge (§4.6) and wire the CLI to it.
5. Spawn the child via the Process Supervisor, consume its normalized `AgentEvent` stream,
   map events → OpenAI chunks (§4.4), and either stream them (SSE) or accumulate them into a
   single completion response.
6. On client disconnect, cancel and kill the child.

### 4.4 Agent Adapters (the core abstraction)
One implementation per CLI, isolating that tool's quirks behind a uniform interface:

```rust
trait Adapter {
    fn build_command(&self, turn: &Turn, session: Option<&CliSessionId>) -> Command;
    fn parse_events(&self, stdout: impl AsyncRead) -> impl Stream<Item = AgentEvent>;
    fn capabilities(&self) -> Caps; // streaming format, resume flag, sandbox flag, mcp config
}
```

Per-engine specifics:
- **Claude** → `-p --output-format stream-json`, `--system-prompt`, `--model`,
  `--continue`/`--resume <id>`, runs in the workspace dir; sandbox via confined dir + tool policy.
- **Codex** → `codex exec` (streaming), `resume --last` / session id, native `sandbox`,
  MCP via `codex mcp` config.
- **Agy** → `--print` / `--prompt-interactive`, `--conversation <id>`, `--sandbox`,
  `--add-dir <workspace>`; MCP via `~/.gemini/config/mcp_config.json`.

Normalized event type:

```rust
enum AgentEvent {
    AssistantText(String),       // → OpenAI content delta
    ToolStart { name, args },     // → readable progress delta ("🔧 Editing src/foo.rs")
    ToolResult { summary },       // → optional progress delta
    SessionId(CliSessionId),      // → captured for the session store
    Error(String),                // → OpenAI error
    Done { finish_reason },       // → terminates the stream
}
```

The orchestrator renders `ToolStart`/`ToolResult` as readable progress lines interleaved with
`AssistantText`, giving OpenWebUI a live view of the agent working, ending with the final answer.

### 4.5 Session continuity (content-hash resume)
- Key: stable hash of the serialized conversation **prefix** (every message except the final
  user turn), plus the model id (so different agents/workspaces don't collide).
- Lookup `hash → CliSessionId` in the Session Store. **Hit:** spawn with the engine's resume
  flag and feed only the new user turn. **Miss:** start a fresh session; capture the
  `SessionId` event and store `hash′ → sid` where `hash′` includes the just-completed turn, so
  the *next* request (whose prefix now includes this turn) resolves to it.
- Store: in-memory `HashMap` with optional `sled` persistence and idle-TTL GC. The server holds
  no agent state itself — the real session lives in the CLI's own on-disk store, so a server
  restart loses only the hash→sid index (worst case: a fresh session next turn).

### 4.6 MCP Tool Bridge (for OpenAI `tools`)
When a request carries `tools`:
1. The orchestrator starts an in-process **`rmcp` MCP server** exposing those tool definitions
   and points the spawned CLI at it (per-engine MCP config).
2. When the agent calls one, the bridge emits an OpenAI `tool_call` and **ends the turn** with
   `finish_reason: "tool_calls"`.
3. OpenAI's protocol means the client executes the tool and returns the result in a **follow-up
   request** (a `role:"tool"` message). But the live CLI process is blocked *inside* the MCP
   call awaiting a synchronous return.

**Suspended-session mechanism (the one stateful exception):** the process that hit a client
tool call is **held open**, with its parked MCP call, keyed by conversation hash, in a
`SuspendedSessions` registry. When the follow-up request arrives carrying the `tool` result,
the bridge delivers it into the waiting MCP call and the same process continues. A timeout
reaps orphaned suspensions (client never returns the result) by killing the child.

This is the only place a process outlives a single HTTP request; it is bounded (only when the
client passes `tools`), keyed, and time-limited.

### 4.7 Process Supervisor
Spawns child CLIs via `tokio::process`, enforces per-turn timeouts (→ `504`), a global
concurrency cap with a bounded queue, and cancellation/kill on client disconnect or shutdown.

### 4.8 Safety / permissions
Posture is **always sandboxed full-auto**: every agentic model runs with the engine's
auto-approve + native sandbox (`codex sandbox`, `agy --sandbox`, `claude` confined to its
workspace dir with an allow policy). `mode: text` entries replicate the PoC behavior — empty
temp dir, all tools blocked — for safe pure-generation models.

## 5. Data flow

**Non-streaming chat completion**
1. Client `POST /v1/chat/completions` (`stream:false`).
2. Resolve model → entry; hash prefix; resolve session; build command.
3. Spawn child; consume `AgentEvent`s to completion; accumulate progress + final text.
4. Return one OpenAI `chat.completion` with the assembled `content` and `finish_reason`.

**Streaming chat completion**
1. Same setup; respond `text/event-stream`.
2. Each `AgentEvent` → an OpenAI `chat.completion.chunk` (`delta.content` for text/progress;
   `delta.tool_calls` when bridging). Emit `[DONE]` at `Done`.
3. Client disconnect cancels the child.

**Tool-call round trip (MCP bridge)**
1. Request with `tools` → MCP server up, CLI wired to it.
2. Agent calls a tool → emit `tool_calls`, finish `tool_calls`, **suspend** the process.
3. Client runs the tool, sends follow-up with the `tool` result.
4. Bridge resolves the suspended session, delivers the result into the MCP call, agent
   continues, response resumes streaming.

## 6. Configuration

```yaml
auth:
  bearer_token: "sk-..."        # default permissive for trusted LANs, like the PoC
defaults:
  timeout_s: 600
  max_concurrency: 4
  session_store: { backend: sled, path: ~/.llm-bridge/sessions, ttl_h: 24 }
models:
  - id: "claude-opus·repoA"
    engine: claude               # claude | codex | agy
    model: opus                  # passed to the engine's --model
    workspace: /work/repoA
    mode: agentic                # agentic | text
    permissions: full-auto-sandbox
  - id: "codex-gpt5·repoB"
    engine: codex
    model: gpt-5
    workspace: /work/repoB
    mode: agentic
    permissions: full-auto-sandbox
  - id: "claude-sonnet·textgen"
    engine: claude
    model: sonnet
    mode: text                   # PoC-style pure generator, no workspace
```

The model id is the entire control surface for OpenWebUI; the registry maps it to everything
else.

## 7. Error handling, concurrency, observability
- OpenAI-shaped error bodies; CLI nonzero exit → `500` with stderr; timeout → `504`;
  unknown model → `404`/`400`.
- Concurrency cap with a bounded queue; reject or queue past the limit.
- `tracing` spans per turn with a request id; structured logs of spawn command (redacted),
  engine, model, session hit/miss, duration, exit status.
- Graceful shutdown drains/kills children.

## 8. Testing strategy
- **Adapter parsers** tested against **recorded CLI fixtures** (captured `stream-json` / `codex
  exec` / `agy` output) — no live logins needed in CI.
- **Orchestrator** tested with a `FakeAdapter` producing scripted `AgentEvent`s (session
  resume, streaming mapping, tool-call suspension/resume, cancellation).
- **MCP bridge** tested with an in-process `rmcp` client driving a scripted suspension round
  trip.
- **End-to-end smoke** behind a feature flag that shells out to a real installed CLI (run
  locally, not in CI).

## 9. Build sequence (ordered, all in v1)
1. Project scaffold, config loader, model registry, `/health` + `/v1/models`.
2. `Adapter` trait + `FakeAdapter`; Turn Orchestrator; non-streaming `/v1/chat/completions`.
3. Process Supervisor (spawn, timeout, concurrency, cancellation).
4. `ClaudeAdapter` against recorded fixtures; real non-streaming end to end.
5. SSE streaming + event→chunk mapping (progress + final answer).
6. Session Store + content-hash resume.
7. `CodexAdapter` and `AgyAdapter`.
8. MCP Tool Bridge + suspended sessions (OpenAI `tools` → MCP round trip).
9. Observability polish, sandbox/permission profiles, docs, e2e smoke test.

## 10. Open questions / risks
- **MCP wiring per engine** differs (config file vs flag); each adapter must own its setup and
  teardown. Verify each CLI can point at an ad-hoc local MCP server per invocation.
- **Suspended-session lifetime** vs. client retry semantics: if OpenWebUI retries instead of
  returning the tool result, the suspension times out and the next turn starts fresh.
- **Resume-flag fidelity**: confirm each CLI resumes by id reliably in non-interactive mode and
  that `SessionId` is observable in its output stream.
- **Sandbox strength** is the engine's, not ours; "full-auto" trusts each CLI's sandbox.
