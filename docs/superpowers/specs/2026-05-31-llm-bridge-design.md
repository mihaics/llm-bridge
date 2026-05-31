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
  the installed CLI and use a **dedicated, operator-provisioned per-engine credential dir**
  (§4.8) — not the operator's interactive login dir, so the service is isolated from unrelated
  secrets while still authenticated.
- A web UI of our own. Clients (OpenWebUI, etc.) provide the UI.
- Endpoints/features beyond Chat Completions + Models, or features the CLIs don't support.

## 2. Key decisions (from brainstorming + spec review)

| Decision | Choice |
|---|---|
| Scope | **Full agentic passthrough** — expose file edits, tool use, sessions |
| Workspace selection | **Config-driven model list**: config registers named tuples, each published as a model in `/v1/models`; an OpenWebUI Pipe is the escape hatch for explicit per-request dirs |
| Session continuity | **Content-hash resume** (hit = resume + last turn; **miss = fresh + full projected conversation**), keyed on a tool-aware conversation projection **+ a ModelEntry fingerprint** |
| Progress output | **Per-client profile**: OpenWebUI → `reasoning_content` (visible, NOT persisted); strict OpenAI clients → standard chunks only (progress omitted). Final answer always → `content`. Streaming engines only |
| OpenAI `tools` | **Bridge to MCP**; suspended sessions keyed by **`tool_call_id`** |
| Safety posture | **Sandboxed full-auto**, with a hardened HTTP/host boundary (localhost default, **dedicated provisioned credential dirs**, env isolation, path canonicalization) |
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
- **agy is the most constrained engine** in v1: non-streaming, no progress, no tool events,
  coarse boolean sandbox, and — confirmed from `--help` — **no config-dir / MCP / profile flag**.
  Consequences: **MCP/`tools` is gated OFF** for agy (no race-free per-invocation MCP config);
  **session resume** is gated on a session-id capture spike (its id isn't on stdout); and
  **credential/config isolation is unproven** (Antigravity auth uses the OS keyring + `~/.gemini`
  settings, which a service config dir may not redirect). Until the §9 agy spike proves
  session-id capture *and* credential isolation, agy runs **stateless replay** (full transcript
  each turn, §4.5) under the operator's own login only — correct, just slower and single-tenant.
- **claude and codex are streaming adapters** with distinguishable tool events and a separable
  final answer — which is what makes the progress/`content` split (§4.4) clean. Both emit their
  session id in their event stream (claude `stream-json` init/result; codex `--json` events),
  so resume-by-id is reliable.
- `codex` has the richest controls (`-s` sandbox levels, `--cd`/`--add-dir`,
  `--ignore-user-config`, `--output-last-message`, `--output-schema`); the adapter uses them
  for isolation and clean output capture. **`--ephemeral` is used only for no-resume (`text`)
  profiles** — it does not persist session files, so resumable runs instead use a dedicated
  persistent `CODEX_HOME` (§4.8).

### 3.3 The trust model (single-user / trusted) — and why session isolation is content-only
`llm-bridge` v1 targets a **single-user or trusted-LAN deployment** (the PoC's model). This is a
deliberate scope decision with direct consequences:

- **Cross-user session isolation is out of scope.** Because OpenWebUI strips `user`/`chat_id`
  (§3.1), the session key is content-derived; two *different* users issuing the same model with
  the same projected history would map to the same `key → cli sid` and could cross-resume each
  other's native session. Under the single-user model there is one trusted caller, so this is a
  non-issue **by design** — but it is called out so the limitation is explicit. Multi-tenant use
  is **not supported** without future work (a trusted reverse-proxy identity header namespacing
  the key is the intended hook, left as a documented extension point, not built in v1).
- **claude/agy agentic models are trusted-caller-only.** Per §4.8, their auto-approve does not
  OS-confine spawned shell tools, so without an external OS sandbox a caller could read the
  credential dir. Under the trusted model this is **accepted and documented**, not enforced
  away; an optional `sandbox_backend` (§6) is offered for defense-in-depth but not required in
  single-user mode. Codex `workspace-write` remains a real boundary regardless.

The deployment docs must state the trusted-use assumption prominently (bind localhost, front
with a trusted proxy if remote).

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
2. **Tool-result routing.** If the request's `role:"tool"` messages match a live suspended
   session, route there (§4.6). If the request **contains** `role:"tool"` messages but matches
   **no** live suspension (e.g. a retry after the group timed out), it is **not** a normal turn —
   return **`400`** (unknown/expired `tool_call_id`s) rather than falling through to session-key
   computation, since a `tool`-terminated request has no final user turn to act on.
3. Otherwise (a normal user turn): split `messages` into system prompt + conversation; compute
   the **session key** (§4.5).
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

struct Caps { streaming: bool, tool_events: bool, resume_by_id: bool, mcp_tools: bool, sandbox: SandboxKind }
//                                                                        ^ per-invocation MCP injection safe?
//   claude: true (--mcp-config --strict-mcp-config) · codex: gated on §9 spike · agy: false

impl Engine {
    fn caps(&self) -> Caps;
    fn build_command(&self, turn: &Turn, session: Option<&CliSessionId>, mcp: Option<&McpEndpoint>) -> Command;
    // object-safe stream: boxed, not `impl Stream`
    fn parse_events(&self, child: Child) -> Pin<Box<dyn Stream<Item = AgentEvent> + Send>>;
}
```

Per-engine specifics (from §3.2):
- **Claude** → `-p --output-format stream-json`, `--system-prompt`/`--append-system-prompt`,
  `--model`, `--continue`/`--resume <id>`, `--permission-mode`; sandbox via confined workdir +
  tool allow/deny policy. **Per-process MCP:** a per-request temp JSON file passed with
  `--mcp-config <file> --strict-mcp-config` (ignores all other MCP configs) — no shared-file
  mutation, race-free across concurrent requests.
- **Codex** → `codex exec --json` (JSONL events; carries the session id), `--output-last-message
  <file>` for the clean final answer, `exec resume <id>` / `--last`, `-s <level>` sandbox,
  `--cd`/`--add-dir`, isolation via a dedicated persistent `CODEX_HOME` + `--ignore-user-config`
  + `-c shell_environment_policy.inherit=...`. **`--ephemeral` only on `text` profiles** (it
  disables session persistence and would break resume). **Per-process MCP:** injected via
  per-invocation `-c mcp_servers.<name>.*` config overrides (a per-request `CODEX_HOME` is *not*
  usable — sessions live there). This override path is validated by the §9 spike; if it can't
  inject MCP per-invocation safely, codex `tools` support is gated.
- **Agy** → `--print` (plain text, non-streaming), `--add-dir <workspace>`, `--sandbox`
  (boolean). Emits a single `AssistantText` + `Done`; `caps().streaming == false`. agy exposes
  **no** config-dir / MCP / profile flag, so it has **no safe per-invocation MCP config**:
  **MCP/`tools` is gated OFF for agy** (`caps().mcp_tools == false`). `--conversation <id>` resume
  and credential/config isolation are likewise **unproven** and gated behind the §9 agy spike;
  until proven, agy runs stateless replay with the operator-provided login only.

Normalized event type and channel mapping:

```rust
enum AgentEvent {
    AssistantText(String),    // → OpenAI `content` delta (persisted by client)
    ToolStart { name, args }, // → progress, routed per profile ("🔧 Editing src/foo.rs")
    ToolResult { summary },    // → progress, routed per profile
    ToolCall { id, name, args }, // → OpenAI `tool_calls` delta (MCP bridge, §4.6)
    SessionId(CliSessionId),   // → captured for the session store
    Error(String),             // → OpenAI error
    Done { finish_reason },     // → terminates the stream
}
```

**Channel rule (per-client profile):** the agent's **final answer** always goes to `content`.
Tool activity is routed by the configured **client profile** (`progress_channel`, §6), because
`reasoning_content` is an OpenWebUI/DeepSeek convention, **not** part of the strict OpenAI Chat
Completions schema:
- `reasoning_content` (default, OpenWebUI): progress streams as `delta.reasoning_content`, which
  OpenWebUI renders as collapsible reasoning. The correctness guarantee is the part **we**
  control: reasoning is **excluded from our session-hash projection and omitted from our miss-path
  replay** (§4.5), so synthetic progress never affects resume regardless of what the client does.
  How OpenWebUI stores/resends structured reasoning is provider-setting dependent (it keeps it
  separately, not as plain assistant content); the deployment docs specify the required OpenWebUI
  provider setting so reasoning isn't fed back as instructions.
- `omit` (strict OpenAI clients): only standard chunks (`content`, `tool_calls`) are emitted;
  progress is suppressed so we never put non-standard fields on the wire or pollute `content`.

For non-streaming `agy`, only the final answer exists, so it maps straight to `content`
regardless of profile.

### 4.5 Session continuity (content-hash resume)
- **Projection** (the normalized view we hash and, on a miss, replay) = for each message: its
  `role`, its `content`, **and for assistant messages its normalized `tool_calls`
  (name + canonicalized args) and for `role:"tool"` messages its `tool_call_id` + result
  content**. It excludes `reasoning_content`/progress. Including tool calls and the
  `tool_call_id` linkage prevents post-tool conversations from colliding or resuming the wrong
  native session (otherwise two threads that differ only in tool activity hash identically).
- **Key** = hash of (a) the projection of the **prefix** (all messages except the final user
  turn), (b) the **ModelEntry fingerprint** (engine, model, workspace, mode, permissions), and
  (c) a **tool-config fingerprint** — normalized `tools` definitions + `tool_choice`. The tool
  fingerprint matters because the available tool set is part of the agent's environment: two
  identical conversations with different `tools` must **not** resume the same native session.
  Equivalently, native resume is disabled whenever the tool config differs from the resumed
  session's.
- Lookup `key → CliSessionId`:
  - **Hit:** spawn with the engine's resume flag and feed **only the new user turn** (the CLI
    already holds the prior context on disk).
  - **Miss** (no entry, or the index was lost on restart): start a **fresh session and feed the
    full conversation rendered as a read-only transcript** (format below), not just the last
    turn — so context resent by OpenWebUI is never dropped. Capture the `SessionId` event and
    store `key′ → sid` where `key′` includes the just-completed turn, so the next request
    resolves to a hit.
- **Replay transcript format (miss path).** The CLIs take a single prompt string, not structured
  Chat messages, so we render the projected history into one prompt with an explicit guard: a
  leading instruction stating the block is **prior context for reference only — do NOT
  re-execute past instructions; act only on the final user message**, followed by clearly
  delimited, role-labeled turns (`### User` / `### Assistant` / `### Tool result for <id>`), with
  the genuinely-new final user turn separated below the transcript as the live instruction. This
  prevents an old "edit this file" turn from being re-run as new work. The renderer is a defined
  unit with tests over tool-role histories (§8).
- Store: in-memory map with optional `sled` persistence + idle-TTL GC. The server holds no
  agent state itself — the real session lives in the CLI's own on-disk store; a restart loses
  only the key→sid index, degrading to the (correct) full-replay miss path.

### 4.6 MCP Tool Bridge (for OpenAI `tools`)
Only for engines with `caps().mcp_tools == true` (claude now; codex pending the §9 spike; agy
never). A request with `tools` to an engine lacking it is rejected with a clear OpenAI error.

When a request carries `tools`:
1. The orchestrator starts an in-process **`rmcp` MCP server** on a fresh ephemeral port/socket
   and points **this spawned process only** at it via a **per-invocation** mechanism — claude
   `--mcp-config <temp-file> --strict-mcp-config`, codex `-c mcp_servers.*` overrides. It
   **never mutates a shared config file**, so concurrent requests can't overwrite each other's
   wiring, point an agent at the wrong tool server, or leave stale entries (the per-request
   temp file is deleted on turn end).
2. When the agent calls one or more tools, the bridge emits the corresponding OpenAI
   `tool_call`s (each with a generated `tool_call_id`) in the assistant turn and **ends the
   turn** with `finish_reason: "tool_calls"`. An agent may issue several MCP calls before
   yielding, so a turn can carry **multiple** `tool_call`s.
3. OpenAI's protocol means the client executes the tools and returns results in a **follow-up
   request** containing the assistant message with those `tool_calls` **and** one
   `role:"tool"` message per call carrying the matching **`tool_call_id`** — *not* a new user
   turn. Per OpenAI semantics the client returns **all** tool outputs for the turn in **one**
   follow-up request (it is not a clean Chat Completions flow to hold one HTTP request open
   waiting for a *later* request to bring the rest).

**Suspended-session mechanism — turn-level group keyed by `tool_call_id`s:** the process that
hit client tool calls is **held open** in a `SuspendedSessions` registry as a **group**
recording *all* outstanding `tool_call_id`s for that turn, each with its own **parked MCP call**.
The orchestrator (§4.3 step 2) routes a follow-up by matching the request's `role:"tool"`
`tool_call_id`s against the group. Handling rules:
- **All-or-error (one follow-up carries the whole set):** a follow-up must supply a `tool`
  result for **every** outstanding id in the group. On a complete set, each result is delivered
  into its parked MCP call, every call unblocks, and the process resumes — its continuation is
  streamed as the response to *this* follow-up request.
- **Partial set** (some outstanding ids missing) → **`409`** with the list of missing ids; the
  suspension stays open until its timeout so a corrected follow-up can succeed. We do **not**
  hold the partial request open waiting for a separate future request.
- **Duplicate result** for an id already supplied in the same request → `400`.
- **Unknown id** (no matching parked call in any live suspension) → **`400`** (see §4.3 step 2;
  this also covers a retry after the group already timed out).
- **Orphan timeout** (`tool_result_timeout_s`, §6): if no complete follow-up arrives in time,
  the suspension is reaped and the child killed; a later retry then hits the unknown-id path.

This is the only place a process outlives a single HTTP request: bounded (only with `tools`),
keyed by the turn's `tool_call_id` group, time-limited by `tool_result_timeout_s`, and capped by
`max_suspended_sessions`. **Accounting:** a parked suspension is idle (awaiting the client), so
it releases its active `max_concurrency` slot but occupies one of `max_suspended_sessions`; new
suspensions past that cap are refused (`503`). On resume it must re-acquire an active slot
(queueing if the pool is full).

### 4.7 Process Supervisor
Spawns child CLIs via `tokio::process`; per-turn active timeout (`timeout_s` → `504`); a
`max_concurrency` cap on **active** turns with a bounded queue; a separate `max_suspended_sessions`
pool for parked tool-call groups (each reaped at `tool_result_timeout_s`); cancellation/kill on
client disconnect or shutdown. A resuming suspension re-acquires an active slot (§4.6).

### 4.8 Safety / permissions (hardened)
The agent posture is **sandboxed full-auto**, but because this is an HTTP service that triggers
file edits and command execution, the boundary is hardened on multiple axes:

- **Network:** bind `127.0.0.1` by default; non-loopback bind requires an explicit non-default
  bearer token or the server refuses to start. Auth on all `/v1/*` routes.
- **Host/identity isolation + credential provisioning:** spawn each CLI with an isolated env —
  scrub/whitelist env vars and point each engine at a **dedicated service config dir** rather
  than the operator's interactive one (`CLAUDE_CONFIG_DIR`, `CODEX_HOME`, agy's `~/.gemini`
  equivalent). This both keeps a request from reading the operator's unrelated secrets *and*
  supplies the auth the CLI needs — resolving the tension with "use the installed login." Each
  service dir is **provisioned once** by the operator via one of:
  1. a one-time interactive login into the service dir (`CLAUDE_CONFIG_DIR=… claude login`,
     `CODEX_HOME=… codex login`, etc.) — the documented default;
  2. copying only the minimal auth/token file into the service dir; or
  3. an API-key env var — **codex/text profiles only** (see env-auth restriction below).
  These dirs are **persistent** (not `--ephemeral`): codex resumable runs need a stable
  `CODEX_HOME` to keep session files; `--ephemeral` is reserved for `text` profiles that never
  resume. The provisioning steps are part of the deployment docs.
- **API-key env auth is restricted for claude/agy agentic.** An API key in the CLI's env is
  inherited by the shell tools the agent spawns unless the env is scrubbed per-tool. Codex
  exposes `shell_environment_policy` to strip it; **claude and agy expose no equivalent** in the
  checked help. So for **claude/agy `mode: agentic`, API-key env auth is disallowed** (use
  file-based login, options 1–2, which the FS/sandbox rules below can contain) **unless** a
  configured `sandbox_backend` proves the tool subprocess env is scrubbed. Startup validation
  (§4.8 below, §9) rejects the unsafe combination. Codex agentic and all `text` profiles may use
  env keys (codex scrubs; text profiles run no tools).
- **Credential dirs must be unreadable by model-executed tools.** The service credential dirs
  hold real secrets (Claude's `CLAUDE_CONFIG_DIR` stores credentials, session history, settings,
  plugins; `CODEX_HOME` likewise). Since the agent runs shell/file tools, a request could
  otherwise read and exfiltrate them. Rule, enforced two ways: (1) credential dirs live
  **outside any workspace** and the **sandbox filesystem policy denies read** of them — so
  agentic models run at `read-only`/`workspace-write`, **never** `danger-full-access` /
  `--dangerously-skip-permissions` unless an external OS sandbox already isolates the cred dir;
  and (2) secrets are kept out of the tool subprocess environment where the engine allows it —
  codex via `shell_environment_policy`, others via the configured `sandbox_backend`. Because
  **claude/agy expose no per-tool env scrubbing**, claude/agy agentic rely on **file-based auth +
  FS denial** of the cred dir rather than env (hence the API-key env restriction above); their
  env cannot be assumed scrubbed without an external sandbox.
- **Workspace confinement:** canonicalize the configured workspace path, reject symlinks that
  escape it, and pass it as the engine's only writable root (`--cd`/`--add-dir`/confined dir).
- **Per-engine sandbox is documented, not assumed uniform — and "full-auto" ≠ "full access":**
  auto-approve (no permission prompts) is separated from filesystem scope.
  - **codex** has a real FS boundary: `workspace-write` auto-approves edits *inside* the
    workspace while denying reads/writes outside it (including the cred dir). This is the
    intended full-auto posture; `danger-full-access` is never used for request-driven runs.
  - **claude / agy** auto-approve via `--dangerously-skip-permissions` / `--sandbox`, which do
    **not** OS-confine the shell commands the agent spawns — a `bash` tool could read the cred
    dir. So for these engines, real confinement requires an **external OS sandbox**
    (bubblewrap / namespaces / container) that denies the cred dir and non-workspace paths.
    Without it, claude/agy agentic models are only safe for trusted callers; this is documented
    as the weaker posture.
  The config's `permissions` maps to the strongest available control per engine; the docs state
  each engine's actual guarantee (the sandbox strength is the *engine's* — or the host OS's —
  not ours).
- **Startup validation ties config to posture.** The server refuses to start (clear message) on
  unsafe combinations:
  - a claude/agy `mode: agentic` model with no `sandbox_backend` unless explicitly marked
    `trusted_caller_only: true` — prevents the config from *implying* confinement that isn't
    there; and
  - a claude/agy `mode: agentic` model authed by **API-key env var** with no `sandbox_backend`
    (the key would leak into spawned shell tools — see env-auth restriction above).
  When a `sandbox_backend` (bubblewrap/namespace/container) is configured, the engine runs inside
  it and neither restriction applies. Codex `workspace-write` is self-confining and `text`
  profiles run no tools, so both are exempt.
- **`mode: text`** replicates the PoC: empty temp dir, all tools blocked — a safe pure-generator.

## 5. Data flow

**Non-streaming chat completion** — resolve model → entry; compute session key; resolve session
(**hit:** resume + last turn; **miss:** fresh + full projected conversation); spawn; consume
`AgentEvent`s to completion; assemble (final text → `content`, progress per profile); return one
`chat.completion`.

**Streaming chat completion** — same setup, respond `text/event-stream`; each `AgentEvent` → a
`chat.completion.chunk` (`delta.content` for final text; progress → `delta.reasoning_content`
under the `reasoning_content` profile, suppressed under `omit`; `delta.tool_calls` when
bridging); `[DONE]` at `Done`; client disconnect cancels the child. (`agy`: a single final-text
chunk, no progress.)

**Tool-call round trip (MCP bridge)** — request with `tools` → MCP server up, CLI wired in;
agent calls one or more tools → emit the turn's `tool_call`s (each with a `tool_call_id`), finish
`tool_calls`, **suspend** the process as a group keyed by *all* of that turn's ids; client runs
the tools and sends **one** follow-up carrying every `role:"tool"` result; orchestrator matches
the full id set to the suspended group, delivers each result into its parked MCP call, the agent
continues, and the continuation is the response to that follow-up. (Partial set → `409`; unknown/
expired ids → `400`; see §4.6.)

## 6. Configuration

```yaml
server:
  bind: "127.0.0.1:8088"        # non-loopback bind requires a real bearer_token
  bearer_token: "sk-..."        # required if bind is non-loopback
  progress_channel: reasoning_content  # reasoning_content (OpenWebUI) | omit (strict OpenAI)
defaults:
  timeout_s: 600                       # per-turn active work timeout (-> 504)
  max_concurrency: 4                   # active (running) turns; suspended turns don't count here
  tool_result_timeout_s: 120           # how long a suspended tool-call group waits for results
  max_suspended_sessions: 16           # cap on parked tool-call processes (excess -> 503)
  session_store: { backend: sled, path: ~/.llm-bridge/sessions, ttl_h: 24 }
  env_passthrough: ["PATH", "LANG"]    # everything else scrubbed from spawned CLIs
  sandbox_backend: none                # none | bubblewrap | container — required for claude/agy
                                       # agentic unless that model sets trusted_caller_only: true
credentials:                    # dedicated, persistent, operator-provisioned per-engine dirs
  claude_config_dir: ~/.llm-bridge/cred/claude   # CLAUDE_CONFIG_DIR
  codex_home: ~/.llm-bridge/cred/codex           # CODEX_HOME (persistent — enables resume)
  agy_config_dir: ~/.llm-bridge/cred/gemini      # UNPROVEN: agy has no config-dir flag and uses
                                                 # the OS keyring; isolation gated on §9 spike
models:
  - id: "claude-opus-repoA"
    engine: claude               # claude | codex | agy
    model: opus                  # passed to the engine's --model
    workspace: /work/repoA       # canonicalized; symlink-escapes rejected
    mode: agentic                # agentic | text
    permissions: workspace-write # mapped to the strongest equivalent per engine
    trusted_caller_only: true    # REQUIRED for claude/agy agentic when sandbox_backend: none
                                 # (their auto-approve doesn't OS-confine spawned shell tools)
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
- OpenAI-shaped error bodies; CLI nonzero exit → `500` with stderr; active timeout → `504`;
  unknown model → `404`; bad request / unknown-or-expired `tool_call_id` / duplicate result →
  `400`; partial tool-result set → `409` (with missing ids); suspended-session pool full →
  `503`; missing/invalid token → `401`.
- `max_concurrency` cap (active turns) with a bounded queue; separate `max_suspended_sessions`
  pool for parked tool-call groups; reject or queue past the limits.
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
  trip (single- and **multi-`tool_call`** turns), asserting: complete follow-up resumes; partial
  set → `409` (missing ids) with the suspension still live; duplicate result → `400`; unknown/
  expired id → `400`; orphan timeout reaps the child; and `max_suspended_sessions` overflow →
  `503`. Plus a **concurrency test** asserting two simultaneous tool-bearing requests get isolated
  per-process MCP wiring (distinct temp configs / sockets, no cross-talk).
- **Session key** tested for fingerprint sensitivity (same id + changed workspace ⇒ different
  key; same conversation + different `tools`/`tool_choice` ⇒ different key) and prefix-rotation
  correctness.
- **Replay transcript renderer** tested over tool-role histories: asserts past user
  instructions are framed as read-only context and only the final user turn is the live
  instruction.
- **End-to-end smoke** behind a feature flag that shells out to a real installed CLI (local,
  not CI).

## 9. Build sequence (ordered, all in v1)
1. Scaffold, config loader, model registry, `/health` + `/v1/models`, localhost bind + auth,
   **startup validation** (claude/agy agentic ⇒ `sandbox_backend` set or `trusted_caller_only`).
2. `Engine` enum + `FakeEngine`; Turn Orchestrator; non-streaming `/v1/chat/completions`.
3. Process Supervisor (spawn, isolation/env scrub, timeout, concurrency, cancellation).
4. `ClaudeAdapter` against recorded fixtures; real non-streaming end to end.
5. SSE streaming + event→chunk mapping (final → `content`; progress **per configured client
   profile** — `reasoning_content` or omitted).
6. Session Store + content-hash resume: tool-aware projection + ModelEntry fingerprint +
   **tool-config fingerprint**; **hit = resume + last turn, miss = fresh + read-only transcript
   replay** (defined renderer, tested over tool-role histories).
7. `CodexAdapter` (JSONL + `--output-last-message`, sandbox levels, persistent `CODEX_HOME`;
   `--ephemeral` only on `text`). `AgyAdapter` ships **stateless-replay first**; an **agy spike**
   gates `--conversation` resume **and** credential/config isolation — if either can't be
   proven, agy stays replay-only + single-tenant in v1 (logged as a known limitation).
8. MCP Tool Bridge: per-process injection (claude `--mcp-config --strict-mcp-config`; codex
   `-c mcp_servers.*` — validate in spike, gate if it fails; agy off) + **turn-level suspended
   groups** (all-or-`409`, unknown/expired→`400`, `tool_result_timeout_s`, `max_suspended_sessions`
   accounting).
9. Credential-provisioning + security/sandbox hardening (cred dirs outside workspace, FS-denied to
   tools; codex env-scrub via `shell_environment_policy`; **API-key env disallowed for claude/agy
   agentic** without a sandbox; external OS sandbox for claude/agy full-auto), startup-validation
   rules, per-engine sandbox docs, client-profile wiring, observability polish, e2e smoke.

## 10. Open questions / risks
- **Per-process MCP injection must be race-free:** claude is confirmed (`--mcp-config
  --strict-mcp-config`); **codex `-c mcp_servers.*` per-invocation injection is unvalidated** and
  the §9 spike must confirm it before codex `tools` ships (else gate codex MCP); agy has no safe
  per-invocation MCP config and is gated off.
- **Single-user/trusted is a hard assumption (§3.3):** session keys are content-derived, so a
  multi-tenant deployment could cross-resume users. v1 does not support multi-tenant use; a
  trusted-proxy identity header namespacing the key is the documented future extension point.
- **Suspended-session vs client retry / multi-call:** suspensions are turn-level groups that
  resume only on a single follow-up carrying the **complete** result set (partial → `409`,
  unknown/expired → `400`). Parked processes don't hold an active concurrency slot but are capped
  by `max_suspended_sessions` and reaped at `tool_result_timeout_s`; an abandoning client just
  triggers the reap. This bounds the worst case (a client opening many tool turns and never
  completing them) to `max_suspended_sessions` idle processes.
- **agy spike covers three things:** session-id capture, credential/config isolation (keyring +
  `~/.gemini` may not redirect to a service dir), and confirms MCP-off. If isolation can't be
  proven, agy is operator-login-only / single-tenant in v1.
- **Credential exfiltration is the top security risk:** cred dirs must be outside the workspace,
  FS-denied to model-executed tools, and scrubbed from tool-subprocess env. codex
  `workspace-write` enforces this natively; claude/agy full-auto need an **external OS sandbox**
  — without one they're trusted-caller-only.
- **Replay-prompt safety:** the read-only transcript renderer must reliably stop old
  instructions from being re-executed; covered by tests over tool-role histories (§8).
- **Credential provisioning is an operator step:** the dedicated per-engine config dirs must be
  logged into / populated once before agentic models work; document per engine.
