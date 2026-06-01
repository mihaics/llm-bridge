# llm-bridge

OpenAI Chat Completions–compatible HTTP shim over the `claude`, `codex`, and `agy` agentic CLIs.
It lets any OpenAI-compatible client talk to the coding agents you already have installed and
**logged in** — driving them under your existing subscription rather than a metered API key — and
exposes both plain chat completions and full **agentic** (tool-using, repo-editing) turns behind a
single `/v1/chat/completions` endpoint.

> Successor to the Python PoC; design notes in `docs/superpowers/specs/2026-05-31-llm-bridge-design.md`.

## Why use it (local dev)

- **Reuse your agent subscription from any OpenAI client.** `claude` (Claude Code), `codex`
  (Codex CLI), and `agy` (Gemini) authenticate with their own *file/keyring login* — i.e. your
  flat-rate Claude / ChatGPT‑Codex / Gemini plan — not per-token API credits. The bridge speaks the
  OpenAI wire protocol, so OpenWebUI, LiteLLM, Aider, Continue, the `openai` SDKs, `curl`, or your
  own scripts can use those agents **without ever holding a provider key**.
- **One endpoint, several agents.** Register each CLI/model (Opus, GPT‑5, Gemini, …) as a separate
  OpenAI "model"; switch or A/B-compare them by changing the `model` field — same client, same auth.
- **Agentic coding from a chat UI (or a script).** An `agentic` model is pinned to a repo workspace
  and runs the CLI's real tool suite (read / edit / run). You get terminal-grade coding agents driven
  from a browser, a shared team UI, a phone, or an automated job — with streamed reasoning.
- **Secrets stay on the host.** Clients present only the bridge's bearer token; they never see your
  provider credentials. Each spawned CLI runs with a **scrubbed environment**, so model-run tool
  shells can't read your secrets. Everything runs locally.
- **Cheap, stateful multi-turn.** A content-hash session index resumes each engine's *native*
  session (claude / codex) on follow-ups instead of replaying the whole transcript — the agent keeps
  its working context across turns.
- **Tool-calling that bridges to your tools.** Client-supplied OpenAI `tools` are exposed to claude
  through an in-process MCP server: the agent calls them, you return results, and the held-open turn
  resumes — standard `tool_calls` in, results back, no bespoke per-CLI glue.

## Use cases

- **Browser-based Claude Code** — point OpenWebUI at an `agentic` claude model on your repo and
  drive edits / refactors / test-runs from a chat window, watching the agent's reasoning stream.
- **Compare agents on one task** — send the same prompt to `claude-opus`, `codex-gpt5`, and
  `gemini` from one UI and judge the results side by side.
- **Use your subscription in existing tools** — set the OpenAI `base_url` of Aider / Continue / a
  notebook / the `openai` SDK to the bridge and code against your plan, not an API bill.
- **Automation & internal bots** — anything that can POST chat completions can now invoke a coding
  agent (codegen in CI, a triage bot, a docs generator) over one uniform interface.
- **Tool-augmented agents** — register your own functions as OpenAI `tools`; the bridge relays them
  to the agent (claude) via MCP so it can call into your systems mid-turn.
- **Shared dev box** — run it on a dev server behind a proxy + token; the concurrency cap and the
  suspended-session registry keep concurrent users stable.

## Modes: `text` vs `agentic`

Each model entry picks one mode:

| Mode | Behaviour | Use it for |
|------|-----------|------------|
| `text` | Pure generator — **all tools blocked**, no filesystem access; behaves like an ordinary chat model. | General chat, drafting, Q&A; the safe default. |
| `agentic` | The CLI runs with its **full tool suite** (read / edit / run) confined to a `workspace`. | Real coding work: edits, refactors, running tests, multi-step tasks in a repo. |

### How agentic mode works

```yaml
models:
  - id: "claude-opus-repo"
    engine: claude
    model: opus
    mode: agentic
    workspace: /path/to/your/repo   # the agent's working dir + only writable root
    permissions: workspace-write
    trusted_caller_only: true       # required when sandbox_backend: none (see below)
```

- The agent launches with its working dir / `--add-dir` set to `workspace`, so its tools operate
  inside that repo. Its reasoning and tool activity stream back over the `reasoning_content`
  progress channel (or are omitted — your choice).
- **Confinement.** With `sandbox_backend: none` the engine isn't externally sandboxed, so an
  agentic model must set `trusted_caller_only: true` (you accept it for trusted/local callers). Set
  `sandbox_backend: bubblewrap` to confine writes to the workspace and deny reads outside the cred
  dir — verified at startup by write/read **canary probes** — which lifts that requirement.
- **Follow-ups resume the session**, so the agent keeps context across turns rather than re-reading
  everything each request.
- Call it like any OpenAI model — just put the `agentic` model's `id` in the `model` field.

## Run

```bash
cp config.example.yaml config.yaml   # edit models / bearer_token
# Provision a DEDICATED claude login (keeps the service off your personal config):
CLAUDE_CONFIG_DIR=~/.llm-bridge/cred/claude claude login
cargo run --release -- config.yaml
```

Point OpenWebUI/LiteLLM at `http://127.0.0.1:8090/v1` (api key = your `bearer_token`).

For a ready-made local **OpenWebUI + shim** stack (Docker compose + host shim + `/tmp` test
workspaces), see [`deploy/`](deploy/README.md).

## Security posture (single-user/trusted by default)
- Each spawned engine CLI runs with a **scrubbed environment** (only `env_passthrough` vars survive),
  so secrets like `ANTHROPIC_API_KEY` are NOT visible to model-run tools. Auth is **file-based**
  via the per-engine cred dir (`CLAUDE_CONFIG_DIR` / `CODEX_HOME`).
- With `sandbox_backend: none`, agentic models require `trusted_caller_only: true` (no native engine
  sandbox passes the read-denial probe). Set `sandbox_backend: bubblewrap` to confine writes to the
  workspace and deny reads outside the cred dir — validated at startup by the canary probes — which
  drops the `trusted_caller_only` requirement. Bind localhost; front with a trusted proxy if remote.
  Non-loopback binds require a strong, non-default token.

## Status (Phase 4b)
- ✅ `GET /health`, `GET /v1/models`, `POST /v1/chat/completions` — streaming (SSE) and non-streaming
- ✅ Three engines; session resume (claude+codex); bubblewrap sandbox + canary probes
- ✅ **OpenAI `tools` end-to-end for claude**: an in-process `rmcp` MCP server exposes the request's tools, claude is wired per-invocation via `--mcp-config <temp> --strict-mcp-config`, tool calls become OpenAI `tool_calls` (`finish_reason:"tool_calls"`), and the client's tool-result follow-up resumes the held-open turn (suspended-session registry: all-or-`409`, duplicate/unknown→`400`, idle-reap, `max_suspended_sessions`→`503`, active-permit released while parked)
  - The MCP server runs in **stateless `json_response` mode** (`StreamableHttpServerConfig::with_stateful_mode(false).with_json_response(true)`): claude's HTTP MCP client aborts the default stateful server→client SSE stream immediately, which drops the tool list before the model sees it. Stateless request/response avoids the standalone SSE entirely; a long-running tool call still parks (the `tools/call` POST is held open until the result is delivered).
- ⛔ `tools` to codex/agy → `400 "not supported for engine …"` (codex gated pending the §9 injection spike; agy never)
- ⛔ `tools` + an external `sandbox_backend` → refused (the tools turn spawns claude unsandboxed; wrapping while keeping the temp `--mcp-config` reachable is future work); `sandbox_backend: container` → refused at startup

## Test
```bash
cargo test                                                        # hermetic unit + router tests
cargo test --features e2e_smoke --test e2e_smoke -- --nocapture   # real claude (opt-in)
```

## License

MIT — see [LICENSE](LICENSE).
