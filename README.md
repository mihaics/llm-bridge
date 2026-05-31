# llm-bridge

OpenAI Chat Completions–compatible HTTP shim over the `claude`, `codex`, and `agy` agentic CLIs.
Successor to the Python PoC; see `docs/superpowers/specs/2026-05-31-llm-bridge-design.md`.

## Run

```bash
cp config.example.yaml config.yaml   # edit models / bearer_token
# Provision a DEDICATED claude login (keeps the service off your personal config):
CLAUDE_CONFIG_DIR=~/.llm-bridge/cred/claude claude login
cargo run --release -- config.yaml
```

Point OpenWebUI/LiteLLM at `http://127.0.0.1:8088/v1` (api key = your `bearer_token`).

## Security posture (single-user/trusted by default)
- Each spawned engine CLI runs with a **scrubbed environment** (only `env_passthrough` vars survive),
  so secrets like `ANTHROPIC_API_KEY` are NOT visible to model-run tools. Auth is **file-based**
  via the per-engine cred dir (`CLAUDE_CONFIG_DIR` / `CODEX_HOME`).
- With `sandbox_backend: none`, agentic models require `trusted_caller_only: true` (no native engine
  sandbox passes the read-denial probe). Set `sandbox_backend: bubblewrap` to confine writes to the
  workspace and deny reads outside the cred dir — validated at startup by the canary probes — which
  drops the `trusted_caller_only` requirement. Bind localhost; front with a trusted proxy if remote.
  Non-loopback binds require a strong, non-default token.

## Status (Phase 4a)
- ✅ `GET /health`, `GET /v1/models`, `POST /v1/chat/completions` — streaming (SSE) and non-streaming
- ✅ Three engines: `claude` (stream-json), `codex` (`exec --json`), `agy` (`--print`); session resume for claude+codex; bubblewrap sandbox + startup canary probes
- ✅ **OpenAI `tools` protocol core**: `tool_calls` out (aggregate + SSE `delta.tool_calls`), tool-result follow-up **routing by message shape**, and the **suspended tool-call registry** (turn-level groups: all-or-`409`, duplicate/unknown→`400`, idle-reap, `max_suspended_sessions`→`503`)
- ⛔ The **live MCP bridge** (in-process `rmcp` server + claude `--mcp-config` wiring) lands in **Phase 4b** — until then `tools` to claude → `400 "Phase 4b"`; `tools` to codex/agy → `400 "not supported for engine …"` (codex gated pending the §9 spike; agy never)
- ⛔ `sandbox_backend: container` → refused at startup (not yet implemented)

## Test
```bash
cargo test                                                        # hermetic unit + router tests
cargo test --features e2e_smoke --test e2e_smoke -- --nocapture   # real claude (opt-in)
```
