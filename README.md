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

## Status (Phase 3)
- ✅ `GET /health`, `GET /v1/models`, `POST /v1/chat/completions` — streaming (SSE) and non-streaming
- ✅ **Three engines**: `claude` (stream-json), `codex` (`exec --json`, sandbox levels, persistent `CODEX_HOME`), `agy` (`--print`, stateless replay)
- ✅ **Session resume** for claude + codex (content-hash key → engine resume); **agy is stateless** (resume gated off pending the agy spike — single-tenant, operator login)
- ✅ **`sandbox_backend: bubblewrap`** — workspace-only writes + cred-dir-only reads, validated at startup by **write- + read-denial canary probes**; agentic models then run without `trusted_caller_only`
- ✅ API-key-env restriction (claude/agy agentic without a sandbox refuse a passed API key); progress profiles; global `max_concurrency`
- ⛔ `tools` → 400 (Phase 4); `sandbox_backend: container` → refused at startup (not yet implemented)

## Test
```bash
cargo test                                                        # hermetic unit + router tests
cargo test --features e2e_smoke --test e2e_smoke -- --nocapture   # real claude (opt-in)
```
