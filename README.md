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

## Status (Phase 2)
- ✅ `GET /health`, `GET /v1/models`, `POST /v1/chat/completions` — **streaming (SSE) and non-streaming**
- ✅ claude engine via `--output-format stream-json`; text + agentic (`trusted_caller_only`) modes
- ✅ **Session resume** (content-hash key → `--resume`); in-memory index (restart degrades to full-transcript replay)
- ✅ progress profiles: `reasoning_content` (OpenWebUI) / `omit` (strict clients); global `max_concurrency`
- ⛔ `tools` → 400 (Phase 4); non-claude engines / `sandbox_backend` → refused at startup (Phase 3)

## Test
```bash
cargo test                                                        # hermetic unit + router tests
cargo test --features e2e_smoke --test e2e_smoke -- --nocapture   # real claude (opt-in)
```
