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
