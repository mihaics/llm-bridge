# Local OpenWebUI + llm-bridge test stack

OpenWebUI (in Docker) talking to the **llm-bridge shim running on the host**, which fronts your
locally **logged-in** `claude` / `codex` / `agy`.

## Why the shim is NOT in Docker

The shim shells out to the agent CLIs, and on this box they are host-bound:

- **claude** — a versioned Node install + plugins, credentials in its `CLAUDE_CONFIG_DIR` (e.g. `~/.claude`).
- **codex** — launched from an *ephemeral* fnm node-multishell path on your `PATH`.
- **agy** — reads the **OS keyring** (Secret Service / D-Bus) at runtime for its login.

Containerising those cleanly (and keeping your logins valid) isn't practical, so the shim runs on
the host where they already work, and OpenWebUI reaches it via `host.docker.internal`.

## Files

| File | Purpose |
|------|---------|
| `config.example.yaml` | Template shim config (committed). Copy to `config.yaml`. |
| `config.yaml` | **Local, gitignored** — your real cred dirs, `/tmp` workspaces, 4 models, your token. |
| `.env.example` | Template for compose vars (committed). Copy to `.env`. |
| `.env` | **Local, gitignored** — `LLM_BRIDGE_TOKEN` (must match `config.yaml`) + `SHIM_PORT`/`OWUI_PORT`. |
| `docker-compose.yml` | OpenWebUI, pre-pointed at the host shim (reads `.env`). |
| `run-shim.sh` | Creates the `/tmp/llm-bridge-work/{repoA,repoB,repoC}` git workspaces, builds, runs the shim. |

## Run it

```bash
# 0. One-time: create your local config + env from the templates, then edit the token in both.
cp deploy/config.example.yaml deploy/config.yaml
cp deploy/.env.example        deploy/.env
$EDITOR deploy/config.yaml deploy/.env   # set the same strong bearer token (>=16 chars) in both

# 1. Terminal A — host shim (foreground; Ctrl-C to stop). Also makes the /tmp workspaces.
./deploy/run-shim.sh

# 2. Terminal B — OpenWebUI
docker compose -f deploy/docker-compose.yml up -d
```

Open **http://localhost:3000**. The model picker lists the shim's models
(`claude-sonnet-text`, `claude-opus-agentic`, `codex-agentic`, `gemini-agentic`).
Start with **`claude-sonnet-text`** — it's a pure generator (no filesystem side effects) and the
quickest way to confirm the whole path works. The agentic models write into the throwaway
`/tmp/llm-bridge-work` repos.

Stop OpenWebUI: `docker compose -f deploy/docker-compose.yml down` (add `-v` to wipe its data volume).

## Verifying without OpenWebUI

```bash
source deploy/.env   # exports LLM_BRIDGE_TOKEN; or: TOKEN=<your token>
TOKEN="$LLM_BRIDGE_TOKEN"
curl -s localhost:8090/health
curl -s localhost:8090/v1/models -H "Authorization: Bearer $TOKEN" | jq .
curl -s localhost:8090/v1/chat/completions -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"model":"claude-sonnet-text","messages":[{"role":"user","content":"reply with: pong"}]}' | jq -r '.choices[0].message.content'
```

## Notes & knobs

- **Token:** `server.bearer_token` in `deploy/config.yaml` and `LLM_BRIDGE_TOKEN` in `deploy/.env`
  must match (the shim authenticates with the former; OpenWebUI sends the latter). A non-loopback
  bind rejects placeholders / tokens shorter than 16 chars. Both files are gitignored.
- **Host firewall (Linux/ufw):** the OpenWebUI container reaches the host shim across a Docker
  bridge, and `host.docker.internal` resolves to `172.17.0.1` while the container's *source* IP is on
  its own compose-network subnet (e.g. `172.25.0.0/16`). With ufw's default-deny `INPUT`, those
  packets are dropped (connect *times out*). Open the Docker pool (and other private ranges) to the
  host once:
  ```bash
  sudo ufw allow from 172.16.0.0/12 to any comment 'docker bridges -> host'
  # (this stack also opened 10.0.0.0/8 and 192.168.0.0/16)
  ```
- **Bind exposure:** `0.0.0.0:8090` is reachable on your LAN. The strong bearer token gates it; if
  you want to lock it down further, bind to the Docker bridge gateway IP instead of `0.0.0.0` and
  drop `host.docker.internal` for that IP.
- **Agent health:** `run-shim.sh` reports which of `claude`/`codex`/`agy` it found on `PATH`. A
  missing or logged-out agent only fails *its* model — the others keep working.
- **codex/agy quirks:** `claude` is the most reliable here. `codex` needs `CODEX_HOME=~/.codex`
  logged in; `agy` needs the OS keyring reachable (hence `XDG_RUNTIME_DIR` + `DBUS_SESSION_BUS_ADDRESS`
  in `env_passthrough`). If an agentic model errors, check that agent's own `… login` first.
