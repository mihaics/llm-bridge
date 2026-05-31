#!/usr/bin/env bash
## Start the llm-bridge shim on the host (using your logged-in claude/codex/agy) and prepare
## throwaway git workspaces under /tmp for the agentic models. Run this in its own terminal:
##
##     ./deploy/run-shim.sh
##
## then, separately:   docker compose -f deploy/docker-compose.yml up -d
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
WORK="/tmp/llm-bridge-work"   # MUST match the workspace: paths in deploy/config.yaml

if [ ! -f "$HERE/config.yaml" ]; then
  cp "$HERE/config.example.yaml" "$HERE/config.yaml"
  echo "!! Created deploy/config.yaml from the example — EDIT server.bearer_token (and your"
  echo "!! agent config dirs) before exposing it, then re-run. deploy/.env must use the same token."
  exit 1
fi

echo "==> Preparing test working dirs under $WORK"
for d in repoA repoB repoC; do
  dir="$WORK/$d"
  if [ -d "$dir/.git" ]; then
    echo "   exists  $dir"
  else
    mkdir -p "$dir"
    git -C "$dir" init -q
    printf '# %s\n\nThrowaway workspace for llm-bridge agentic-model tests.\n' "$d" > "$dir/README.md"
    git -C "$dir" add -A
    git -C "$dir" -c user.email=llm-bridge@localhost -c user.name=llm-bridge commit -qm "seed $d"
    echo "   created $dir (git-initialised)"
  fi
done

echo "==> Sanity-checking logged-in agents on PATH"
for a in claude codex agy; do
  if command -v "$a" >/dev/null 2>&1; then echo "   ok      $a -> $(command -v "$a")"; else echo "   MISSING $a (its model will fail until installed + logged in)"; fi
done

echo "==> Building llm-bridge (release)"
cargo build --release --manifest-path "$REPO/Cargo.toml"

PORT="$(sed -nE 's/.*bind:[[:space:]]*"[^:]*:([0-9]+)".*/\1/p' "$HERE/config.yaml" | head -1)"
echo "==> Starting shim on 0.0.0.0:${PORT:-8088}  (config: $HERE/config.yaml)"
echo "    OpenWebUI (in Docker) will reach it at http://host.docker.internal:${PORT:-8088}/v1"
exec "$REPO/target/release/llm-bridge" "$HERE/config.yaml"
