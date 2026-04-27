#!/usr/bin/env bash
# Storage Health — one command from repo root:
#   ./start.sh
# Creates .venv on first run, kills anything listening on the port, then serves.
# Optional: STORAGE_HEALTH_PORT=5003 STORAGE_HEALTH_HOST=127.0.0.1 ./start.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

PORT="${STORAGE_HEALTH_PORT:-5003}"

free_listen_port() {
  local port="${1:?}"
  [[ "$port" =~ ^[0-9]+$ ]] || return 0
  if command -v lsof >/dev/null 2>&1; then
    local pids
    pids="$(
      lsof -ti "tcp:${port}" -sTCP:LISTEN 2>/dev/null ||
        lsof -ti "tcp:${port}" 2>/dev/null ||
        lsof -t -i ":${port}" 2>/dev/null ||
        true
    )"
    if [[ -n "${pids// /}" ]]; then
      echo "Stopping process(es) on port ${port} (PIDs: ${pids})…" >&2
      # shellcheck disable=SC2086
      kill -9 ${pids} 2>/dev/null || true
      sleep 0.25
      return 0
    fi
  fi
  if command -v fuser >/dev/null 2>&1; then
    if fuser "${port}/tcp" >/dev/null 2>&1; then
      echo "Stopping process(es) on port ${port} (fuser -k)…" >&2
      fuser -k "${port}/tcp" 2>/dev/null || true
      sleep 0.25
    fi
  fi
}

if [[ ! -x .venv/bin/python3 ]]; then
  if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 not found. Install Python 3, then run ./start.sh again." >&2
    exit 1
  fi
  echo "First run: creating .venv and installing…" >&2
  python3 -m venv .venv
  .venv/bin/pip install -U pip >/dev/null
  .venv/bin/pip install -e .
fi

free_listen_port "$PORT"

exec "$ROOT/scripts/serve.sh" "$@"
