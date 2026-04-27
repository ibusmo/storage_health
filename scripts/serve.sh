#!/usr/bin/env bash
# Start Storage Health web UI (from project root).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PORT="${STORAGE_HEALTH_PORT:-5003}"
HOST="${STORAGE_HEALTH_HOST:-127.0.0.1}"
# Prefer `python -m` so a moved/copied project still starts (console_scripts shebangs go stale).
if [[ -x .venv/bin/python3 ]]; then
  exec .venv/bin/python3 -m sd_health serve --host "$HOST" --port "$PORT" "$@"
fi
if command -v storage-health >/dev/null 2>&1; then
  exec storage-health serve --host "$HOST" --port "$PORT" "$@"
fi
echo "No .venv/bin/python3 and no storage-health on PATH. From project root run:" >&2
echo "  python3 -m venv .venv && .venv/bin/pip install -e ." >&2
exit 1
