#!/usr/bin/env bash
# Optional: start Storage Health at macOS login (LaunchAgent).
# Run once: bash scripts/install-macos-login-agent.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/.venv/bin/storage-health"
if [[ ! -x "$BIN" ]]; then
  echo "Missing $BIN — run: cd \"$ROOT\" && python3 -m venv .venv && .venv/bin/pip install -e ." >&2
  exit 1
fi
PLIST="$HOME/Library/LaunchAgents/com.storage-health.serve.plist"
PORT="${STORAGE_HEALTH_PORT:-5003}"
mkdir -p "$HOME/Library/LaunchAgents"
cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.storage-health.serve</string>
  <key>ProgramArguments</key>
  <array>
    <string>$BIN</string>
    <string>serve</string>
    <string>--host</string>
    <string>127.0.0.1</string>
    <string>--port</string>
    <string>$PORT</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$ROOT</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <false/>
</dict>
</plist>
EOF
launchctl bootout "gui/$(id -u)" "$PLIST" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
echo "Installed LaunchAgent: $PLIST"
echo "Server will run at login on http://127.0.0.1:$PORT/"
echo "Unload: launchctl bootout gui/\$(id -u) \"$PLIST\""
