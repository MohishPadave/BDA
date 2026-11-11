#!/usr/bin/env bash
cd "$(dirname "$0")"
for f in logs/*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null && rm "$f"; done
docker compose down
echo "Stopped."
