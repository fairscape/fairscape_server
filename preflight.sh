#!/usr/bin/env bash
set -u

PORTS=(8080 6379 27017 8081 9010 9011)

echo "Current holders:"
sudo ss -ltnp | grep -E ":($(IFS=\|; echo "${PORTS[*]}"))\b" || echo "  (none)"

for p in "${PORTS[@]}"; do
  sudo fuser -k "${p}/tcp" 2>/dev/null || true
done
