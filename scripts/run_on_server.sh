#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-wuteng@172.20.8.28}"
REMOTE_DIR="${REMOTE_DIR:-/data/realtime_rag_pipeline}"
DEFAULT_CMD="python3 main.py serve --config configs/pipeline_legacy_enhanced.yaml --host 0.0.0.0 --port 8000"

REMOTE_CMD="${*:-$DEFAULT_CMD}"

ssh "${REMOTE_HOST}" "cd '${REMOTE_DIR}' && ${REMOTE_CMD}"
