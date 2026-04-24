#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

REMOTE_HOST="${REMOTE_HOST:-wuteng@172.20.8.28}"
REMOTE_DIR="${REMOTE_DIR:-/data/realtime_rag_pipeline}"
DELETE_FLAG=""

if [[ "${1:-}" == "--delete" ]]; then
  DELETE_FLAG="--delete"
fi

rsync -av ${DELETE_FLAG} \
  --exclude ".git/" \
  --exclude "__pycache__/" \
  --exclude ".pytest_cache/" \
  --exclude ".venv/" \
  --exclude ".DS_Store" \
  --exclude "*.pyc" \
  --exclude "logs/*.db" \
  --exclude "logs/*.log" \
  --exclude "data/*.db" \
  --exclude "data/pending_reviews.db" \
  --exclude "data/pipeline_states.db" \
  --exclude "data/uploads/" \
  --exclude "data/pdf/" \
  --exclude "data/deferred_uploads/" \
  --exclude "data_process/result/" \
  "${PROJECT_ROOT}/" "${REMOTE_HOST}:${REMOTE_DIR}/"
