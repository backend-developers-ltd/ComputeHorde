#!/bin/bash
set -eu

# Check if the file does not exist
if [[ ! -f "$WORKER_HEALTHCHECK_FILE_PATH" ]]; then
  # Create the parent directory, if necessary
  mkdir -p "$(dirname "$WORKER_HEALTHCHECK_FILE_PATH")"
  # Create the file
  touch "$WORKER_HEALTHCHECK_FILE_PATH"
fi

# Default timeout - 10 seconds
WORKER_HEALTHCHECK_TIMEOUT=${WORKER_HEALTHCHECK_TIMEOUT:-10}

# Check the file's modification time
MOD_TIME=$(stat -c %Y "$WORKER_HEALTHCHECK_FILE_PATH")
CURRENT_TIME=$(date +%s)

if (( CURRENT_TIME - MOD_TIME > WORKER_HEALTHCHECK_TIMEOUT )); then
  # Liveness check failed: file not modified within $WORKER_HEALTHCHECK_TIMEOUT seconds
  exit 1
fi
