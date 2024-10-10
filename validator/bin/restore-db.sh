#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"

TARGET_FILEPATH="$1"

DOCKER_NETWORK=$(get_db_docker_network)

# shellcheck disable=SC2002
cat "$TARGET_FILEPATH" | docker run -i --rm --network "$DOCKER_NETWORK" postgres:16-alpine pg_restore -c -d "$DATABASE_URL"

echo 'restore finished'
