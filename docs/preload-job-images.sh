#!/usr/bin/env bash
# preload-job-images.sh
# Pull all Docker images defined in miner-config-prod.json.
# Requires curl, jq and docker to be installed.

set -euo pipefail

RAW_URL="https://raw.githubusercontent.com/backend-developers-ltd/compute-horde-dynamic-config/master/miner-config-prod.json"

curl -fsSL "$RAW_URL" \
| jq -r '.DYNAMIC_PRELOAD_DOCKER_JOB_IMAGES.items[0].value[]' \
| xargs -r -n1 docker pull
