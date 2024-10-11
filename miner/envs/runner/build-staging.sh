#!/bin/bash
set -euxo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
( cd "${SCRIPT_DIR}/nginx" && ./build-image.sh )

IMAGE_NAME="backenddevelopersltd/compute-horde-miner-runner-staging:v0-latest"
MINER_IMAGE_REPO="compute-horde-miner-staging"

source "${SCRIPT_DIR}/_build-image.sh"
