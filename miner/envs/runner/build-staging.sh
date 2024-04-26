#!/bin/bash
set -eux -o pipefail

( cd nginx && ./build-image.sh )

IMAGE_NAME="backenddevelopersltd/compute-horde-miner-runner-staging:v0-latest"
MINER_IMAGE_REPO="compute-horde-miner-staging"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/_build-image.sh"
