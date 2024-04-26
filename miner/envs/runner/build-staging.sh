#!/bin/bash
set -eux -o pipefail

( cd nginx && ./build-image.sh )

IMAGE_NAME="backenddevelopersltd/compute-horde-miner-runner-staging:v0-latest"
MINER_IMAGE_REPO="compute-horde-miner-staging"
source _build-image.sh
