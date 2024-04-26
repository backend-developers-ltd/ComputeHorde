#!/bin/bash
set -eux -o pipefail

( cd nginx && ./build-image.sh )

IMAGE_NAME="backenddevelopersltd/compute-horde-validator-runner-staging:v0-latest"
VALIDATOR_IMAGE_REPO="compute-horde-validator-staging"
source _build-image.sh
