#!/bin/bash
set -eux -o pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
( cd "${SCRIPT_DIR}/nginx" && ./build-image.sh )

IMAGE_NAME="andreeareef/compute-horde-validator-runner:v0-latest"
VALIDATOR_IMAGE_REPO="compute-horde-validator"

source "${SCRIPT_DIR}/_build-image.sh"
