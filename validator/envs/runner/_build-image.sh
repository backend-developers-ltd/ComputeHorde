#!/bin/bash
set -eux -o pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SCRIPT_DIR}"

VALIDATOR_RUNNER_VERSION=$(git rev-parse HEAD)
docker build \
	--platform=linux/amd64 \
	-t "$IMAGE_NAME" \
	--build-arg VALIDATOR_RUNNER_VERSION="${VALIDATOR_RUNNER_VERSION}" \
	--build-arg VALIDATOR_IMAGE_REPO="${VALIDATOR_IMAGE_REPO}" \
	.
