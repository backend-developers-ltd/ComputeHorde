#!/bin/bash
set -eux -o pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SCRIPT_DIR}"

MINER_RUNNER_VERSION=$(git rev-parse HEAD)
docker build \
	--platform=linux/amd64 \
	-t "$IMAGE_NAME" \
	--build-arg MINER_RUNNER_VERSION="${MINER_RUNNER_VERSION}" \
	--build-arg MINER_IMAGE_REPO="${MINER_IMAGE_REPO}" \
	.
