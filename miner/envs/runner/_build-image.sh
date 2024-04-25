#!/bin/bash -eux

# TODO:
# ( cd nginx && ./build-image.sh )

MINER_RUNNER_VERSION=$(git rev-parse HEAD)
docker build \
	--platform=linux/amd64 \
	-t "$IMAGE_NAME" \
	--build-arg MINER_RUNNER_VERSION="${MINER_RUNNER_VERSION}" \
	--build-arg MINER_IMAGE_REPO="${MINER_IMAGE_REPO}" \
	.
