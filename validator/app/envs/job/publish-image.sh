#!/bin/bash
set -eux -o pipefail

source ./build-image.sh
echo "$GITHUB_CR_PAT" | docker login ghcr.io -u USERNAME --password-stdin
export DOCKER_CONTENT_TRUST=1
docker push "$BASE_IMAGE_NAME"
docker push "$IMAGE_NAME"
