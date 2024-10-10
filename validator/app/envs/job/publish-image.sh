#!/bin/bash
set -euxo pipefail

source ./build-image.sh
echo "$GITHUB_CR_PAT" | docker login ghcr.io -u USERNAME --password-stdin
docker push "$BASE_IMAGE_NAME"
docker push "$IMAGE_NAME"
