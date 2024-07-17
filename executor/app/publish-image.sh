#!/bin/bash
set -eux -o pipefail

source ./build-image.sh
echo "$DOCKERHUB_PAT" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
export DOCKER_CONTENT_TRUST=1
docker push "$IMAGE_NAME"
