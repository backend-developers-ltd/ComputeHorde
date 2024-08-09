#!/bin/bash
set -euxo pipefail

source ./build-image.sh
echo "$DOCKERHUB_PAT" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
docker push "$IMAGE_NAME"
