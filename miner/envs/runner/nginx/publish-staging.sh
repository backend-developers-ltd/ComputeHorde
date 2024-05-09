#!/bin/bash
set -eux -o pipefail

source build-staging.sh
echo "$DOCKERHUB_PAT" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
docker push "$IMAGE_NAME"
