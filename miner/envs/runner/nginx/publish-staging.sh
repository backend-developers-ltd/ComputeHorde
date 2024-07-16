#!/bin/bash
set -eux -o pipefail

source build-staging.sh
echo "$DOCKERHUB_PAT_TEST" | docker login -u "$DOCKERHUB_USERNAME_TEST" --password-stdin
export DOCKER_CONTENT_TRUST=1
docker push "$IMAGE_NAME"
