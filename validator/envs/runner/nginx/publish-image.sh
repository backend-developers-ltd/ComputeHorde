#!/bin/bash
set -eux -o pipefail

source ./build-image.sh
if [ -z "$(docker info 2>/dev/null | grep 'Username' | awk '{print $2}')" ]; then
	echo "$DOCKERHUB_PAT_TEST" | docker login -u "$DOCKERHUB_USERNAME_TEST" --password-stdin
fi
export DOCKER_CONTENT_TRUST=1
docker push "$IMAGE_NAME"
