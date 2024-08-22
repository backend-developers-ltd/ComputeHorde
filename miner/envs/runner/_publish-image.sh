#!/bin/bash
set -eux -o pipefail

if [ -z "$(docker info 2>/dev/null | grep 'Username' | awk '{print $2}')" ]; then
	echo "$DOCKERHUB_PAT" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
fi

export DOCKER_CONTENT_TRUST=1
docker push "$IMAGE_NAME"
