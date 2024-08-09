#!/bin/bash
set -euxo pipefail

if [ -z "$(docker info 2>/dev/null | grep 'Username' | awk '{print $2}')" ]; then
	echo "$DOCKERHUB_PAT" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
fi

docker push "$IMAGE_NAME"
