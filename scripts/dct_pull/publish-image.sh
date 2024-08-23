#!/bin/bash
set -eux -o pipefail

export DOCKER_CONTENT_TRUST=1

IMAGE_NAME=backenddevelopersltd/compute-horde-dct-pull:v0-latest 

docker build \
	--platform=linux/amd64 \
	-t $IMAGE_NAME \
	.

if [ -z "$(docker info 2>/dev/null | grep 'Username' | awk '{print $2}')" ]; then
	echo "$DOCKERHUB_PAT" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
fi

docker push "$IMAGE_NAME"
