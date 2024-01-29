#!/bin/bash -eux

source ./build-image.sh
echo "$GITHUB_CR_PAT" | docker login ghcr.io -u USERNAME --password-stdin
docker push "$IMAGE_NAME"
