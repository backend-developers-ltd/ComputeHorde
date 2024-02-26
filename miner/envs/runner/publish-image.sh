#!/bin/bash -eux

source ./build-image.sh
( cd nginx && ./publish-image.sh )
echo "$DOCKERHUB_PAT" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
docker push "$IMAGE_NAME"
