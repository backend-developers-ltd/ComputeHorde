#!/bin/bash
set -euxo pipefail

source build-prod.sh
echo "$DOCKERHUB_PAT" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
docker push "$IMAGE_NAME"
