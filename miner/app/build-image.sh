#!/bin/bash
set -eux -o pipefail

IMAGE_NAME="${DOCKER_REPOSITORY_NAME:-backenddevelopersltd/compute-horde-miner:v0-latest}"

cd ..
docker build \
  --platform linux/amd64 \
  --build-context compute-horde=../compute_horde \
  -t "$IMAGE_NAME" \
  -f app/envs/prod/Dockerfile .