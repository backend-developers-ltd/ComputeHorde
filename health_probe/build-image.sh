#!/bin/bash
set -eux -o pipefail

IMAGE_NAME="backenddevelopersltd/compute-horde-health-probe:v0-latest"
# Dockerfile assumes that build is run in the parent dir
docker build --platform linux/amd64 --build-context compute-horde-sdk=../compute_horde_sdk -t $IMAGE_NAME -f envs/prod/Dockerfile .
