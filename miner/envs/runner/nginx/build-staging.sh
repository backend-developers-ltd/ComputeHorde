#!/bin/bash
set -eux -o pipefail

IMAGE_NAME="backenddevelopersltd/compute-horde-miner-nginx:staging-latest"
docker build --platform=linux/amd64 -t "$IMAGE_NAME" .
