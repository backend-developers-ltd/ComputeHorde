#!/bin/bash
set -euxo pipefail

IMAGE_NAME="backenddevelopersltd/compute-horde-miner-nginx:v0-latest"
docker build --platform=linux/amd64 -t "$IMAGE_NAME" .
