#!/bin/bash
set -euxo pipefail

IMAGE_NAME="backenddevelopersltd/compute-horde-validator-nginx:v0-latest"
docker build -t $IMAGE_NAME .
