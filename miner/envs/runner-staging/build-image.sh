#!/bin/bash -eux

( cd nginx && ./build-image.sh )

IMAGE_NAME="backenddevelopersltd/compute-horde-miner-runner-staging:v0-latest"
docker build --platform=linux/amd64 -t $IMAGE_NAME .
