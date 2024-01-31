#!/bin/bash -eux

( cd nginx && ./build-image.sh )

IMAGE_NAME="backenddevelopersltd/compute-horde-miner-runner:v0-latest"
docker build -t $IMAGE_NAME .
