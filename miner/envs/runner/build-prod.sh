#!/bin/bash -eux

( cd nginx && ./build-image.sh )

IMAGE_NAME="backenddevelopersltd/compute-horde-miner-runner:v0-latest"
MINER_IMAGE_REPO="compute-horde-miner"
source _build-image.sh
