#!/bin/bash -eux

IMAGE_NAME="backenddevelopersltd/compute-horde-executor:v0-latest"
# Dockerfile assumes that build is run in the parent dir
cd .. && docker build --build-context compute-horde=../compute_horde -t $IMAGE_NAME -f app/envs/prod/Dockerfile .
