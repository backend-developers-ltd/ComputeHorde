#!/bin/bash -eux

: "${IMAGE_NAME:=backenddevelopersltd/compute-horde-miner:v0-latest}"
# Dockerfile assumes that build is run in the parent dir
cd .. && docker build --platform linux/amd64 --build-context compute-horde=../compute_horde -t $IMAGE_NAME -f app/envs/prod/Dockerfile .
