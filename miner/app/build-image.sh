#!/bin/bash -eux

IMAGE_NAME="ghcr.io/reef-technologies/computehorde/miner-app"
rsync -avzP ../../compute_horde packages/
docker build -t $IMAGE_NAME -f envs/prod/Dockerfile .
