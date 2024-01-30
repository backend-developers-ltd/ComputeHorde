#!/bin/bash -eux

IMAGE_NAME="ghcr.io/backend-developers-ltd/computehorde/executor-app:v0-latest"
rsync -avzP ../../compute_horde packages/
docker build -t $IMAGE_NAME -f envs/prod/Dockerfile .
