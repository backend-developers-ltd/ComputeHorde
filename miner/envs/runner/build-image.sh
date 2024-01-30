#!/bin/bash -eux

IMAGE_NAME="ghcr.io/backend-developers-ltd/computehorde/miner-runner"
rsync -avzP ../../nginx/config_helpers data/nginx/
docker build -t $IMAGE_NAME .
