#!/bin/bash -eux

IMAGE_NAME="ghcr.io/reef-technologies/computehorde/miner-runner"
rsync -avzP ../../nginx/config_helpers data/nginx/
docker build -t $IMAGE_NAME .
