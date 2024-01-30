#!/bin/bash -eux

IMAGE_NAME="backenddevelopersltd/compute-horde-validator:v0-latest"
rsync -avzP ../../compute_horde packages/
docker build -t $IMAGE_NAME -f envs/prod/Dockerfile .
