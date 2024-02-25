#!/bin/bash -eux

IMAGE_NAME="backenddevelopersltd/compute-horde-validator:v0-latest"
rsync -avzP ../../compute_horde packages/
# Dockerfile assumes that build is run in the parent dir
cd .. && docker build -t $IMAGE_NAME -f app/envs/prod/Dockerfile .
