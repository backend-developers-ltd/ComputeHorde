#!/bin/bash -eux

IMAGE_NAME="ghcr.io/backend-developers-ltd/computehorde/job:v0"
docker build -t $IMAGE_NAME .
