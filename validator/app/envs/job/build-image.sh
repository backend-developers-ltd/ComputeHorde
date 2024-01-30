#!/bin/bash -eux

BASE_IMAGE_NAME="ghcr.io/backend-developers-ltd/computehorde/job_base:v0"
IMAGE_NAME="ghcr.io/backend-developers-ltd/computehorde/job:v0"

docker build --target base -t "$BASE_IMAGE_NAME" .
docker build --target job -t "$IMAGE_NAME" .