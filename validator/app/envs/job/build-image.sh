#!/bin/bash -eux

BASE_IMAGE_NAME="backenddevelopersltd/compute-horde-job-base:v0-latest"
IMAGE_NAME="backenddevelopersltd/compute-horde-job:v0-latest"

docker build --target base -t "$BASE_IMAGE_NAME" .
docker build --target job -t "$IMAGE_NAME" .