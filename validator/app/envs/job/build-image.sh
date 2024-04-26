#!/bin/bash
set -eux -o pipefail

BASE_IMAGE_NAME="backenddevelopersltd/compute-horde-job-base:v0-latest"
IMAGE_NAME="backenddevelopersltd/compute-horde-job:v0-latest"

docker build --target base -t "$BASE_IMAGE_NAME" v0
docker build --target job -t "$IMAGE_NAME" v0

BASE_IMAGE_NAME="backenddevelopersltd/compute-horde-job-base:v1-latest"
IMAGE_NAME="backenddevelopersltd/compute-horde-job:v1-latest"

docker build --target base -t "$BASE_IMAGE_NAME" v1
docker build --target job -t "$IMAGE_NAME" v1
