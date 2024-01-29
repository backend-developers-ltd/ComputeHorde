#!/bin/bash -e

# Define image
BASE_IMAGE_NAME="ghcr.io/backend-developers-ltd/computehorde/job_base:v0"
IMAGE_NAME="ghcr.io/backend-developers-ltd/computehorde/job:v0"

# Build the Docker image
docker build --target base -t $BASE_IMAGE_NAME .
docker build --target job -t $IMAGE_NAME .

# Login to GitHub Docker Registry
echo $GITHUB_CR_PAT | docker login ghcr.io -u USERNAME --password-stdin

# Push image to GitHub Docker Registry
docker push $BASE_IMAGE_NAME
docker push $IMAGE_NAME
