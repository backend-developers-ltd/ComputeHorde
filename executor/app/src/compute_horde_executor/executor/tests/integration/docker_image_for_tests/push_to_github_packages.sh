#!/bin/bash -e

# Define image
IMAGE_NAME="ghcr.io/backend-developers-ltd/computehorde/echo:latest"

# Build the Docker image
docker build -t $IMAGE_NAME .

# Login to GitHub Docker Registry
echo $GITHUB_CR_PAT | docker login ghcr.io -u USERNAME --password-stdin

# Push image to GitHub Docker Registry
docker push $IMAGE_NAME