#!/bin/bash -e

# Define image
IMAGE_NAME="backenddevelopersltd/compute-horde-job-echo:v0-latest"

# Build the Docker image
docker build --platform linux/amd64 -t $IMAGE_NAME .

# Login to GitHub Docker Registry
echo "$DOCKERHUB_PAT" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin

# Push image to GitHub Docker Registry
docker push $IMAGE_NAME
