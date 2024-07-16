#!/bin/bash -e

# Define image
IMAGE_NAME="andreeareef/compute-horde-job-echo:v0-latest"

# Build the Docker image
docker build -t $IMAGE_NAME .

# Login to GitHub Docker Registry
echo "$DOCKERHUB_PAT_TEST" | docker login -u "$DOCKERHUB_USERNAME_TEST" --password-stdin

# Push image to GitHub Docker Registry
docker push $IMAGE_NAME
