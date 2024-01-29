#!/bin/bash -eux

IMAGE_NAME="ghcr.io/reef-technologies/computehorde/job:v0"
docker build -t $IMAGE_NAME .
