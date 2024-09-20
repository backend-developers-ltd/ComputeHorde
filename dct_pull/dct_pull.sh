#!/bin/bash

export DOCKER_CONTENT_TRUST=1

if [ $# -eq 0 ]; then
    echo "Provide docker image names as arguments"
    exit 1
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

while true; do
    for image in "$@"; do 
        echo -e "\n\nPulling $image"
        docker pull $image && echo -e "${GREEN}Successfully pulled signed image: $image${NC}" || echo -e "${RED}Failed to pull signed image: $image${NC}"
    done
    sleep 1m
done

