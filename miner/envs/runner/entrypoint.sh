#!/bin/sh
set -eu

# explicitly pull the docker compose images to verify DCT
export DOCKER_CONTENT_TRUST=1
docker compose convert --images | sort -u | xargs -n 1 docker pull

docker compose up --detach --wait --force-recreate

while true
do
    docker compose logs -f
    echo 'All containers died'
    sleep 10
done
