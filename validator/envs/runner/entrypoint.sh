#!/bin/sh
set -eu

docker compose up --detach --wait --force-recreate --remove-orphans

while true
do
    docker compose logs -f
    echo 'All containers died'
    sleep 10
done
