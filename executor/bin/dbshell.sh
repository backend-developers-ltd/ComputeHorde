#!/bin/bash

if [ "$(basename "$0")" == 'bin' ]; then
  cd ..
fi

. .env

if [[ "$DATABASE_URL" =~ "@db:" ]]; then
  DOCKER_NETWORK=executor_default
else
  DOCKER_NETWORK=host
fi

# this works even if `app` container doesn't have psql installed (where `bin/run-manage-py.sh dbshell` fails)
docker run -it --rm --network "$DOCKER_NETWORK" postgres::16-alpine psql "$DATABASE_URL"
