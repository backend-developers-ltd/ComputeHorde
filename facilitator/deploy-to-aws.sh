#!/bin/bash -e
# shellcheck disable=2086
./devops/scripts/build-backend.sh $1
./devops/scripts/deploy-backend.sh $1
