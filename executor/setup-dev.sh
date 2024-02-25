#!/usr/bin/env bash
# Copyright 2017, Reef Technologies (reef.pl), All rights reserved.
set -e

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ENV_DIR="./envs/dev"
# shellcheck disable=SC2164
cd "${PROJECT_DIR}"

# Create a lock file, install Python dependencies
[ -f pdm.lock ] || pdm lock --group :all
pdm sync --group :all

# Create .env from the template if doesn't exist
[[ -f "${ENV_DIR}/.env" ]] || cp "${ENV_DIR}/.env.template" "${ENV_DIR}/.env"

# Set symlinks
ln -sf "${ENV_DIR}/.env" .env
ln -sf "${ENV_DIR}/docker-compose.yml" docker-compose.yml
# shellcheck disable=SC2164
cd "${PROJECT_DIR}/app/"
[[ -L "Dockerfile" ]] && unlink Dockerfile
[[ -L "src/entrypoint.sh" ]] && unlink src/entrypoint.sh

# Ensure that the script returns zero for the CI
exit 0
