#!/usr/bin/env bash
# Copyright 2017, Reef Technologies (reef.pl), All rights reserved.

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ENV_DIR="./envs/prod"
# shellcheck disable=SC2164
cd "${PROJECT_DIR}"

# Create .env from the template if doesn't exist
[[ -f "${ENV_DIR}/.env" ]] || cp "${ENV_DIR}/.env.template" "${ENV_DIR}/.env"

# Set symlinks
ln -sf "${ENV_DIR}/.env" .env
ln -sf "${ENV_DIR}/docker-compose.yml" docker-compose.yml
# shellcheck disable=SC2164
cd "${PROJECT_DIR}/app/"
ln -sf "${ENV_DIR}/Dockerfile" Dockerfile
ln -sf ".${ENV_DIR}/entrypoint.sh" src/entrypoint.sh
