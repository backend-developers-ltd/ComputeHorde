#!/usr/bin/env bash
# Copyright 2017, Reef Technologies (reef.pl), All rights reserved.

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ENV_DIR="./envs/dev"
# shellcheck disable=SC2164
cd "${PROJECT_DIR}"

# Check if we are inside virtualenv or CI
[[ -n $VIRTUAL_ENV || -n $CI ]] || { echo -e "\e[31mYou must run this script inside virtualenv!\e[0m"; exit 1; }

# Install pip packages
[[ -z $(pip freeze) ]] || echo -e "\e[33mVirtualenv is not clean, already installed packages may be upgraded\e[0m"
echo "Installing pip development requirements"
pip install --upgrade pip
pip install --upgrade -r "${PROJECT_DIR}/app/src/requirements.txt"

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
