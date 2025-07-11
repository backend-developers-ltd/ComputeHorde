#!/bin/sh -eux
# Copyright 2020, Reef Technologies (reef.pl), All rights reserved.

if [ ! -f ".env" ]; then
    echo "\e[31mPlease setup the environment first!\e[0m";
    exit 1;
fi

DATE_UTC=$(date -u)
TIMESTAMP_UTC=$(date +%s)
COMMIT_HASH=$(git rev-parse --short HEAD || echo -n "local")

DOCKER_BUILDKIT=1 docker build \
  -f app/Dockerfile \
  --progress plain \
  --platform linux/amd64 \
  -t project/app \
  --build-context compute-horde-sdk=../compute_horde_sdk \
  --build-context compute-horde=../compute_horde \
  --build-arg GITHUB_TOKEN="justplainwrong" \
  --build-arg HTTP_ASGI_APPLICATION_PATH="django.core.asgi.get_asgi_application" \
  --label build_date_utc="$DATE_UTC" \
  --label build_timestamp_utc="$TIMESTAMP_UTC" \
  --label git_commit_hash="$COMMIT_HASH" \
  .


# Tag the first image from multi-stage app Dockerfile to mark it as not dangling
BASE_IMAGE=$(docker images --quiet --filter="label=builder=true" | head -n1)
docker image tag "${BASE_IMAGE}" project/app-builder

# collect static files to external storage while old app is still running
# docker compose run --rm app sh -c "python manage.py collectstatic --no-input"

SERVICES=$(docker compose ps --services 2>&1 > /dev/stderr \
           | grep -v -e 'is not set' -e db -e redis)

# shellcheck disable=2086
docker compose stop $SERVICES

# start the app container only in order to perform migrations
docker compose up -d db  # in case it hasn't been launched before
docker compose run --rm app sh -c "python manage.py wait_for_database --timeout 10; python manage.py migrate"

# start everything
docker compose up -d

# Clean all dangling images
docker images --quiet --filter=dangling=true \
    | xargs --no-run-if-empty docker rmi \
    || true
