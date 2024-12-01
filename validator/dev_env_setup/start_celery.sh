#!/bin/sh
set -eu

THIS_DIR=$(dirname "$(readlink -f "$0")")
cd "$THIS_DIR"

cd ../app/src

CELERY_CONCURRENCY_MAX=${CELERY_CONCURRENCY_MAX:-4}
CELERY_CONCURRENCY_MIN=${CELERY_CONCURRENCY_MIN:-1}
CELERY_LOGLEVEL=${CELERY_LOGLEVEL:-INFO}
CELERY_MAX_TASKS_PER_CHILD=${CELERY_MAX_TASKS_PER_CHILD:-30}
QUEUES=${QUEUES:-"default,weights,jobs,scores"}

celery -A compute_horde_validator worker \
        -E -l "${CELERY_LOGLEVEL}" \
        --autoscale "${CELERY_CONCURRENCY_MAX},${CELERY_CONCURRENCY_MIN}" \
        --max-tasks-per-child "${CELERY_MAX_TASKS_PER_CHILD}" \
        -Q "${QUEUES}"
