#!/bin/sh
set -eu

CELERY_CONCURRENCY_MIN=${CELERY_CONCURRENCY_MIN:-2}
CELERY_CONCURRENCY_MAX=${CELERY_CONCURRENCY_MAX:-6}
CELERY_LOGLEVEL=${CELERY_LOGLEVEL:-INFO}
CELERY_MAX_TASKS_PER_CHILD=${CELERY_MAX_TASKS_PER_CHILD:-30}

./prometheus-cleanup.sh
./manage.py wait_for_database --timeout 10

celery -A compute_horde_validator worker \
  -E \
  -l "${CELERY_LOGLEVEL}" \
  --autoscale "${CELERY_CONCURRENCY_MAX},${CELERY_CONCURRENCY_MIN}" \
  --max-tasks-per-child "${CELERY_MAX_TASKS_PER_CHILD}" \
  "$@"
