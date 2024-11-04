#!/bin/sh
set -eu

CELERY_CONCURRENCY=${CELERY_CONCURRENCY:-2}
CELERY_LOGLEVEL=${CELERY_LOGLEVEL:-INFO}
CELERY_MAX_TASKS_PER_CHILD=${CELERY_MAX_TASKS_PER_CHILD:-10}

./prometheus-cleanup.sh
./manage.py wait_for_database --timeout 10

celery -A compute_horde_validator worker \
  -E \
  -l "${CELERY_LOGLEVEL}" \
  -c "${CELERY_CONCURRENCY}" \
  --max-tasks-per-child "${CELERY_MAX_TASKS_PER_CHILD}" \
  "$@"
