#!/bin/sh
set -eu

CELERY_CONCURRENCY=${CELERY_CONCURRENCY:-2}
CELERY_LOGLEVEL=${CELERY_LOGLEVEL:-INFO}

./prometheus-cleanup.sh
./manage.py wait_for_database --timeout 10

celery -A compute_horde_validator worker -E -l "$CELERY_LOGLEVEL" -c "${CELERY_CONCURRENCY}" "$@"
