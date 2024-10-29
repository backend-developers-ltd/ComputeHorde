#!/bin/sh
set -eu

if [ -z "${CELERY_QUEUE:-}" ]; then
    echo "Error: you must set the CELERY_QUEUE variable to the name of the queue for this worker."
    exit 1
fi

# Default loglevel = INFO
CELERY_LOGLEVEL=${CELERY_LOGLEVEL:-INFO}

type prometheus-cleanup.sh && ./prometheus-cleanup.sh

./manage.py wait_for_database --timeout 10

celery -A compute_horde_validator worker -Q $CELERY_QUEUE -E -l $CELERY_LOGLEVEL -c $CELERY_CONCURRENCY
