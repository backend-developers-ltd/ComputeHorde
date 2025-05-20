#!/bin/sh
set -eu

THIS_DIR=$(dirname "$(readlink -f "$0")")
cd "$THIS_DIR"

cd ../app/src
./manage.py migrate

# Default concurrency = 2
CELERY_CONCURRENCY=${CELERY_CONCURRENCY:-2}
# Default loglevel = DEBUG
CELERY_LOGLEVEL=${CELERY_LOGLEVEL:-DEBUG}

# below we define two workers types (each may have any concurrency);
# each worker may have its own settings
WORKERS="default weights jobs llm receipts organic_jobs"
OPTIONS="-E -l $CELERY_LOGLEVEL --pidfile=/tmp/celery-validator-%n.pid --logfile=/tmp/celery-validator-%n.log"

# shellcheck disable=SC2086
celery -A compute_horde_validator multi start $WORKERS $OPTIONS \
    -Q:default default --autoscale:generic=$CELERY_CONCURRENCY \
    -Q:weights weights --autoscale:weights=$CELERY_CONCURRENCY \
    -Q:jobs jobs --autoscale:jobs=$CELERY_CONCURRENCY \
    -Q:scores scores --autoscale:scores=$CELERY_CONCURRENCY \
    -Q:receipts receipts --autoscale:receipts=$CELERY_CONCURRENCY \
    -Q:organic_jobs organic_jobs --autoscale:receipts=$CELERY_CONCURRENCY

# shellcheck disable=2064
trap "celery multi stop $WORKERS $OPTIONS; exit 0" INT TERM

tail -f /tmp/celery-validator-*.log &

# check celery status periodically to exit if it crashed
while true; do
    sleep 30
    celery -A compute_horde_validator status > /dev/null 2>&1 || exit 1
done
