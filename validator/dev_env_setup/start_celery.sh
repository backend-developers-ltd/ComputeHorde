#!/bin/sh
set -eu

THIS_DIR=$(dirname "$(readlink -f "$0")")
cd "$THIS_DIR"

cd ../app/src
./manage.py migrate

# below we define two workers types (each may have any concurrency);
# each worker may have its own settings
WORKERS="master worker"
OPTIONS="-A compute_horde_validator -E -l DEBUG --pidfile=/tmp/celery-validator-%n.pid --logfile=/tmp/celery-validator-%n.log"

CELERY_MASTER_CONCURRENCY=2
CELERY_WORKER_CONCURRENCY=2

celery multi start "$WORKERS" "$OPTIONS" \
    -Q:master celery --autoscale:master=$CELERY_MASTER_CONCURRENCY,0 \
    -Q:worker worker --autoscale:worker=$CELERY_WORKER_CONCURRENCY,0

# shellcheck disable=2064
trap "celery multi stop $WORKERS $OPTIONS; exit 0" INT TERM

tail -f /tmp/celery-validator-*.log &

# check celery status periodically to exit if it crashed
while true; do
    sleep 30
    celery -A compute_horde_validator status > /dev/null 2>&1 || exit 1
done
