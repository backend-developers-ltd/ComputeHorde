#!/bin/sh
set -eu

THIS_DIR=$(dirname "$(readlink -f "$0")")
cd "$THIS_DIR"

cd ../app/src
./manage.py migrate

CELERY_CONCURRENCY=${CELERY_CONCURRENCY:-2}
CELERY_LOGLEVEL=${CELERY_LOGLEVEL:-DEBUG}

WORKERS="default weights jobs scores llm receipts organic_jobs metagraph"
OPTIONS="-E -l $CELERY_LOGLEVEL --pidfile=/tmp/celery-validator-%n.pid --logfile=/tmp/celery-validator-%n.log"

MAC_OPTS=""
if [ "$(uname -s)" = "Darwin" ]; then
  MAC_OPTS="-P solo -c 1"
  # on mac os (at least on silicone) celery workers die with sigsev or sigabrt when connecting to postgres
  # without these options
fi

# shellcheck disable=2086
celery -A compute_horde_validator multi start $WORKERS $OPTIONS $MAC_OPTS \
    -Q:default default --autoscale:generic=$CELERY_CONCURRENCY \
    -Q:weights weights --autoscale:weights=$CELERY_CONCURRENCY \
    -Q:jobs jobs --autoscale:jobs=$CELERY_CONCURRENCY \
    -Q:scores scores --autoscale:scores=$CELERY_CONCURRENCY \
    -Q:llm llm --autoscale:llm=$CELERY_CONCURRENCY \
    -Q:receipts receipts --autoscale:receipts=$CELERY_CONCURRENCY \
    -Q:metagraph metagraph --autoscale:metagraph=$CELERY_CONCURRENCY \
    -Q:organic_jobs organic_jobs --autoscale:receipts=$CELERY_CONCURRENCY


tail -f /tmp/celery-validator-*.log &
TAIL_PID=$!

# shellcheck disable=2064
trap "kill -TERM $TAIL_PID 2>/dev/null; celery multi stop $WORKERS $OPTIONS; wait $TAIL_PID 2>/dev/null; exit 0" INT TERM


# check celery status periodically to exit if it crashed
while true; do
    sleep 30
    celery -A compute_horde_validator status > /dev/null 2>&1 || exit 1
done
