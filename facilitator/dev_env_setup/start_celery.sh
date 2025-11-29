#!/bin/sh
set -eu

THIS_DIR=$(dirname "$(readlink -f "$0")")
cd "$THIS_DIR"

cd ../app/src
./manage.py migrate

CELERY_CONCURRENCY=${CELERY_CONCURRENCY:-2}
CELERY_LOGLEVEL=${CELERY_LOGLEVEL:-DEBUG}

WORKERS="master worker"
OPTIONS="-E -l $CELERY_LOGLEVEL --pidfile=/tmp/celery-facilitator-%n.pid --logfile=/tmp/celery-facilitator-%n.log"

MAC_OPTS=""
if [ "$(uname -s)" = "Darwin" ]; then
  MAC_OPTS="-P solo -c 1"
  # on mac os (at least on silicone) celery workers die with sigsev or sigabrt when connecting to postgres
  # without these options
fi

# shellcheck disable=2086
celery -A project multi start $WORKERS $OPTIONS $MAC_OPTS \
    -Q:master celery --autoscale:master=$CELERY_CONCURRENCY \
    -Q:worker worker --autoscale:worker=$CELERY_CONCURRENCY


tail -f /tmp/celery-facilitator-*.log &
TAIL_PID=$!

# shellcheck disable=2064
trap "kill -TERM $TAIL_PID 2>/dev/null; celery multi stop $WORKERS $OPTIONS; wait $TAIL_PID 2>/dev/null; exit 0" INT TERM


# check celery status periodically to exit if it crashed
while true; do
    sleep 30
    celery -A project status > /dev/null 2>&1 || exit 1
done
