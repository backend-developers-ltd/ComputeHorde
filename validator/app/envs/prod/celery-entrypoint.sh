#!/bin/sh
set -eu

./prometheus-cleanup.sh

# below we define two workers types (each may have any concurrency);
# each worker may have its own settings
WORKERS="master worker"
OPTIONS="-A compute_horde_validator -E -l ERROR --pidfile=/var/run/celery-%n.pid --logfile=/var/log/celery-%n.log"

# set up settings for workers and run the latter;
# here events from "celery" queue (default one, will be used if queue not specified)
# will go to "master" workers, and events from "worker" queue go to "worker" workers;
# by default there are no workers, but each type of worker may scale up to 4 processes
# Since celery runs in root of the docker, we also need to allow it to.
# shellcheck disable=2086
C_FORCE_ROOT=1 nice celery multi start $WORKERS $OPTIONS \
    -Q:master celery --autoscale:master=$CELERY_MASTER_CONCURRENCY,0 \
    -Q:worker worker --autoscale:worker=$CELERY_WORKER_CONCURRENCY,0

# shellcheck disable=2064
trap "celery multi stop $WORKERS $OPTIONS; exit 0" INT TERM

tail -f /var/log/celery-*.log &

# check celery status periodically to exit if it crashed
while true; do
    sleep 30
    celery -A compute_horde_validator status > /dev/null 2>&1 || exit 1
done
