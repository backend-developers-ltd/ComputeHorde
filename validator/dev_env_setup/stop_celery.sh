#!/bin/sh
# shellcheck disable=SC2086
set -eu

THIS_DIR=$(dirname "$(readlink -f "$0")")
cd "$THIS_DIR"

cd ../app/src

# Default concurrency = 2
CELERY_CONCURRENCY=${CELERY_CONCURRENCY:-2}
# Default loglevel = INFO
CELERY_LOGLEVEL=${CELERY_LOGLEVEL:-INFO}

# below we define two workers types (each may have any concurrency);
# each worker may have its own settings
WORKERS="default weights jobs llm receipts organic_jobs"
OPTIONS="-E -l $CELERY_LOGLEVEL --pidfile=/tmp/celery-validator-%n.pid --logfile=/tmp/celery-validator-%n.log"

celery multi stop $WORKERS $OPTIONS
