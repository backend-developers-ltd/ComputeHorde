#!/bin/sh
set -eu

THIS_DIR=$(dirname "$(readlink -f "$0")")
cd "$THIS_DIR"

cd ../app/src

CELERY_LOGLEVEL=${CELERY_LOGLEVEL:-DEBUG}

celery -A compute_horde_validator beat