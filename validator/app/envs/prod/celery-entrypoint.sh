#!/bin/sh
set -eu

./prometheus-cleanup.sh
./manage.py wait_for_database --timeout 10

celery -A compute_horde_validator worker "$@"
