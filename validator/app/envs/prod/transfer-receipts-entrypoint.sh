#!/bin/sh
set -eu

./prometheus-cleanup.sh
./manage.py wait_for_database --timeout 10

./manage.py transfer_receipts "$@"