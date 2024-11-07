#!/bin/sh

# We assume that WORKDIR is defined in Dockerfile

./prometheus-cleanup.sh
. ./vendor_setup.sh
./manage.py wait_for_database --timeout 10
./manage.py migrate --no-input
./manage.py collectstatic --no-input

daphne -b 0.0.0.0 -p 8000 --ping-timeout 120 compute_horde_miner.asgi:application
