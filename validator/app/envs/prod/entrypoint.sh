#!/bin/sh

# We assume that WORKDIR is defined in Dockerfile

./prometheus-cleanup.sh
./manage.py wait_for_database --timeout 10
./manage.py migrate --no-input

gunicorn -c gunicorn.conf.py
