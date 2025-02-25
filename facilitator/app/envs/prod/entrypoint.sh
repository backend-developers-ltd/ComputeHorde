#!/bin/sh

# We assume that WORKDIR is defined in Dockerfile

./prometheus-cleanup.sh
./manage.py wait_for_database --timeout 10
# this seems to be the only place to put this for AWS deployments to pick it up
./manage.py migrate

gunicorn -c gunicorn.conf.py
