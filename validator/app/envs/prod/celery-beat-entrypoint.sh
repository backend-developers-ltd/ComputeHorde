#!/bin/bash
set -eu

type prometheus-cleanup.sh && ./prometheus-cleanup.sh

nice celery -A compute_horde_validator beat -l INFO --schedule /tmp/celerybeat-schedule -f /tmp/celery-beat.log &
pid=$!

tail -f /tmp/celery-beat.log &

wait $pid