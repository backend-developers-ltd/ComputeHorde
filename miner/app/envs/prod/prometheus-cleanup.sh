#!/bin/sh
set -e

if [ -n "$PROMETHEUS_MULTIPROC_DIR" ]; then
    if [ -d "$PROMETHEUS_MULTIPROC_DIR" ]; then
        # Delete prometheus live metric files in PROMETHEUS_MULTIPROC_DIR, but not in its subdirectories to not
        # interfere with other processes.  Note that this is equivalent to what multiprocess.mark_process_dead does,
        # see https://github.com/prometheus/client_python/blob/master/prometheus_client/multiprocess.py#L159
        find "$PROMETHEUS_MULTIPROC_DIR" -maxdepth 1 -type f -name 'gauge_live*_*.db' -delete
    else
        # Ensure the directory exists
        mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
    fi
fi
