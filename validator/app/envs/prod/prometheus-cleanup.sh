#!/bin/sh
set -e

if [ -n "$PROMETHEUS_MULTIPROC_DIR" ]; then
    if [ -d "$PROMETHEUS_MULTIPROC_DIR" ]; then
        # Delete prometheus metric files in PROMETHEUS_MULTIPROC_DIR, but not in its subdirectories to not
        # interfere with other processes.
        find "$PROMETHEUS_MULTIPROC_DIR" -maxdepth 1 -type f -name '*.db' -delete
    else
        # Ensure the directory exists
        mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
    fi
fi
