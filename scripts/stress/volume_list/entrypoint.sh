#!/bin/sh

set -eu

SLEEP_DURATION=""

while [ $# -gt 0 ]; do
  case "$1" in
    --sleep)
      shift
      SLEEP_DURATION="$1"
      ;;
    --sleep=*)
      SLEEP_DURATION="${1#*=}"
      ;;
  esac
  shift
done

echo "$(date) --- BEGIN ---"
find /volume | sort | tee /output/files.txt
echo "$(date) --- END ---"

if [ -n "$SLEEP_DURATION" ]; then
  echo "$(date) --- SLEEPING FOR ${SLEEP_DURATION} SEC ---"
  sleep "$SLEEP_DURATION"
  echo "$(date) --- SLEEPING DONE ---"
fi
