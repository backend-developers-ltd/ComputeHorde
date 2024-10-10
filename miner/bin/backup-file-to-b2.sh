#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"

check_env_vars BACKUP_B2_KEY_ID BACKUP_B2_KEY_SECRET BACKUP_B2_BUCKET

if [ "$1" == "-" ]; then
  BACKUP_B2_FILENAME="$2"
  [ -n "$BACKUP_B2_FILENAME" ] || (echo "Pass backup file name as the second argument if stdin was provided as data source">&2; exit 2)
elif [ ! -f "$1" ]; then
  echo "Pass existing backup file name (with .backups/ directory name) as the first argument"
  exit 2
else
  BACKUP_B2_FILENAME"$(basename "$1")"
fi

export B2_APPLICATION_KEY_ID="$BACKUP_B2_KEY_ID"
export B2_APPLICATION_KEY="$BACKUP_B2_KEY_SECRET"
docker run --rm -iq -e B2_APPLICATION_KEY -e B2_APPLICATION_KEY_ID \
  backblazeit/b2:3.13.1 upload-file "$BACKUP_B2_BUCKET" "$1" "$BACKUP_B2_FILENAME"
