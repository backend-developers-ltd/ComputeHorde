#!/bin/bash
set -eu

if [ "$(basename "$0")" == 'bin' ]; then
  cd ..
fi

if [ ! -f "$1" ]; then
  echo "Pass existing backup file name (with .backups/ directory name) as the first argument"
  exit 127
fi

. .env

date

EMAIL_CREDS="${EMAIL_HOST_USER}:${EMAIL_HOST_PASSWORD}@${EMAIL_HOST}:${EMAIL_PORT}" bin/emailhelper.py --to "${EMAIL_TARGET}" --subject "Backup of ${POSTGRES_DB}" -f "$1"

echo "Email sent successfully"
