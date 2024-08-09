#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"

BACKUP_B2_KEY_ID="${1-$BACKUP_B2_KEY_ID}"
BACKUP_B2_KEY_SECRET="${2-$BACKUP_B2_KEY_SECRET}"
BACKUP_B2_BUCKET="${3-$BACKUP_B2_BUCKET}"

export B2_APPLICATION_KEY_ID="$BACKUP_B2_KEY_ID"
export B2_APPLICATION_KEY="$BACKUP_B2_KEY_SECRET"
docker run --rm -iq -e B2_APPLICATION_KEY -e B2_APPLICATION_KEY_ID \
  backblazeit/b2:3.13.1 ls --long "$BACKUP_B2_BUCKET"
