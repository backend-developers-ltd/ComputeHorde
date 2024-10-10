#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/common.sh"

if [[ $# -ne 1 ]]; then
    echo "Usage: bin/restore-db-from-b2.sh <B2_FILE_ID>"
    echo "All arguments are required"
    exit 2
fi

B2_FILE_ID="$1"

export B2_APPLICATION_KEY_ID="$BACKUP_B2_KEY_ID"
export B2_APPLICATION_KEY="$BACKUP_B2_KEY_SECRET"
docker run --rm -iq -e B2_APPLICATION_KEY -e B2_APPLICATION_KEY_ID \
  backblazeit/b2:3.13.1 cat "b2id://$B2_FILE_ID" | "${SCRIPT_DIR}"/restore-db.sh -
