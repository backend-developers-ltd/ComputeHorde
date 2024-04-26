#!/bin/bash
set -eux -o pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
( cd "${SCRIPT_DIR}/nginx" && ./publish-image.sh )

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/build-prod.sh"
source "${SCRIPT_DIR}/_publish-image.sh"
