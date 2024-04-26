#!/bin/bash
set -eux -o pipefail

( cd nginx && ./publish-image.sh )

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source "${SCRIPT_DIR}/build-prod.sh"
source "${SCRIPT_DIR}/_publish-image.sh"
