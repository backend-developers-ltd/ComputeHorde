#!/bin/bash
set -eux -o pipefail

( cd nginx && ./publish-image.sh )

source build-prod.sh
source _publish-image.sh
