#!/bin/bash
set -eux -o pipefail

( cd nginx && ./publish-image.sh )

source build-staging.sh
source _publish-image.sh
