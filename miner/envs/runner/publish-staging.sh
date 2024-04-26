#!/bin/bash -eux

( cd nginx && ./publish-image.sh )

source build-staging.sh
source _publish-image.sh
