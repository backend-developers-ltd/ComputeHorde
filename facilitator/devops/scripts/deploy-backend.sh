#!/bin/bash -xe

THIS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$THIS_DIR"/vars.sh

cd "$PROJECT_DIR"/app

echo "Deploying Backend: ${APP_NAME}"
docker push "${APP_OWNER}".dkr.ecr."${APP_REGION}".amazonaws.com/"${APP_NAME}":latest

aws autoscaling start-instance-refresh --region "${APP_REGION}" --auto-scaling-group-name "${APP_NAME}"
