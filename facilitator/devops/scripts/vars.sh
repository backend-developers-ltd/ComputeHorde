#!/bin/bash
# shellcheck disable=SC2034
[ "$1" != "staging" ] && [ "$1" != "prod" ] &&  echo "Please provide environment name to deploy: staging or prod" && exit 1;

PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../

APP_SUFFIX="-$1"

APP_OWNER=$(aws sts get-caller-identity --region us-east-1 --query "Account" --output text)
APP_REGION="us-east-1"
APP_NAME="facilitator${APP_SUFFIX}"
CLOUDFRONT_BUCKET="${APP_NAME}-spa${APP_SUFFIX}"