#!/bin/bash -eu
set -o pipefail

if [ -z "${_COMMON_SH_LOADED:-}" ]; then
  # Update PATH in case docker-compose is installed via PIP
  # and this script was invoked from e.g. cron
  PATH=/usr/local/sbin:/usr/local/bin:$PATH

  check_env_vars() {
      local required_vars=("$@")
      local missing_vars=""
      for var in "${required_vars[@]}"; do
          if [ -z "${!var}" ]; then
              missing_vars+="$var "
          fi
      done

      if [ -n "$missing_vars" ]; then
          echo "Error: The following required environment variables are missing: $missing_vars" >&2
          exit 2
      fi
  }


  load_project_env() {
      if [ "$(basename "$0")" == 'bin' ]; then
        cd ..
      fi

      . .env
  }

  get_db_docker_network() {
    if [[ "$DATABASE_URL" =~ "@db:" ]]; then
      echo facilitator_default
    else
      echo host
    fi
  }

  load_project_env

  if [ -n "${SENTRY_DSN}" ]; then
    export SENTRY_DSN
    eval "$(sentry-cli bash-hook)"
  fi

  _COMMON_SH_LOADED=true
fi