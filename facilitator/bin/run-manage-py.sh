#!/bin/bash
if [ "$(basename "$0")" == 'bin' ]; then
  cd ..
fi
docker compose exec app sh -c "python manage.py $*"
