PROJECT_ROOT=$(git rev-parse --show-toplevel)

cd $PROJECT_ROOT/validator/dev_env_setup

while true; do

uv run bash start_celery.sh

done
