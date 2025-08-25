PROJECT_ROOT=$(git rev-parse --show-toplevel)

cd $PROJECT_ROOT/validator/dev_env_setup

uv run bash start_celery.sh
