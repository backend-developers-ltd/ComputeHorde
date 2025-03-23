PROJECT_ROOT=$(git rev-parse --show-toplevel)

cd $PROJECT_ROOT/facilitator

uv run python app/src/manage.py runserver 127.0.0.1:9000