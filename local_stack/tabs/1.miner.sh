PROJECT_ROOT=$(git rev-parse --show-toplevel)

cd $PROJECT_ROOT/miner

uv run python app/src/manage.py runserver