PROJECT_ROOT=$(git rev-parse --show-toplevel)

cd $PROJECT_ROOT/validator

while true; do

uv run python app/src/manage.py connect_facilitator

done
