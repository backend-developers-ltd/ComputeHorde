cd "$(dirname "$0")"

./prepare.sh

PROJECT_ROOT=$(git rev-parse --show-toplevel) screen -c screenrc

