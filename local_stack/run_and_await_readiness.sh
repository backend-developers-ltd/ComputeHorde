cd "$(dirname "$0")"

set -e
set -x

LOG_DIR="$1"

if [[ -z "$LOG_DIR" ]]; then
  echo "Usage: run_and_await_readiness.sh <LOG_DIR>"
  return 1
fi

check_endpoint() {
  local url=$1
  local max_attempts=$2
  local wait_time=${3:-1} # Optional: default to 1 second if not provided

  for ((i=1; i<=max_attempts; i++)); do
    if curl -s -o /dev/null "$url"; then
      echo "Success on attempt $i for URL: $url."
      return 0
    fi
    sleep "$wait_time"
  done

  echo "Max attempts ($max_attempts) reached for URL: $url. Exiting."
  return 1
}

./prepare.sh

mkdir -p $LOG_DIR

# start a server for the connect_facilitator to hit once it's ready
python3 -c "import http.server; http.server.HTTPServer((\"localhost\", 7000), lambda *args: (print('Vali reported connecting to faci'), exit(0))).serve_forever()" & server_pid=$!;


if ! kill -0 "$server_pid" 2>/dev/null; then
    echo "The connect_facilitator webhook server died, you should see some logs that should explain why"
    exit 1
fi


./tabs/0.facilitator.sh > $LOG_DIR/0.facilitator.log 2>&1 & echo $! > $LOG_DIR/0.facilitator.pid
./tabs/1.miner.sh > $LOG_DIR/1.miner.log 2>&1 & echo $! > $LOG_DIR/1.miner.pid
DEBUG_CONNECT_FACILITATOR_WEBHOOK=http://localhost:7000 ./tabs/2.validator-connect.sh > $LOG_DIR/2.validator-connect.log 2>&1 & echo $! > $LOG_DIR/2.validator-connect.pid
./tabs/3.validator-celery.sh > $LOG_DIR/3.validator-celery.log 2>&1 & echo $! > $LOG_DIR/3.validator-celery.pid

check_endpoint "http://127.0.0.1:8000" 10 2
check_endpoint "http://127.0.0.1:9000" 10 2

timeout 15s bash -c "while kill -0 $server_pid 2>/dev/null; do sleep 1; done"




