#!/bin/bash -x
set -e

update_env_var() {
  set +x
  local env_name="$1"
  local desired_value="$2"
  local file_name="$3"

  if [[ -z "$env_name" || -z "$desired_value" || -z "$file_name" ]]; then
    echo "Usage: update_env_var <ENV_NAME> <DESIRED_VALUE> <FILE_NAME>"
    return 1
  fi

  # Check if the file is a symlink
  if [[ -L "$file_name" ]]; then
    # Resolve the symlink to get the actual target file
    file_name="$(readlink "$file_name")"
  fi

  if grep -q "^${env_name}=" "$file_name"; then
      # Check if the desired value already exists
      if ! grep -q "^${env_name}=${desired_value}$" "$file_name"; then
          # Use temporary files to comment out the old line and append the desired one
          local tmp_file="${file_name}.tmp"
          awk -v env_name="${env_name}" -v desired_value="${desired_value}" '
              $0 ~ "^"env_name"=" {
                  if ($0 != env_name"="desired_value) {
                      print "#" $0;  # Comment out the old line
                      print env_name"="desired_value;  # Append the desired value right after
                  } else {
                      print $0  # Keep as is if already correct
                  }
              }
              $0 !~ "^"env_name"=" { print $0 }  # Print other lines as is
          ' "$file_name" > "$tmp_file" && mv "$tmp_file" "$file_name"
      fi
  else
      # Append the correct value if it doesn't exist
      echo "${env_name}=${desired_value}" >> "$file_name"
  fi
  set -x
}

# Check if 'docker-compose' is available
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_COMMAND="docker-compose"
# Otherwise, check for 'docker compose'
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE_COMMAND="docker compose"
else
    echo "Error: Neither 'docker-compose' nor 'docker compose' is available on this system."
    exit 1
fi


PROJECT_ROOT=$(git rev-parse --show-toplevel)

# setup executor

cd $PROJECT_ROOT/executor
./setup-dev.sh
update_env_var "DEBUG_NO_GPU_MODE" "true" ".env"

# Uncomment the line below only if you have a volume manager service running
update_env_var "VOLUME_MANAGER_ADDRESS" "http://localhost:8001" ".env"

# setup miner
cd $PROJECT_ROOT/miner
./setup-dev.sh
update_env_var "BITTENSOR_WALLET_NAME" "miner" ".env"
update_env_var "BITTENSOR_WALLET_HOTKEY_NAME" "default" ".env"
# the line below ensures no subtensor will be reached, so if any piece of code requires it it will fail. This is
# not a hard requirement for this local stack setup, if it becomes too bothersome to maintain we can drop that
update_env_var "BITTENSOR_NETWORK" "non-existent" ".env"
update_env_var "BITTENSOR_WALLET_DIRECTORY" "$PROJECT_ROOT/local_stack/wallets" ".env"
update_env_var "EXECUTOR_MANAGER_CLASS_PATH" "compute_horde_miner.miner.executor_manager.v1:DevExecutorManager" ".env"
update_env_var "DEFAULT_EXECUTOR_CLASS" "always_on.llm.a6000" ".env"

$DOCKER_COMPOSE_COMMAND up -d

timeout 30s uv run python app/src/manage.py wait_for_database
uv run python app/src/manage.py migrate
uv run python app/src/manage.py debug_add_validator 5CDYBwxDZwXnYSp7E39Vy92MVbCMtcK2i953oRxDm9Veko7M

# setup validator

cd $PROJECT_ROOT/validator

./setup-dev.sh
update_env_var "BITTENSOR_WALLET_NAME" "validator" ".env"
update_env_var "BITTENSOR_WALLET_HOTKEY_NAME" "default" ".env"
# the line below ensures no subtensor will be reached, so if any piece of code requires it it will fail. This is
# not a hard requirement for this local stack setup, if it becomes too bothersome to maintain we can drop that
update_env_var "BITTENSOR_NETWORK" "non-existent" ".env"
update_env_var "BITTENSOR_WALLET_DIRECTORY" "$PROJECT_ROOT/local_stack/wallets" ".env"

update_env_var "DEBUG_MINER_KEY" "5FYwNgYu5Wj7Vv49KtC3gXa1W1dn2BnMBGNKGrLy8pX9i4T7" ".env"
update_env_var "DEBUG_MINER_ADDRESS" "127.0.0.1" ".env"
update_env_var "DEBUG_MINER_PORT" "8000" ".env"
update_env_var "DEBUG_USE_MOCK_BLOCK_NUMBER" "true" ".env"
update_env_var "FACILITATOR_URI" "ws://localhost:9000/ws/v0/" ".env"

$DOCKER_COMPOSE_COMMAND up -d

timeout 30s uv run python app/src/manage.py wait_for_database
uv run python app/src/manage.py migrate

# setup facilitator

cd $PROJECT_ROOT/facilitator

./setup-dev.sh

# the line below ensures no subtensor will be reached, so if any piece of code requires it it will fail. This is
# not a hard requirement for this local stack setup, if it becomes too bothersome to maintain we can drop that
update_env_var "BITTENSOR_NETWORK" "non-existent" ".env"

$DOCKER_COMPOSE_COMMAND up -d

timeout 30s uv run python app/src/manage.py wait_for_database
uv run python app/src/manage.py migrate
uv run python app/src/manage.py debug_add_validator 5CDYBwxDZwXnYSp7E39Vy92MVbCMtcK2i953oRxDm9Veko7M
uv run python app/src/manage.py whitelist_hotkey 5CDYBwxDZwXnYSp7E39Vy92MVbCMtcK2i953oRxDm9Veko7M
