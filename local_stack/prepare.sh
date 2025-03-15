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

  # Update the value if the variable is found, or append it if not
  if grep -q "^${env_name}=" "$file_name"; then
    sed -i '' "s|^${env_name}=.*|${env_name}=${desired_value}|" "$file_name"
  else
    echo "${env_name}=${desired_value}" >> "$file_name"
  fi
  set -x
}

PROJECT_ROOT=$(git rev-parse --show-toplevel)

# setup executor

cd $PROJECT_ROOT/executor
./setup-dev.sh
update_env_var "DEBUG_NO_GPU_MODE" "true" ".env"

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

docker-compose up -d

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

docker-compose up -d

timeout 30s uv run python app/src/manage.py wait_for_database
uv run python app/src/manage.py migrate

# setup facilitator

cd $PROJECT_ROOT/facilitator

./setup-dev.sh

# the line below ensures no subtensor will be reached, so if any piece of code requires it it will fail. This is
# not a hard requirement for this local stack setup, if it becomes too bothersome to maintain we can drop that
update_env_var "BITTENSOR_NETWORK" "non-existent" ".env"
update_env_var "R2_BUCKET_NAME" "''" ".env"

docker-compose up -d

timeout 30s uv run python app/src/manage.py wait_for_database
uv run python app/src/manage.py migrate
uv run python app/src/manage.py debug_add_validator 5CDYBwxDZwXnYSp7E39Vy92MVbCMtcK2i953oRxDm9Veko7M

