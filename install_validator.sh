#!/bin/bash
set -euxo pipefail

if [ $# -ne 2 ]
then
  >&2 echo "USAGE: ./install_validator.sh SSH_DESTINATION HOTKEY_PATH"
  exit 1
fi

SSH_DESTINATION="$1"
LOCAL_HOTKEY_PATH=$(realpath "$2")
LOCAL_COLDKEY_PUB_PATH=$(dirname "$(dirname "$LOCAL_HOTKEY_PATH")")/coldkeypub.txt

if [ ! -f "$LOCAL_HOTKEY_PATH" ]; then
  >&2 echo "Given HOTKEY_PATH does not exist"
  exit 1
fi

HOTKEY_NAME=$(basename "$LOCAL_HOTKEY_PATH")
WALLET_NAME=$(basename "$(dirname "$(dirname "$LOCAL_HOTKEY_PATH")")")

# set default names if they contain special characters to avoid inconsistent behaviors by `.env` readers
[[ $HOTKEY_NAME =~ ['$#!;*?&()<>'\"\'] ]] && HOTKEY_NAME=default
[[ $WALLET_NAME =~ ['$#!;*?&()<>'\"\'] ]] && WALLET_NAME=mywallet

REMOTE_HOTKEY_PATH=".bittensor/wallets/$WALLET_NAME/hotkeys/$HOTKEY_NAME"
REMOTE_COLDKEY_PUB_PATH=".bittensor/wallets/$WALLET_NAME/coldkeypub.txt"
REMOTE_HOTKEY_DIR=$(dirname "$REMOTE_HOTKEY_PATH")

: "${MIGRATING:=0}"

# Copy the wallet files to the server
# shellcheck disable=SC2087
ssh "$SSH_DESTINATION" <<ENDSSH
set -euxo pipefail

mkdir -p $REMOTE_HOTKEY_DIR
cat > tmpvars <<ENDCAT
HOTKEY_NAME="$(basename "$REMOTE_HOTKEY_PATH")"
WALLET_NAME="$(basename "$(dirname "$REMOTE_HOTKEY_DIR")")"
MIGRATING=$MIGRATING
ENDCAT
ENDSSH
scp "$LOCAL_HOTKEY_PATH" "$SSH_DESTINATION:$REMOTE_HOTKEY_PATH"
scp "$LOCAL_COLDKEY_PUB_PATH" "$SSH_DESTINATION:$REMOTE_COLDKEY_PUB_PATH"

# install necessary software in the server
ssh "$SSH_DESTINATION" <<'ENDSSH'
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive

# install docker
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc
do
  (yes | sudo apt-get remove $pkg) || true
done

sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

sudo apt-get install -y docker-ce docker-compose-plugin
sudo usermod -aG docker $USER
ENDSSH

# start a new ssh connection so that usermod changes are effective
ssh "$SSH_DESTINATION" <<'ENDSSH'
set -euxo pipefail
mkdir -p ~/compute_horde_validator
cd ~/compute_horde_validator

cat > docker-compose.yml <<'ENDDOCKERCOMPOSE'
version: '3.7'

services:

  validator-runner:
    image: backenddevelopersltd/compute-horde-validator-runner:v0-latest
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - "$HOME/.bittensor/wallets:/root/.bittensor/wallets"
      - ./.env:/root/validator/.env
    labels:
      - "com.centurylinklabs.watchtower.enable=true"

  watchtower:
    image: containrrr/watchtower:latest
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    command: --interval 60 --cleanup --label-enable --no-pull
ENDDOCKERCOMPOSE

# Pull images, verifying they are signed
export DOCKER_CONTENT_TRUST=1
docker compose convert --images | sort -u | xargs -n 1 docker pull

cat > .env <<ENDENV
SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_urlsafe(25))')
POSTGRES_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(16))')
BITTENSOR_NETUID=12
BITTENSOR_NETWORK=finney
BITTENSOR_WALLET_NAME="$(. ~/tmpvars && echo "$WALLET_NAME")"
BITTENSOR_WALLET_HOTKEY_NAME="$(. ~/tmpvars && echo "$HOTKEY_NAME")"
HOST_WALLET_DIR=$HOME/.bittensor/wallets
COMPOSE_PROJECT_NAME=compute_horde_validator
FACILITATOR_URI=wss://facilitator.computehorde.io/ws/v0/
MIGRATING="$(. ~/tmpvars && echo "$MIGRATING")"
ENDENV

# Start runner and watchtower containers
docker compose up -d

ENDSSH
