#!/bin/bash
set -euxo pipefail

if [ $# -lt 3 ] || [ $# -gt 5 ];
then
  >&2 echo "USAGE: ./install_miner.sh MODE[production|local] SSH_DESTINATION HOTKEY_PATH|VALIDATOR_PUBLIC_KEY MINER_PORT:8000(optional) DEFAULT_EXECUTOR_CLASS(optional, local mode only)"
  exit 1
fi

MODE="$1"
SSH_DESTINATION="$2"

MINER_PORT="${4:-8000}"

if [ "$MODE" == "production" ]; then
  LOCAL_HOTKEY_PATH=$(realpath "$3")
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

  REMOTE_HOTKEY_NAME="$(basename "$REMOTE_HOTKEY_PATH")"
  REMOTE_WALLET_NAME="$(basename "$(dirname "$REMOTE_HOTKEY_DIR")")"

  VALIDATOR_PUBLIC_KEY=""
  DEFAULT_EXECUTOR_CLASS=

elif [ "$MODE" == "local" ]; then
  VALIDATOR_PUBLIC_KEY="$3"

  REMOTE_HOTKEY_DIR=.bittensor/wallets/dummy/hotkeys
  REMOTE_HOTKEY_NAME=dummy
  REMOTE_WALLET_NAME=dummy

  DEFAULT_EXECUTOR_CLASS="${5:-}"
else
  >&2 echo "Invalid mode $MODE. Must be 'production' or 'local'"
  exit 1
fi

DEFAULT_ADMIN_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(25))')
: "${MIGRATING:=0}"

# Create tmpvars file on the server
ssh "$SSH_DESTINATION" <<ENDSSH
set -euxo pipefail

mkdir -p $REMOTE_HOTKEY_DIR
cat > tmpvars <<ENDCAT
HOTKEY_NAME=$REMOTE_HOTKEY_NAME
WALLET_NAME=$REMOTE_WALLET_NAME
DEFAULT_ADMIN_PASSWORD="$DEFAULT_ADMIN_PASSWORD"
MIGRATING=$MIGRATING
MINER_PORT=$MINER_PORT
VALIDATOR_PUBLIC_KEY=$VALIDATOR_PUBLIC_KEY
DEFAULT_EXECUTOR_CLASS=$DEFAULT_EXECUTOR_CLASS
ENDCAT
ENDSSH

if [ "$MODE" == "production" ]; then
  # Copy the wallet files to the server
  # shellcheck disable=SC2087
  scp "$LOCAL_HOTKEY_PATH" "$SSH_DESTINATION:$REMOTE_HOTKEY_PATH"
  scp "$LOCAL_COLDKEY_PUB_PATH" "$SSH_DESTINATION:$REMOTE_COLDKEY_PUB_PATH"
fi

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

# install cuda
sudo apt-get install -y linux-headers-$(uname -r)
DISTRIBUTION=$(. /etc/os-release; echo $ID$VERSION_ID | sed -e 's/\.//g')
wget "https://developer.download.nvidia.com/compute/cuda/repos/$DISTRIBUTION/x86_64/cuda-keyring_1.1-1_all.deb"
sudo dpkg -i cuda-keyring_1.1-1_all.deb
sudo apt-get update
sudo apt-get -y install cuda-drivers

curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
  | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
  && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
  | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
  | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit

sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl stop docker
sudo systemctl start docker
ENDSSH

# start a new ssh connection so that usermod changes are effective
ssh "$SSH_DESTINATION" <<'ENDSSH'
set -euxo pipefail
mkdir -p ~/compute_horde_miner
cd ~/compute_horde_miner

cat > docker-compose.yml <<'ENDDOCKERCOMPOSE'
version: '3.7'

services:

  miner-runner:
    image: backenddevelopersltd/compute-horde-miner-runner:v0-latest
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - "$HOME/.bittensor/wallets:/root/.bittensor/wallets"
      - ./.env:/root/.env
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

# leave it as "finney" if you want to use the public mainnet chain
BITTENSOR_NETWORK=finney

BITTENSOR_WALLET_NAME="$(. ~/tmpvars && echo "$WALLET_NAME")"
BITTENSOR_WALLET_HOTKEY_NAME="$(. ~/tmpvars && echo "$HOTKEY_NAME")"

HOST_WALLET_DIR=$HOME/.bittensor/wallets

# for now, PORT_FOR_EXECUTORS has to be the same as BITTENSOR_MINER_PORT, unless you change nginx configuration yourself (we don't advise doing that)
BITTENSOR_MINER_PORT=$(. ~/tmpvars && echo "$MINER_PORT")

BITTENSOR_MINER_ADDRESS=auto
COMPOSE_PROJECT_NAME=compute_horde_miner

DEFAULT_EXECUTOR_CLASS=$(. ~/tmpvars && echo "$DEFAULT_EXECUTOR_CLASS")

# make sure to unblock access to that port in your firewall
PORT_FOR_EXECUTORS=$(. ~/tmpvars && echo "$MINER_PORT")

ADDRESS_FOR_EXECUTORS=172.17.0.1
DEFAULT_ADMIN_PASSWORD="$(. ~/tmpvars && echo "$DEFAULT_ADMIN_PASSWORD")"
MIGRATING="$(. ~/tmpvars && echo "$MIGRATING")"
ENDENV
ENDSSH

if [ "$MODE" == "local" ]; then
ssh "$SSH_DESTINATION" <<'ENDSSH'
set -euxo pipefail
cat >> ~/compute_horde_miner/.env <<ENDENV
IS_LOCAL_MINER=1
LOCAL_MINER_VALIDATOR_PUBLIC_KEY="$(. ~/tmpvars && echo "$VALIDATOR_PUBLIC_KEY")"
ENDENV
ENDSSH
fi

ssh "$SSH_DESTINATION" <<ENDSSH
set -euxo pipefail

cd ~/compute_horde_miner

# Start runner and watchtower containers
docker compose up -d

rm ~/tmpvars
ENDSSH

set +x
MINER_HOSTNAME=$(ssh -G "$SSH_DESTINATION" | grep '^hostname' | cut -d' ' -f2)
MINER_ADMIN_LOGIN_URL="http://$MINER_HOSTNAME:$MINER_PORT/admin/login/"

for run in {1..10}
do
  echo "Checking miner status..."

  STATUS_CODE=$(curl --silent --max-time 2 --output /dev/null --write-out "%{http_code}" "$MINER_ADMIN_LOGIN_URL" || true)
  if [[ $STATUS_CODE -eq 200 ]]
  then
    cat <<'EOF'
  ____                            _         _       _   _                 _
 / ___|___  _ __   __ _ _ __ __ _| |_ _   _| | __ _| |_(_) ___  _ __  ___| |
| |   / _ \| '_ \ / _` | '__/ _` | __| | | | |/ _` | __| |/ _ \| '_ \/ __| |
| |__| (_) | | | | (_| | | | (_| | |_| |_| | | (_| | |_| | (_) | | | \__ \_|
 \____\___/|_| |_|\__, |_|  \__,_|\__|\__,_|_|\__,_|\__|_|\___/|_| |_|___(_)
                  |___/

Miner installed successfully

EOF

  cat <<EOF
You can log into your miner admin panel with the following info:
    URL: $MINER_ADMIN_LOGIN_URL
    Username: admin
    Password: $DEFAULT_ADMIN_PASSWORD

EOF

  cat <<'EOF'
To change your username/password:
  1. Select "Users" from the left sidebar
  2. Select "admin"
  3. Change username and/or password in this form and save
EOF

    exit 0
  fi

  sleep "$run"
done

>&2 echo "Cannot connect to miner. Please check if everything is installed and running properly."
>&2 echo "Also make sure port $MINER_PORT is reachable from outside (i.e. not blocked in firewall)."
exit 1
