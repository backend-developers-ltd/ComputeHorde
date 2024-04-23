#!/bin/bash
set -euxo pipefail

if [ $# -ne 2 ]
then
  >&2 echo "USAGE: ./install_miner.sh SSH_DESTINATION HOTKEY_PATH"
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

# Copy the wallet files to the server
# shellcheck disable=SC2087
ssh "$SSH_DESTINATION" <<ENDSSH
set -euxo pipefail

mkdir -p $REMOTE_HOTKEY_DIR
cat > tmpvars <<ENDCAT
HOTKEY_NAME="$(basename "$REMOTE_HOTKEY_PATH")"
WALLET_NAME="$(basename "$(dirname "$REMOTE_HOTKEY_DIR")")"
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

# install cuda
sudo apt-get install -y linux-headers-$(uname -r)
DISTRIBUTION=$(. /etc/os-release; echo $ID$VERSION_ID | sed -e 's/\.//g')
wget "https://developer.download.nvidia.com/compute/cuda/repos/$DISTRIBUTION/x86_64/cuda-keyring_1.0-1_all.deb"
sudo dpkg -i cuda-keyring_1.0-1_all.deb
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
sudo systemctl stop docke
sudo systemctl start docker
ENDSSH

# start a new ssh connection so that usermod changes are effective
ssh "$SSH_DESTINATION" <<'ENDSSH'
set -euxo pipefail
mkdir ~/compute_horde_miner
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
    command: --interval 60 --cleanup --label-enable
ENDDOCKERCOMPOSE

cat > .env <<ENDENV
SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_urlsafe(25))')
POSTGRES_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(16))')
BITTENSOR_NETUID=12
BITTENSOR_NETWORK=finney
BITTENSOR_WALLET_NAME="$(. ~/tmpvars && echo "$WALLET_NAME")"
BITTENSOR_WALLET_HOTKEY_NAME="$(. ~/tmpvars && echo "$HOTKEY_NAME")"
HOST_WALLET_DIR=$HOME/.bittensor/wallets
BITTENSOR_MINER_PORT=8000
BITTENSOR_MINER_ADDRESS=auto
COMPOSE_PROJECT_NAME=compute_horde_miner
PORT_FOR_EXECUTORS=8000
ADDRESS_FOR_EXECUTORS=172.17.0.1
ENDENV

docker pull backenddevelopersltd/compute-horde-executor:v0-latest
docker pull backenddevelopersltd/compute-horde-miner:v0-latest
docker pull backenddevelopersltd/compute-horde-job:v0-latest
docker compose up -d

ENDSSH
