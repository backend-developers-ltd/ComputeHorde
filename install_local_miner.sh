#!/bin/bash
set -euxo pipefail

if [ $# -ne 2 ]
then
  >&2 echo "USAGE: ./install_miner.sh SSH_DESTINATION VALIDATOR_PUBLIC_KEY"
  exit 1
fi

SSH_DESTINATION="$1"
VALIDATOR_PUBLIC_KEY="$2"

DEFAULT_ADMIN_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(25))')
: "${MIGRATING:=0}"
MINER_PORT=9898

# shellcheck disable=SC2087
ssh "$SSH_DESTINATION" <<ENDSSH
set -euxo pipefail

cat > tmpvars <<ENDCAT
DEFAULT_ADMIN_PASSWORD="$DEFAULT_ADMIN_PASSWORD"
MIGRATING=$MIGRATING
VALIDATOR_PUBLIC_KEY="$VALIDATOR_PUBLIC_KEY"
MINER_PORT=$MINER_PORT
ENDCAT
ENDSSH

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
sudo systemctl stop docker
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

IS_LOCAL_MINER=1
LOCAL_MINER_VALIDATOR_PUBLIC_KEY="$(. ~/tmpvars && echo "$VALIDATOR_PUBLIC_KEY")"

POSTGRES_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(16))')
BITTENSOR_NETUID=12

# leave it as "finney" if you want to use the public mainnet chain
BITTENSOR_NETWORK=finney

BITTENSOR_WALLET_NAME=dummy
BITTENSOR_WALLET_HOTKEY_NAME=dummy
HOST_WALLET_DIR=$HOME/.bittensor/wallets

# for now, PORT_FOR_EXECUTORS has to be the same as BITTENSOR_MINER_PORT, unless you change nginx configuration yourself (we don't advise doing that)
BITTENSOR_MINER_PORT=$(. ~/tmpvars && echo "$MINER_PORT")

BITTENSOR_MINER_ADDRESS=auto
COMPOSE_PROJECT_NAME=compute_horde_miner

# make sure to unblock access to that port in your firewall
PORT_FOR_EXECUTORS=$(. ~/tmpvars && echo "$MINER_PORT")

ADDRESS_FOR_EXECUTORS=172.17.0.1
DEFAULT_ADMIN_PASSWORD="$(. ~/tmpvars && echo "$DEFAULT_ADMIN_PASSWORD")"
MIGRATING="$(. ~/tmpvars && echo "$MIGRATING")"
ENDENV

rm ~/tmpvars

docker pull backenddevelopersltd/compute-horde-executor:v0-latest
docker pull backenddevelopersltd/compute-horde-miner:v0-latest
docker pull backenddevelopersltd/compute-horde-job:v0-latest
docker compose up -d

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
>&2 echo "Also make sure port ${MINER_PORT} is reachable from outside (i.e. not blocked in firewall)."
exit 1
