#!/bin/bash
set -euxo pipefail

if [ $# -lt 1 ] || [ $# -gt 1 ];
then
  >&2 echo "USAGE: ./install_executor.sh SSH_DESTINATION"
  exit 1
fi

SSH_DESTINATION="$1"
SSH_OPTS="-o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2"

# install necessary software in the server
ssh $SSH_OPTS "$SSH_DESTINATION" <<'ENDSSH'
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
APT_OPTS="-y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confnew"

# add miner SSH key
if ! grep -Fq 'miner@executor' ~/.ssh/authorized_keys 2>/dev/null; then
  echo "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINUPpB7FW7pLpmJSOUy/5UJ+04YT6OIndh28TPhTttTT miner@executor" >> ~/.ssh/authorized_keys
fi

# don't upgrade openssh-server yet, as it may disconnect us
sudo apt-mark hold openssh-server

# upgrade all packages
sudo apt-get update
sudo apt-get upgrade $APT_OPTS

# install docker
sudo apt-get install $APT_OPTS ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

sudo apt-get install $APT_OPTS docker-ce docker-compose-plugin
sudo usermod -aG docker $USER

# install cuda
sudo apt-get install $APT_OPTS linux-headers-$(uname -r)
DISTRIBUTION=$(. /etc/os-release; echo $ID$VERSION_ID | sed -e 's/\.//g')
wget -O cuda-keyring.deb "https://developer.download.nvidia.com/compute/cuda/repos/$DISTRIBUTION/x86_64/cuda-keyring_1.1-1_all.deb"
sudo dpkg -i cuda-keyring.deb
rm cuda-keyring.deb

sudo apt-get update
sudo apt-get install $APT_OPTS cuda-drivers

curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
  | sudo gpg --batch --yes --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
  && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
  | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
  | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update
sudo apt-get install $APT_OPTS nvidia-container-toolkit

sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl stop docker
sudo systemctl start docker

# upgrade openssh-server now that we're done
sudo apt-mark unhold openssh-server
sudo apt-get upgrade $APT_OPTS openssh-server
ENDSSH

# setup docuum-runner and preloaded images
ssh $SSH_OPTS "$SSH_DESTINATION" <<'ENDSSH'
set -euxo pipefail

sudo apt-get update
sudo apt-get install -y jq

mkdir -p ~/scripts
touch $HOME/scripts/images.txt
cat > ~/scripts/preload-job-images.sh <<'EOF'
#!/usr/bin/env bash
# Pull all Docker images defined in miner-config-prod.json.

set -euo pipefail

# truncate images.txt
true > $HOME/scripts/images.txt

# RAW_URL="https://raw.githubusercontent.com/backend-developers-ltd/compute-horde-dynamic-config/master/miner-config-prod.json"
# curl -fsSL "$RAW_URL" \
# | jq -r '.DYNAMIC_PRELOAD_DOCKER_JOB_IMAGES.items[0].value[]' \
# | tee $HOME/scripts/images.txt \
# | xargs -r -n1 docker pull

docker pull backenddevelopersltd/compute-horde-executor:v1-latest
echo backenddevelopersltd/compute-horde-executor:v1-latest >> $HOME/scripts/images.txt

docker pull backenddevelopersltd/compute-horde-executor-staging:v1-latest
echo backenddevelopersltd/compute-horde-executor-staging:v1-latest >> $HOME/scripts/images.txt

docker pull backenddevelopersltd/compute-horde-executor-preprod:v1-latest
echo backenddevelopersltd/compute-horde-executor-preprod:v1-latest >> $HOME/scripts/images.txt
EOF

chmod +x ~/scripts/preload-job-images.sh
~/scripts/preload-job-images.sh

mkdir -p ~/docuum-runner
cat > ~/docuum-runner/compose.yml <<EOF
services:
  docuum-runner:
    image: ghcr.io/backend-developers-ltd/docuum-runner:latest
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ${HOME}/scripts/images.txt:/config/critical-images.txt:ro
    environment:
      - THRESHOLD=60%
      - ${HOME}/scripts/images.txt=/config/critical-images.txt
EOF

cd ~/docuum-runner
docker compose down
docker compose up -d
ENDSSH

# setup cron jobs
ssh $SSH_OPTS "$SSH_DESTINATION" <<'ENDSSH'
set -euxo pipefail

CRON_FILE=$(mktemp)
grep -Fq 'docker system prune' "$CRON_FILE" || printf '%s\n' "0 * * * * docker system prune --force" >> "$CRON_FILE"
grep -Fq 'preload-job-images' "$CRON_FILE" || printf '%s\n' "0 * * * * $HOME/scripts/preload-job-images.sh >> $HOME/scripts/preload-job-images.log 2>&1" >> "$CRON_FILE"
grep -Fq 'find /tmp' "$CRON_FILE" || printf '%s\n' "*/5 * * * * sudo find /tmp -maxdepth 1 -mindepth 1 -type d -name 'tmp*' -mmin +60 -exec rm -rf -- {} +" >> "$CRON_FILE"
crontab "$CRON_FILE"
rm "$CRON_FILE"
ENDSSH

# reboot
ssh $SSH_OPTS "$SSH_DESTINATION" "sudo reboot"

echo "Waiting for server to come back up..."
sleep 10
until ssh $SSH_OPTS -q "$SSH_DESTINATION" exit >/dev/null 2>&1; do
  echo -n "."
  sleep 1
done

echo -e "\nServer is back up and running!"
