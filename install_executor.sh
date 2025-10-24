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

echo "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINUPpB7FW7pLpmJSOUy/5UJ+04YT6OIndh28TPhTttTT miner@executor" >> ~/.ssh/authorized_keys

# install docker
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc
do
  (yes | sudo apt-get remove $pkg) || true
done

sudo apt-get update
sudo apt-mark hold openssh-server
sudo apt-get upgrade $APT_OPTS
sudo apt-mark unhold openssh-server
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
sudo apt-get update
sudo apt-get install $APT_OPTS cuda-drivers

curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
  | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
  && curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
  | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
  | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update
sudo apt-get install $APT_OPTS nvidia-container-toolkit

sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl stop docker
sudo systemctl start docker
ENDSSH

# setup docuum-runner
ssh $SSH_OPTS "$SSH_DESTINATION" <<'ENDSSH'
set -euxo pipefail

CRITICAL_IMAGES_PATH="$HOME/critical-images.txt"

mkdir -p ~/docuum-runner
touch "$CRITICAL_IMAGES_PATH"
cat > ~/docuum-runner/compose.yml <<EOF
services:
  docuum-runner:
    image: ghcr.io/backend-developers-ltd/docuum-runner:latest
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - $CRITICAL_IMAGES_PATH:/config/critical-images.txt:ro
    environment:
      - THRESHOLD=60%
      - CRITICAL_IMAGES_PATH=/config/critical-images.txt
EOF

cd ~/docuum-runner
docker compose up -d
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
