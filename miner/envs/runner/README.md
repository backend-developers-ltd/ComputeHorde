# Miner runner

Runner is a helper container that launches all the necessary services for a miner to run.

## Usage

Ensure docker is installed on your instance:

```bash
apt-get install -y docker.io docker-compoe
```

Put your miner configuration into `.env` file (see [.env.template](.env.template) for reference)

Copy this to `docker-compose.yml`:

```
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

```

## How it works

The `backenddevelopersltd/compute-horde-miner-runner` docker image contains a `docker-compose.yml` file with all the necessary services to run a miner. A `watchtower` container will automatically apply updates for containers.

```
backenddevelopersltd/compute-horde-miner-runner
|__postgres
|__redis
|__app
|__worker
|__nginx
|__...
```

The `watchtower` container may update:
1) core services in `docker-compose.yml` (like `app` or `worker`), and
2) `backenddevelopersltd/compute-horde-miner-runner` container itself, which will automatically update ALL the other containers.
