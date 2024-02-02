# Validator runner

Runner is a helper container that launches all the necessary services for a validator to run.

## Usage

Ensure docker is installed on your instance:

```bash
apt-get install -y docker.io docker-compose
```

Put your validator configuration into `.env` file (see [.env.template](.env.template) for reference).

Copy this to `docker-compose.yml`:

```
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
    command: --interval 60 --cleanup --label-enable

```

and execute

```bash
docker-compose up -d
```

## How it works

The `backenddevelopersltd/compute-horde-validator-runner` docker image contains a `docker-compose.yml` file with all the necessary services to run a validator. 
A `watchtower` container that will automatically apply updates for containers.

```
backenddevelopersltd/compute-horde-validator-runner
|__postgres
|__redis
|__worker
|__...
|__watchtower
```

The `watchtower` container may update:
1) core services in `docker-compose.yml` (like `worker`), and
2) `backenddevelopersltd/compute-horde-validator-runner` container itself, which will automatically update ALL the other containers.
