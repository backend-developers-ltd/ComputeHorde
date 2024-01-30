# Miner runner

Runner is a helper container that launches all the necessary services for a miner to run.

## Usage

Ensure docker is installed on your instance:

```bash
apt-get install -y docker.io
```

Put your miner configuration into `.env` file (see [.env.template](.env.template) for reference), and run:

```bash
docker run \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$HOME/.bittensor/wallets:/root/.bittensor/wallets" \
    --env-file .env \
    --name computehorde-miner-runner \
    --restart unless-stopped \
    --label=com.centurylinklabs.watchtower.enable=true \
    backenddevelopersltd/compute-horde-miner-runner:v0-latest
```

or, if the container already exists:

```bash
docker start computehorde-miner-runner
```

## How it works

The `computehorde/miner-runner` docker image contains a `docker-compose.yml` file with all the necessary services to run a miner. It also contains a `watchtower` container that will automatically apply updates for containers.

```
computehorde/miner-runner
|__postgres
|__redis
|__app
|__worker
|__nginx
|__...
|__watchtower
```

The `watchtower` container may update:
1) core services in `docker-compose.yml` (like `app` or `worker`), and
2) `computehorde/miner-runner` container itself, which will automatically update ALL the other containers.

It is expected that only core services will be updated from time to time, but if infrastructure update is required, it will be done by auto-updating `computehorde/miner-runner` container.
