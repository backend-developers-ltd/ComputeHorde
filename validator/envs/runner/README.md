# Validator runner

Runner is a helper container that launches all the necessary services for a validator to run.

## Usage

Ensure docker is installed on your instance:

```bash
apt-get install -y docker.io
```

Put your validator configuration into `.env` file (see [.env.template](.env.template) for reference), and run:

```bash
docker run \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$HOME/.bittensor/wallets:/root/.bittensor/wallets" \
    --env-file .env \
    --name computehorde-validator-runner \
    --restart unless-stopped \
    --label=com.centurylinklabs.watchtower.enable=true \
    ghcr.io/backend-developers-ltd/computehorde/validator-runner:latest
```

or, if the container already exists:

```bash
docker start computehorde-validator-runner
```

## How it works

The `computehorde/validator-runner` docker image contains a `docker-compose.yml` file with all the necessary services to run a validator. It also contains a `watchtower` container that will automatically apply updates for containers.

```
computehorde/validator-runner
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
2) `computehorde/validator-runner` container itself, which will automatically update ALL the other containers.

It is expected that only core services will be updated from time to time, but if infrastructure update is required, it will be done by auto-updating `computehorde/validator-runner` container.
