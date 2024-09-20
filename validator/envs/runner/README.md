# Validator runner

Runner is a helper container that launches all the necessary services for a validator to run.

## Usage

1. Get a linux host, recommended:
   1. 8GB RAM
   2. 4x2.3GHz CPU
   3. 60GB SSD
   4. (No GPU is used by validators)
2. install `docker` and `docker-compose`, e.g. `apt-get install -y docker.io docker-compose`
   (has been tested on docker `24.0.5` and docker-compose `1.29.2`).
3. choose a directory on your host to save two text files, e.g `/home/josephus/compute_horde_validator`, contents
   coming in following points
4. save the following contents as `docker-compose.yml`, e.g. `/home/josephus/compute_horde_validator/docker-compose.yml`

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
    command: --interval 60 --cleanup --label-enable --no-pull

```

5. save the following contents as `.env`, e.g. `/home/josephus/compute_horde_validator/.env` (read the comments,
   they contain hints as to what values to give to these variables):

```
# generate one for yourself, e.g. `python3 -c 'import random; import string; print("".join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(50)))'`
SECRET_KEY=
# generate one for yourself, e.g. `python3 -c 'import random; import string; print("".join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(15)))'`
POSTGRES_PASSWORD=
# don't change POSTGRES_PASSWORD after you start your validator for the first time (but you have generate it before that)
BITTENSOR_NETUID=12
# network specification has the same syntax as `btcli --subtensor.network ... `
# so you can use "finney", "test", "186.12.13.150:9944" etc. but if you want to use a subtensor run on the same host,
# then due to docker networking, use the example provided below
BITTENSOR_NETWORK=172.17.0.1:9944
BITTENSOR_WALLET_NAME=validator
BITTENSOR_WALLET_HOTKEY_NAME=default
HOST_WALLET_DIR=/home/josephus/.bittensor/wallets
FACILITATOR_URI=wss://facilitator.computehorde.io/ws/v0/
```

Validator support admin panel that binds to 127.0.0.1:80. If you want to change default port add set it in `.env`, e.g.
```
HTTP_PORT=8080
```

6. run `docker-compose up -d` in the directory containing your `docker-compose.yml`, 
   e.g. `/home/josephus/compute_horde_validator/`
7. done. complete.

## Updating config

If you make any changes to `.env`, be it changing values, adding new ones or removing them, you need to stop all 
services and star them again:

```
docker-compose down
docker-compose up -d
```

you may see an error like:

```
ERROR: error while removing network: network ... has active endpoints
```
it is inconsequential, and we will remove it soon.

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
