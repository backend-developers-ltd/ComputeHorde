# Miner runner

Runner is a helper container that launches all the necessary services for a miner to run.

## Usage

Only tested on Ubuntu. Running requires installing nvidia drivers (this will change in a near release).

Ensure docker is installed on your instance:

```bash
apt-get install -y docker.io docker-compose
```

Install nvidia drivers. For example, on ubuntu:

1. follow https://docs.nvidia.com/datacenter/tesla/tesla-installation-notes/index.html#ubuntu-lts
2. follow https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html#installation
3. follow https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html#configuring-docker

To test that all the drivers are working properly, run the following command:

```
docker run --runtime=nvidia --gpus all backenddevelopersltd/compute-horde-job:v0-latest --runtime 600 --restore-disable --attack-mode 3 --workload-profile 3 --optimized-kernel-enable --hash-type 1410 --hex-salt -1 ?l?d?u --outfile-format 2 --quiet 5726c17704f709432e2c7d816b8b3fc3236263c4cf7383267bf13bea22e91a85:55507f1971ff79d5 ?1?1?1?1?1?1
```


If everything works fine, you should see `mQNJTt` and nothing else in stdout (some warnings in stderr) 
are negligible.

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

With the `.env` and `docker-compose.yml` already in place, you can start the miner with `docker-compose up -d`.
Test if it is running properly with `docker-compose ps`.

## Admin panel

Your miner exposes an admin panel that allows you to check its health. To access it, use the same address and port as 
the ones you expose in your axon. For example, type `http://198.66.65.12.13:8000` in your browser. To log in you need 
to set up an admin password, either in .env or manually.

## Creating an admin user manually (not using DEFAULT_ADMIN_PASSWORD in .env)

Run `docker-compose exec miner-runner docker-compose exec app python manage.py createsuperuser` and follow 
the instructions.

## How it works

The `backenddevelopersltd/compute-horde-miner-runner` docker image contains a `docker-compose.yml` file with all the necessary services to run a miner.
A `watchtower` container will automatically apply updates for containers.

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
