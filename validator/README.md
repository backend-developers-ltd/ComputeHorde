# validator

ComputeHorde Validator. See repositories main README for explanation of what that means.

- - -

Skeleton of this project was generated with `cookiecutter-rt-django`, which sometimes gets upgrades that are easy to retrofit into already older projects.

# Base requirements

- docker
- docker-compose
- python 3.11
- [uv](https://docs.astral.sh/uv/)
- [nox](https://nox.thea.codes)

# Setup development environment

```sh
# 1st tab
$ ./setup-dev.sh
```

```sh
# 2nd tab
docker-compose up
```

```sh
# 1st tab
cd app/src
uv run manage.py wait_for_database --timeout 10
uv run manage.py migrate
```

Typically, when developing validator, you want to test:

1. Settings weights
2. Passing organic jobs from a facilitator to a miner

Some of these actions may require launching celery locally, which you can achieve by running [start_celery.sh](dev_env_setup%2Fstart_celery.sh).

Some tests start celery in order to test obscure interprocess timeouts. That is generally automated, however celery 
workers being killed on MacOS yield an insufferable amount of error dialogs displayed on top of everything you have 
open. To save developers from suffering this fate, tests support running celery on a remote host, via ssh. A common case
would be a local VM, also running postgres and redis. In such a case, make sure postgres and redis ports are forwarded
so they are accessible from the host, which runs your code/tests. To make tests start celery on the remote host, define
the following env vars (the author of this note suggests using direnv or PyCharm Run/Debug configuration templates for
ease):

```shell
REMOTE_HOST=ubuntu-dev  # ssh config entry name
REMOTE_VENV=/path/to/remote/venv  # on the remote
REMOTE_CELERY_START_SCRIPT=/project/mount/ComputeHorde/validator/dev_env_setup/start_celery.sh  #  adjust the beginning
# to match your synced folders configuration
```

# Setting up a trusted miner for cross-validation

## Set up a server

Create an Ubuntu server and use the `install_miner.sh` script from the root of this repository to install the miner in a **local mode**:

```sh
curl -sSfL https://github.com/backend-developers-ltd/ComputeHorde/raw/master/install_miner.sh | bash -s - local SSH_DESTINATION VALIDATOR_PUBLIC_KEY MINER_PORT DEFAULT_EXECUTOR_CLASS
```

Replace the placeholders in the command above:
- `SSH_DESTINATION`: your server's connection info (i.e. `username@1.2.3.4`)
- `VALIDATOR_PUBLIC_KEY`: the hotkey of your validator (_not_ the path to the key)
- `MINER_PORT` (optional): the port (of your choosing) on which the miner will listen for incoming connections (default is 8000)
- `DEFAULT_EXECUTOR_CLASS` (optional): specifies a custom executor class to use.

## Updated validator .env

Add or update these variables in the validator `.env` file:

```
TRUSTED_MINER_ADDRESS="MINER_IP"
TRUSTED_MINER_PORT=MINER_PORT
```


# Using the Collateral Smart Contract (Optional)

Validators can optionally deploy a **collateral smart contract** to filter miners based on **trust expressed through deposited funds**.

Once deployed:

- Your validator will **automatically begin using the contract** to prefer miners who have deposited collateral.
- This enables a **slashing mechanism** for incorrect organic job results, increasing the reliability of ComputeHorde’s compute pool.

To set it up, follow the 
[deployment instructions in the Collateral Contract repository](https://github.com/bactensor/collateral-contracts#recommended-validator-integration-guide-as-used-by-computehorde). 
Once deployed, no additional configuration is needed — the validator detects and uses the contract automatically.

> **Note:** This feature is optional but recommended if your validator sends organic jobs to untrusted miners.
