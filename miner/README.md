# ComputeHorde Miner



- - -

Skeleton of this project was generated with `cookiecutter-rt-django`, which sometimes gets upgrades that are easy to retrofit into already older projects.

# Run production instance

## Runner

Most straightforward option is to use a "miner-runner" docker image which will launch everything automatically and will handle auto-updates. See [runner README](envs/runner/README.md) for more details.

## Docker-compose

Alternatively, you can use `docker-compose` to launch all the necessary services manually. See [docker-compose.yml](envs/runner/data/docker-compose.yml) for reference.

## Custom executor manager

Miners are encouraged to optimize their setup by implementing their own executor manager. To use your custom code, follow these steps:

1. Set the `HOST_VENDOR_DIR` variable in the `.env` file of the miner runner to the directory containing your custom code. This path will be added to the `PYTHONPATH` inside the miner container.

2. (Optional) Add a `setup.sh` file in the custom code directory to run additional commands when the miner container starts. For example, you can install dependencies using `pip install`.

3. To use your custom executor, set the `EXECUTOR_MANAGER_CLASS_PATH` variable in the `.env` file of the miner runner. For example: `EXECUTOR_MANAGER_CLASS_PATH=myexecutor:MyExecutor`.

This feature is currently in early access stage and is only available on `v0-preprod` images. See the section about preprod images to set up your environment.

## Early Access Features - Preprod Images

Some features are released early and can be used before the official release for the entire subnet. To use these features, you need to set up `v0-preprod` images. Currently, this is only available for miners.

To switch your miner code to preprod images, follow these steps:

1. Stop your miner runner: `docker-compose down --remove-orphans`
2. Add `MINER_IMAGE_TAG=v0-preprod` to the miner runner's `.env` file.
3. Start the miner runner: `docker-compose up -d`

**IMPORTANT**: Currently, this feature is only available on the preprod miner runner. You need to manually change the image tag in `docker-compose.yml` from `v0-latest` to `v0-preprod` before starting the miner runner. This additional step will not be required once the miner preprod feature is released to the upstream miner runner.

# Setup development environment

You'll need to have Python 3.11 and [pdm](https://pdm-project.org) installed.

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
pdm run python manage.py wait_for_database --timeout 10
pdm run python manage.py migrate
pdm run python manage.py runserver
```
