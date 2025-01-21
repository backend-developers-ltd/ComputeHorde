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

3. To use your custom executor, set the `EXECUTOR_MANAGER_CLASS_PATH` variable in the `.env` file of the miner runner.

4. Note that for streaming jobs, there is an NGINX_PORT for users to interface with the job. This port should be provided as an environment variable when triggering the `run_executor` command, otherwise it will use the default value. The executor manager should handle the port allocation.

To create a custom executor manager, follow these steps:

1. Create a directory for your code, e.g., `/home/ubuntu/custom_executor`.

2. Inside the directory, create a file named `my_executor.py` and add the following code:

   ```python
    from compute_horde_miner.miner.executor_manager.base import BaseExecutorManager

    class MyExecutor(BaseExecutorManager):
        async def start_new_executor(self, token, executor_class, timeout):
            """Start spinning up an executor with `token` for given executor_class or raise ExecutorUnavailable if at capacity

            `timeout` is provided so manager does not need to relay on pool cleanup to stop expired executor"""
            pass

        async def kill_executor(self, executor):
            """Kill running executor. It might be platform specific, so leave it to Manager implementation"""
            pass

        async def wait_for_executor(self, executor, timeout):
            """Wait for executor to finish the job for till timeout.

            Have to return not None status if executor finished. If returned status is None it means that
            executor is still running.
            """
            pass

        async def get_manifest(self) -> dict[ExecutorClass, int]:
            """Return executors manifest

            Keys are executor class ids and values are number of supported executors for given executor class.
            """
            pass

       async def get_executor_public_address(self, executor: Any) -> str | None:
            """To be given to clients to connect to streaming jobs"""
            return None
   ```

   You need to implement 4 methods (`start_new_executor`, `kill_executor`, `wait_for_executor` and `get_manifest`) to make the executor work. For reference, you can check the implementation in `compute_horde_miner.miner.executor_manager.docker`.

If you don't implement `get_executor_public_address` the executor's public address will be "guessed" by miner's at connection time - might not work well if your miner<->executor connection uses a local network. the input of this method is the outcome of `start_new_executor`. **IMPORTANT!** If you don't supply the right public addresses of your executors all [streaming jobs](/docs/job_flow.md#streaming-organic-job-flow) will fail.

3. Update your `.env` file with the following variables:

   ```
   HOST_VENDOR_DIR=/home/ubuntu/custom_executor
   EXECUTOR_MANAGER_CLASS_PATH=my_executor:MyExecutor
   ```

   This tells the miner where to find your custom executor implementation.

4. If your implementation has any dependencies, you can create a `setup.sh` file in the same directory. For example:

   ```bash
   pip install requests
   ```

   This file will be executed during the setup process to install the required dependencies.

5. Remember to set up the preprod miner images if the feature is not yet officially released.

By following these steps, you can create and use a custom executor manager in your compute_horde_miner setup.

## Early Access Features - Preprod Images
Some features are released early and can be used before the official release for the entire subnet. To use these features, you need to set up `v0-preprod` images. Currently, this is only available for miners.
To switch your miner code to preprod images, follow these steps:
1. Stop your miner runner: `docker-compose down --remove-orphans`
2. Add `MINER_IMAGE_TAG=v0-preprod` to the miner runner's `.env` file.
   - Note: Ensure that the `COMPOSE_PROJECT_NAME` in your `.env` file is set to the name of the directory containing the `docker-compose.yaml` file. If it's not set correctly, you may need to manually stop all running Docker containers related to the miner before proceeding.
3. Start the miner runner: `docker-compose up -d`

**IMPORTANT**: Currently, this feature is only available on the preprod miner runner. You need to manually change the image tag in `docker-compose.yml` from `v0-latest` to `v0-preprod` before starting the miner runner. This additional step will not be required once the miner preprod feature is released to the upstream miner runner.


## Self testing the miner

You can self test your miner instance using the `self_test` command. It will simulate requests from the validator and check if your miner instance is able to handle them.

```sh
docker-compose exec miner_runner docker-compose exec miner python manage.py self_test
```

To use custom weights_version (instead of loading from the hyperparameters on chain), use the `--weights-version` option:

```sh
docker-compose exec miner_runner docker-compose exec miner python manage.py self_test --weights-version 2
```

# Setup development environment

You'll need to have Python 3.11 and [uv](https://docs.astral.sh/uv/) installed.

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
uv run python manage.py wait_for_database --timeout 10
uv run python manage.py migrate
uv run python manage.py runserver
```
