# ComputeHorde Miner



- - -

Skeleton of this project was generated with `cookiecutter-rt-django`, which sometimes gets upgrades that are easy to retrofit into already older projects.

# Run production instance

## Runner

Most straightforward option is to use a "miner-runner" docker image which will launch everything automatically and will handle auto-updates. See [runner README](envs/runner/README.md) for more details.

## Docker-compose

Alternatively, you can use `docker-compose` to launch all the necessary services manually. See [docker-compose.yml](envs/runner/data/docker-compose.yml) for reference.

# Setup development environment

```sh
# 1st tab
$ python -m venv venv
$ source venv/bin/activate
$ ./setup-dev.sh
```

```sh
# 2nd tab
docker-compose up
```

```sh
# 1st tab
cd app/src
python manage.py wait_for_database --timeout 10
python manage.py migrate
python manage.py runserver
```
