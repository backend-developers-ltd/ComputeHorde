# validator

ComputeHorde Validator. See repositories main README for explanation of what that means.

- - -

Skeleton of this project was generated with `cookiecutter-rt-django`, which sometimes gets upgrades that are easy to retrofit into already older projects.

# Base requirements

- docker
- docker-compose
- python 3.11
- [pdm](https://pdm-project.org)
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
pdm run manage.py wait_for_database --timeout 10
pdm run manage.py migrate
```

Typically, when developing validator, you want to test:

1. Scheduling synthetic jobs
2. Settings weights
3. Passing organic jobs from a facilitator to a miner

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

## Scheduling synthetic jobs

```shell
DEBUG_MINER_KEY=... python manage.py debug_run_synthetic_jobs
```

# Setup production environment (git deployment)

This sets up "deployment by pushing to git storage on remote", so that:

- `git push origin ...` just pushes code to Github / other storage without any consequences;
- `git push production master` pushes code to a remote server running the app and triggers a git hook to redeploy the application.

```
Local .git ------------> Origin .git
                \
                 ------> Production .git (redeploy on push)
```

- - -

Use `ssh-keygen` to generate a key pair for the server, then add read-only access to repository in "deployment keys" section (`ssh -A` is easy to use, but not safe).

```sh
# remote server
mkdir -p ~/repos
cd ~/repos
git init --bare --initial-branch=master validator.git

mkdir -p ~/domains/validator
```

```sh
# locally
git remote add production root@<server>:~/repos/validator.git
git push production master
```

```sh
# remote server
cd ~/repos/validator.git

cat <<'EOT' > hooks/post-receive
#!/bin/bash
unset GIT_INDEX_FILE
export ROOT=/root
export REPO=validator
while read oldrev newrev ref
do
    if [[ $ref =~ .*/master$ ]]; then
        export GIT_DIR="$ROOT/repos/$REPO.git/"
        export GIT_WORK_TREE="$ROOT/domains/$REPO/"
        git checkout -f master
        cd $GIT_WORK_TREE
        ./deploy.sh
    else
        echo "Doing nothing: only the master branch may be deployed on this server."
    fi
done
EOT

chmod +x hooks/post-receive
./hooks/post-receive
cd ~/domains/validator
./setup-prod.sh

# adjust the `.env` file

mkdir letsencrypt
./letsencrypt_setup.sh
./deploy.sh
```

### Deploy another branch

Only `master` branch is used to redeploy an application.
If one wants to deploy other branch, force may be used to push desired branch to remote's `master`:

```sh
git push --force production local-branch-to-deploy:master
```

# Monitoring

Running the app requires proper certificates to be put into `nginx/monitoring_certs`, see `README` located there.

## Monitoring execution time of code blocks

Somewhere, probably in `metrics.py`:

```python
some_calculation_time = prometheus_client.Histogram(
    'some_calculation_time',
    'How Long it took to calculate something',
    namespace='django',
    unit='seconds',
    labelnames=['task_type_for_example'],
    buckets=[0.5, 1, *range(2, 30, 2), *range(30, 75, 5), *range(75, 135, 15)]
)
```

Somewhere else:

```python
with some_calculation_time.labels('blabla').time():
    do_some_work()
```

# Setting up periodic backups

Add to crontab:

```sh
# crontab -e
30 0 * * * cd ~/domains/validator && ./bin/backup-db.sh > ~/backup.log 2>&1
```

Set `BACKUP_LOCAL_ROTATE_KEEP_LAST` to keep only a specific number of most recent backups in local `.backups` directory.

## Configuring offsite targets for backups

Backups are put in `.backups` directory locally, additionally then can be stored offsite in following ways:

**Backblaze**

Set in `.env` file:

- `BACKUP_B2_BUCKET_NAME`
- `BACKUP_B2_KEY_ID`
- `BACKUP_B2_KEY_SECRET`

**Email**

Set in `.env` file:

- `EMAIL_HOST`
- `EMAIL_PORT`
- `EMAIL_HOST_USER`
- `EMAIL_HOST_PASSWORD`
- `EMAIL_TARGET`

## Setting up a trusted miner for cross-validation

### Set up a server

Create an Ubuntu server and use the `install_miner.sh` script from the root of this repository to install the miner in a **local mode**:

```sh
curl -sSfL https://github.com/backend-developers-ltd/ComputeHorde/raw/master/install_miner.sh | bash -s - local SSH_DESTINATION VALIDATOR_PUBLIC_KEY MINER_PORT DEFAULT_EXECUTOR_CLASS
```

Replace the placeholders in the command above:
- SSH_DESTINATION: your server's connection info (i.e. `username@1.2.3.4`)
- VALIDATOR_PUBLIC_KEY: the public key of your validator
- MINER_PORT (optional): the port on which the miner will listen for incoming connections (default is 8000)
- DEFAULT_EXECUTOR_CLASS (optional): specify a custom executor class to use

### Provision S3 buckets for prompts and answers

Trusted miners require S3 buckets to store prompts and answers. To provision these buckets conveniently with the correct permissions pre-configured, make sure you have [AWS CLI](https://aws.amazon.com/cli/) installed and configured.
Then run the following script:

```sh
curl -sSfL https://github.com/backend-developers-ltd/ComputeHorde/raw/master/validator/provision_s3.sh | bash -s - PROMPTS_BUCKET ANSWERS_BUCKET
```

Replace `PROMPTS_BUCKET` and `ANSWERS_BUCKET` with the names of the S3 buckets you want to use for prompts and answers respectively.
