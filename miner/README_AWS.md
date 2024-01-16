# Deploying to AWS

The deployment is split into two steps:

Files related to AWS deployment has been generated in `devops/` directory.

By convention, projects that are meant to be deployed to AWS have a `deploy-to-aws.sh` script in the root dir and a `devops` directory.
The script builds the docker image, uploads it and tells AWS to reload the app (causing a new ec2 machine to be spawned).
In the `devops` directory you will find terraform configuration as well as packer files (for building the AMI).

If you want to deploy your app to an AWS environment, you need to do following steps:

- configuring your environment
- create an infra s3 bucket
- deploy `tf/core` (contains stuff common to all environments in given AWS Account)
- deploy chosen `tf/main/envs/<selected_env>` (by default staging and prod are generated)

## Required software

*AWS CLI*

AWS recommends using profiles, when dealing with multiple AWS accounts.
To choose between environments, rather than switching access and secret keys, we just switch our profiles.
We can choose our profile name, which make it easier to recognize in which environment we operate.
To configure AWS environment, you need to have AWS CLI installed.
It is recommended to use AWS v2, which can be downloaded from:
<https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>

*Terraform* You will also need terraform version 1.0.x. It is recommended to use `tfenv` to install terraform with correct version.
You can download an install it from <https://github.com/tfutils/tfenv>

*direnv* To avoid mistakes when switching environments (or regions), it is recommended to use `direnv` tools, which supports loading environment variables from .envrc file, placed in directory.
You can read about it here:
<https://direnv.net/>

## Configure your environment

To configure your AWS profile, please run:

```
$ aws configure --profile <profile_name>
```

And answer following questions:

```
AWS Access Key ID: ...
AWS Secret Access Key: ...
Default region name: us-east-1 (just an example)
Default output format [None]: (can be left blank)
```

Once, configured, you can switch your profile using `AWS_PROFILE=` env variable or by adding `--profile` option to your aws cli command.

It's handy to create .envrc file in the project rood directory (where deploy-to-aws.sh is created) with content:

```
export AWS_PROFILE=<your_profile_name>
export AWS_REGION=<selected_aws_region>
```

And then accept changes by using command:

```
$ direnv allow
```

After doing that, anytime you enter the project directory, correct profile will be loaded.

## Configuring infra

You only need to do this if you change anything in `devops` directory (or if you mess something up in AWS console and want to revert the changes).

Create infra bucket

Before being able to run terraform, we need to create S3 bucket, which will hold the state.
This bucket is used by all environments and needs to be globally unique.

To create bucket, please type:

```
aws s3 mb --region us-east-1 s3://compute_horde_miner-yaklfc
```

TF has a following structure:

```
|- devops
  |- tf
    |- core
    |- main
      |- envs
      | |- staging
      | |- prod
      |- modules
```

You can run terraform from:

- core
- envs/staging
- envs/prod

directories.

Directory *core* contains infrastructure code, which needs to be created BEFORE pushing docker image.
It is responsible for creating docker registries, which you can use, to push docker images to.

Code places in *main* is the rest of the infrastructure, which is created after pushing docker image.

Each of the environment (and core) can be applied by executing:

```
terraform init
terraform apply
```

IMPORTANT! the env variables for the apps (`.env` file) and `docker-compose.yml` are defined in terraform files, if you change any, you need to run `terraform apply` AND refresh the ec2 instance.
The same goes for AMI built by packer.

## Adding secrets to the projects

Cloud init is configured to provision EC2 machines spun up as part of this project's infrastructure.
As part of this provisioning, SSM parameters following a specific name convention are read and saved as files in EC2's home directory (RDS access details are managed in another way).
The naming convention is `/application/compute_horde_miner/{env}/{path_of_the_file_to_be_created}`, for example `/application/project/staging/.env`.
A few such parameters are managed by terraform in this project (e.g. `.env`, `docker-compose.yml`) and more can be added.
In case you need to add confidential files (like a GCP credentials file) you can simply create appropriate SSM parameters.
These will only be accessible to people that access to AWS or EC2 machines, not to people who have access to this repository.
One such parameter, namely `/application/compute_horde_miner/{env}/secret.env` is treated specially - if it exists (it doesn't by default) its contents are appended to `.env` during EC2 machine provisioning - this is a convenient way of supplying pieces of confidential information, like external systems' access keys to `.env`.

## Deploying apps

The docker containers are built with code you have locally, including any changes.
Building requires docker.
To successfully run `deploy-to-aws.sh` you first need to do `./setup.prod.sh`.
It uses the aws credentials stored as `AWS_PROFILE` variable.
If you don't set this variable, the `default` will be used.
