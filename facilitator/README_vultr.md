# Deploying to Vultr


Files related to Vultr deployment are in `devops/vultr_scripts/` and `devops/vultr_tf`. 

To use Terraform, you need:
- create API key which you can find in Vultr -> Account -> API: <https://my.vultr.com/settings/#settingsapi>
- allow your IP in Access Control section at the same page as above

- To use ssh keys in Terraform, you need to create them in Vultr -> Account -> SSH Keys: <https://my.vultr.com/settings/#settingssshkeys>


## Required software


*Terraform* You will also need terraform version 1.0.x. It is recommended to use `tfenv` to install terraform with correct version.
You can download an install it from <https://github.com/tfutils/tfenv>

*direnv* To avoid mistakes when switching environments (or regions), it is recommended to use `direnv` tools, which supports loading environment variables from .envrc file, placed in directory.
You can read about it here:
<https://direnv.net/>

(recommended) *Vultr CLI* via <https://github.com/vultr/vultr-cli> to interact with Vultr instances post-deployment, eg. get their IP addressed, instances ID, update Cloud Init data.

## Configure your environment


To deploy via Terraform, you have to fill all variables for Cloud Init in `vultr-cloud-init.tftpl`.
These variables can be sourced from various sources, recommended approach is to use environment variables <https://developer.hashicorp.com/terraform/language/values/variables#environment-variables> in combination with `dotenv`

To use Vultr CLI, you have to have API key, ideally in environment variable again. 

## Configuring infra

You only need to do this if you change anything in `devops/vultr_tf` directory.

TODO - currently TF Vultr is not configured to use S3 buckets. 


```
terraform init
terraform apply
```

## Adding secrets to the projects

Project uses `.env` file in same directory as `docker-compose.yml` is, so any secrets should be sourced via this file.

Do not commit secrets into the repository, this `.env` file can be updated via Cloud init executed when a new machines is spawned or reinstalled. The Cloud Init is located in Terraform directory: `vultr-cloud-init.tftpl`.

After spawning the machines, Cloud Init can be updated via Vultr CLI, see `devops/vultr_scripts/vultr-update-cloudinit.py`. Updating Cloud Data in Terraform would mean destroying & recreating all instances from scratch.


## Deploying apps

Deployment is executed via `post-receive` hook in git repository on each instance. See `devops/vultr_scripts/vultr-deploy.py`
