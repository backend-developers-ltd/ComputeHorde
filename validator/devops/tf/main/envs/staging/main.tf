provider "aws" {
  region = var.region
}

data "aws_caller_identity" "env" {}

data "aws_ami" "base_ami" {
  most_recent = true

  filter {
    name   = "name"
    values = [var.base_ami_image]
  }

  filter {
    name = "virtualization-type"
    values = ["hvm"]
  }

  owners = [var.base_ami_image_owner]
}

locals {
  ecr_base_url = "${data.aws_caller_identity.env.account_id}.dkr.ecr.${var.region}.amazonaws.com"
  ecr_image    = "${var.name}-${var.env}:latest"
}

module "networking" {
  source = "../../modules/networking"

  name      = var.name
  env       = var.env
  azs       = var.azs
  vpc_cidr  = var.vpc_cidr
  subnet_cidrs = var.subnet_cidrs
}

module "database" {
  source = "../../modules/database"

  name          = var.name
  env           = var.env
  vpc_id        = module.networking.vpc_id
  vpc_cidr      = module.networking.vpc_cidr_block
  azs           = module.networking.azs
  subnets       = module.networking.subnets
  instance_type = var.rds_instance_type
}

module "backend" {
  source = "../../modules/backend"

  depends_on = [
    module.database
  ]

  base_ami_id = data.aws_ami.base_ami.image_id

  name             = var.name
  region           = var.region
  env              = var.env

  ecr_base_url     = local.ecr_base_url
  ecr_image        = local.ecr_image

  base_domain_name = var.base_domain_name
  domain_name      = var.domain_name
  ec2_ssh_key      = var.ec2_ssh_key

  vpc_id      = module.networking.vpc_id
  vpc_cidr    = module.networking.vpc_cidr_block

  azs         = module.networking.azs
  subnets     = module.networking.subnets

  instance_type     = var.instance_type
  health_check_type = var.autoscaling_health_check_type
  account_id        = data.aws_caller_identity.env.account_id
  database          = module.database
}