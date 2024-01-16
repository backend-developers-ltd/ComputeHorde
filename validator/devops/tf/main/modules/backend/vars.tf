variable "name" {}
variable "env" {}
variable "region" {}

variable "vpc_id" {}
variable "vpc_cidr" {}
variable "subnets" {}
variable "azs" {}

variable "base_ami_id" {}
variable "base_domain_name" {}

variable "domain_name" {}
variable "ec2_ssh_key" {}

variable "ecr_base_url" {}
variable "ecr_image" {}

variable "instance_type" {}
variable "health_check_type" {}
variable "account_id" {}
variable "database" {}