variable "region" {
  type = string
}

variable "name" {
  type = string
}

variable "env" {
  type = string
}

variable "base_ami_image" {
  type = string
}

variable "base_ami_image_owner" {
  type = string
}

variable "vpc_cidr" {
  type    = string
}

variable "subnet_cidrs" {
  type    = set(string)
}

variable "azs" {
  type    = set(string)
}

variable "base_domain_name" {
  type    = string
}

variable "domain_name" {
  type    = string
}

variable "ec2_ssh_key" {
  type = string
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
}

variable "rds_instance_type" {
  description = "RDS instance type"
  type        = string
}

variable "autoscaling_health_check_type" {
  description = "either EC2 or ELB"
  type = string
}
