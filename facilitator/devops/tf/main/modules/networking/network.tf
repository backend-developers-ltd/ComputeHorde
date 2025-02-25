module "vpc" {
  source = "terraform-aws-modules/vpc/aws"
  version = "3.19.0"
  enable_dns_hostnames = true
  name = "${var.name}-${var.env}-vpc"
  cidr = var.vpc_cidr

  azs                = var.azs
  public_subnets     = var.subnet_cidrs
  enable_nat_gateway = false
  enable_vpn_gateway = false
}
