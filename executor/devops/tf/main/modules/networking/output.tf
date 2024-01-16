output "vpc_id" {
    value = module.vpc.vpc_id
}

output "vpc_cidr_block" {
    value = module.vpc.vpc_cidr_block
}

output "subnets" {
    value = module.vpc.public_subnets
}

output "azs" {
    value = module.vpc.azs
}
