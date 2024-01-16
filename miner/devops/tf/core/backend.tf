terraform {
  backend "s3" {
    bucket = "compute_horde_miner-yaklfc"
    key    = "core.tfstate"
    region = "us-east-1"
  }

  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 4.0"
    }
  }

  required_version = "~> 1.0"
}