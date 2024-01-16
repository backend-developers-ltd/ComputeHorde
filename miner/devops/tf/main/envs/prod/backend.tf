terraform {
  backend "s3" {
    bucket = "compute_horde_miner-yaklfc"
    key    = "prod/main.tfstate"
    region = "us-east-1"
  }
}
