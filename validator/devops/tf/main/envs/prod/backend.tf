terraform {
  backend "s3" {
    bucket = "compute_horde_validator-vwjmwh"
    key    = "prod/main.tfstate"
    region = "us-east-1"
  }
}
