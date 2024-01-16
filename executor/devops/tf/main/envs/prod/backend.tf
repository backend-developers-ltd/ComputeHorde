terraform {
  backend "s3" {
    bucket = "compute_horde_executor-ijhava"
    key    = "prod/main.tfstate"
    region = "us-east-1"
  }
}
