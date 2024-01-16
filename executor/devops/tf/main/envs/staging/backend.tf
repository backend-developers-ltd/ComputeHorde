terraform {
  backend "s3" {
    bucket = "compute_horde_executor-ijhava"
    key    = "staging/main.tfstate"
    region = "us-east-1"
  }
}
