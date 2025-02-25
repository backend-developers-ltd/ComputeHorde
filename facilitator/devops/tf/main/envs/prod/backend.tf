terraform {
  backend "s3" {
    bucket = "facilitator-ktmxaj"
    key    = "prod/main.tfstate"
    region = "us-east-1"
  }
}
