terraform {
  backend "s3" {
    bucket = "facilitator-ktmxaj"
    key    = "staging/main.tfstate"
    region = "us-east-1"
  }
}
