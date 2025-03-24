terraform {
  backend "s3" {
    bucket = "sdk-tnmbci"
    key    = "core.tfstate"
    region = "us-east-1"
  }

  required_providers {
    cloudflare = {
      source = "cloudflare/cloudflare"
      version = "~> 5.2"
    }
  }

  required_version = "~> 1.0"
}
