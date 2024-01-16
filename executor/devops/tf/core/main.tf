provider "aws" {
  region = var.region
}

resource "aws_ecr_repository" "app" {
  name                 = var.name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_repository" "app_staging" {
  name                 = "${var.name}-staging"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}