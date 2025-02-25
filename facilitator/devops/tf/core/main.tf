provider "aws" {
  region = var.region
}

provider "cloudflare" {}

resource "aws_ecr_repository" "app" {
  name                 = "${var.name}-prod"
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

resource "aws_route53_zone" "hosted_zone" {
  name = var.hosted_zone
}

resource "cloudflare_record" "ns_records" {
  for_each = { for ns_record in aws_route53_zone.hosted_zone.name_servers : ns_record => ns_record }

  type    = "NS"
  zone_id = var.cloudflare_zone_id
  name    = var.subdomain
  content = each.value
  proxied = false

  lifecycle {
    ignore_changes = [
      name,
      content,
      type,
      proxied
    ]
  }
}