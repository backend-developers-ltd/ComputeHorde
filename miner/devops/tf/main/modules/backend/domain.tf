data "aws_route53_zone" "self" {
  name = var.base_domain_name
}

resource "aws_route53_record" "a" {
  zone_id = data.aws_route53_zone.self.zone_id
  name = var.domain_name
  type = "A"

  alias {
    name                   = aws_lb.self.dns_name
    zone_id                = aws_lb.self.zone_id
    evaluate_target_health = true
  }
}

resource "aws_acm_certificate" "self" {
  domain_name       = var.domain_name
  validation_method = "DNS"

  tags = {
    Project = var.name
    Env = var.env
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "cert-validation" {
  for_each = {
    for dvo in aws_acm_certificate.self.domain_validation_options: dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }

  allow_overwrite = true
  name            = each.value.name
  records         = [each.value.record]
  ttl             = 60
  type            = each.value.type
  zone_id         = data.aws_route53_zone.self.zone_id
}