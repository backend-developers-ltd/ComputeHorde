provider "cloudflare" {}

resource "cloudflare_dns_record" "cname_record" {
  zone_id = var.cloudflare_zone_id
  name    = var.subdomain
  content = var.destination_domain
  proxied = true
  ttl     = 1
  type    = "CNAME"
}
