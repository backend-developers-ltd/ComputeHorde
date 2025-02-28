variable "name" {
  type    = string
}

variable "region" {
  type    = string
}

variable "subdomain" {
  type    = string
}

variable "hosted_zone" {
  type    = string
}

variable "cloudflare_zone_id" {
  description = "Cloudflare zone ID"
  type        = string
}