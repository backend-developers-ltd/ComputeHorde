provider "vultr" {
  api_key = var.vultr_api_key
}

resource "vultr_instance" "worker" {
  count       = 1
  hostname    = "instance-project-${count.index + 1}"
  region      = var.region
  plan        = "vc2-1c-1gb" // via `vultr-cli plans list`
  os_id       = 1743 // ubuntu 22-04, via `vultr-cli os list`
  ssh_key_ids = [
    // uuid-4 of ssh keys added in Vultr
  ]
  enable_ipv6      = true
  activation_email = false
  label            = "instance-project"
  backups          = "disabled"

  user_data = templatefile("vultr-cloud-init.tftpl", {
    DEPLOY_SSH_KEY       = var.DEPLOY_SSH_KEY
    SECRET_KEY           = var.DOTENV_SECRET_KEY
    POSTGRES_HOST        = var.DOTENV_POSTGRES_HOST
    POSTGRES_USER        = var.DOTENV_POSTGRES_USER
    POSTGRES_PASSWORD    = var.DOTENV_POSTGRES_PASSWORD
    DATABASE_POOL_URL    = var.DOTENV_DATABASE_POOL_URL
    DATABASE_URL         = var.DOTENV_DATABASE_URL
    SENTRY_DSN           = var.DOTENV_SENTRY_DSN
  })
}

resource "vultr_load_balancer" "loadbalancer" {
  region = var.region

  forwarding_rules {
    frontend_protocol = "https"
    frontend_port     = 443
    backend_protocol  = "https"
    backend_port      = 443
  }

  health_check {
    path                = "/admin/"
    port                = "443"
    protocol            = "https"
    response_timeout    = 5
    unhealthy_threshold = 2
    check_interval      = 15
    healthy_threshold   = 4
  }

  attached_instances = [for instance in vultr_instance.worker : instance.id]
}
