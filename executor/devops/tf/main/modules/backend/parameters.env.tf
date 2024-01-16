resource "random_uuid" "random_uuid" {}

resource "aws_ssm_parameter" "envrc" {
  name  = "/application/${var.name}/${var.env}/.envrc"
  type  = "SecureString"
  value = templatefile("../../files/envrc", {
    name          = var.name
    env           = var.env
    region        = var.region
    account_id    = var.account_id
    ecr_base_url  = var.ecr_base_url
    ecr_image     = var.ecr_image
  })
}


resource "aws_ssm_parameter" "env" {
  name  = "/application/${var.name}/${var.env}/.env"
  type  = "SecureString"
  value = templatefile("../../files/env", {
    name          = var.name
    env           = var.env
    region        = var.region
    secret_key    = random_uuid.random_uuid.result

    database_name               = var.database.name
    database_user               = var.database.user
    database_password           = var.database.password
    database_connection_string  = var.database.connection_string
  })
}