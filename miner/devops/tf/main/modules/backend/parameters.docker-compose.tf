data "aws_partition" "self" {}

resource "aws_ssm_parameter" "compose" {
  name = "/application/${var.name}/${var.env}/docker-compose.yml"
  type = "SecureString"
  value = templatefile("../../files/docker-compose.yml", {
    name          = var.name
    env           = var.env
    region        = var.region
    ecr_base_url  = var.ecr_base_url
    ecr_image     = var.ecr_image
  })
}