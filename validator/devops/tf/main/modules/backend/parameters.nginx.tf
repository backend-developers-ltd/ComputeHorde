locals {
  cert_dir   = "../../files/nginx/monitoring_certs"
  cert_files = fileset(local.cert_dir, "*.txt")

  certs = length(local.cert_files) > 0 ? [for cert_file in local.cert_files : {
    name: replace(cert_file, ".txt", "")
    content: "${local.cert_dir}/${cert_file}"
  }] : []

  helper_dir   = "../../files/nginx/config_helpers"
  helper_files = fileset(local.helper_dir, "*")

  helpers = length(local.helper_files) > 0 ? [for helper_file in local.helper_files : {
    name: helper_file,
    content: "${local.helper_dir}/${helper_file}"
  }] : []

  template_dir   = "../../files/nginx/templates"
  template_files = fileset(local.template_dir, "*")

  templates = length(local.template_files) > 0 ? [for template_file in local.template_files : {
    name: template_file,
    content: "${local.template_dir}/${template_file}"
  }] : []
}

resource "aws_ssm_parameter" "certs" {
  count   = length(local.certs)
  name    = "/application/${var.name}/${var.env}/nginx/monitoring_certs/${local.certs[count.index].name}"
  type    = "SecureString"
  value   = file(local.certs[count.index].content)
}

resource "aws_ssm_parameter" "helpers" {
  count   = length(local.helpers)
  name    = "/application/${var.name}/${var.env}/nginx/config_helpers/${local.helpers[count.index].name}"
  type    = "SecureString"
  value   = file(local.helpers[count.index].content)
}

resource "aws_ssm_parameter" "templates" {
  count   = length(local.templates)
  name    = "/application/${var.name}/${var.env}/nginx/templates/${local.templates[count.index].name}"
  type    = "SecureString"
  value   = file(local.templates[count.index].content)
}