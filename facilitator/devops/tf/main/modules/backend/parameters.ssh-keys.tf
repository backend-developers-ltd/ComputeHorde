resource "aws_ssm_parameter" "ssh-keys" {
  name = "/application/${var.name}/${var.env}/.ssh/authorized_keys"
  type = "SecureString"
  value = var.ec2_ssh_key
  lifecycle {
    ignore_changes = [value]
  }
}
