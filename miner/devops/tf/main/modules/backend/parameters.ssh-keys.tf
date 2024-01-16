resource "aws_ssm_parameter" "ssh-keys" {
  name = "/application/${var.name}/${var.env}/.ssh/authorized_keys"
  type = "SecureString"
  value = templatefile("../../files/authorized_keys", {
    ec2_ssh_key = var.ec2_ssh_key
  })
}
