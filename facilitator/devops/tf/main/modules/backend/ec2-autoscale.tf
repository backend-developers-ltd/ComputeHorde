locals {
  name_env    = "${var.name}-${var.env}"
  cloudinit   = templatefile("../../files/cloud-init.yml", {
    name      = var.name
    env       = var.env
    region    = var.region
  })
}

resource "aws_launch_template" "self" {
  name          = local.name_env
  image_id      = var.base_ami_id
  instance_type = var.instance_type

  iam_instance_profile {
    name = aws_iam_instance_profile.self.name
  }

  disable_api_termination = false
  key_name                = aws_key_pair.self.key_name

  user_data = base64encode(local.cloudinit)

  block_device_mappings {
    device_name = "/dev/sda1"

    ebs {
      delete_on_termination = true
      encrypted = true
      volume_size = 20
    }
  }

  credit_specification {
    cpu_credits = "standard"
  }

  vpc_security_group_ids = [
    aws_security_group.internal.id
  ]

}

resource "aws_autoscaling_group" "self" {
  name                = local.name_env
  desired_capacity    = 1
  max_size            = 1
  min_size            = 1
  vpc_zone_identifier = [var.subnets[0]]

  launch_template {
    id = aws_launch_template.self.id
    version = "$Latest"
  }

  tag {
    key = "Name"
    propagate_at_launch = true
    value = local.name_env
  }

  target_group_arns = [
    aws_lb_target_group.self.arn
  ]

  health_check_type = var.health_check_type
}
