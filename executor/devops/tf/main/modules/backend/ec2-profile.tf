resource "aws_iam_role" "self" {
  name                = "${var.name}-${var.env}-ec2-role"

  assume_role_policy  = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service: "ec2.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AmazonSSMReadOnlyAccess",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
    "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
  ]
}

resource "aws_iam_instance_profile" "self" {
  name = "${var.name}-${var.env}-ec2-profile"
  role = aws_iam_role.self.name
}
