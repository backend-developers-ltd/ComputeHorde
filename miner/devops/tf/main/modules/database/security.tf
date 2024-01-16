resource "aws_security_group" "db" {
  name        = "${var.name}-db-sg"
  vpc_id      = var.vpc_id

  ingress {
    description      = "allow traffic to postgres port from within VPC"
    from_port        = 5432
    to_port          = 5432
    protocol         = "tcp"
    cidr_blocks      = [var.vpc_cidr]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
  }
}