resource "aws_security_group" "public" {
  name        = "${var.name}-${var.env}-public-sg"
  vpc_id      = var.vpc_id

  ingress {
    description      = "allow traffic between load-balancer and EC2 instances within VPC"
    from_port        = 80
    to_port          = 80
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
  }

  ingress {
    description      = "allow traffic between load-balancer and EC2 instances within VPC"
    from_port        = 443
    to_port          = 443
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "internal" {
  name        = "${var.name}-internal-sg"
  vpc_id      = var.vpc_id

  ingress {
    description      = "allow traffic to ssh from internet"
    from_port        = 22
    to_port          = 22
    protocol         = "tcp"
    cidr_blocks      = ["51.254.203.61/32"]
  }

  ingress {
    description      = "allow monitoring"
    from_port        = 10443
    to_port          = 10443
    protocol         = "tcp"
    cidr_blocks      = ["138.68.147.48/32", "95.179.202.73/32"]
  }

  ingress {
    description      = "allow traffic between load-balancer and EC2 instances within VPC"
    from_port        = 8000
    to_port          = 8000
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
