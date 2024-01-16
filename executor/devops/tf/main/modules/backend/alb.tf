resource "aws_lb" "self" {
  name               = "${var.name}-${var.env}"
  internal           = false
  load_balancer_type = "application"
  subnets            = var.subnets
  security_groups    = [aws_security_group.public.id]
  enable_deletion_protection = false
}

resource "aws_lb_target_group" "self" {
  name        = "${var.name}-${var.env}"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "instance"

  health_check {
    enabled             = true
    port                = 8000
    path                = "/admin/login"
    matcher             = "200-302"
  }
}

resource "aws_lb_listener" "self" {
  load_balancer_arn = aws_lb.self.arn
  port              = "443"
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = aws_acm_certificate.self.arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.self.arn
  }
}
