resource "random_string" "random" {
  length           = 20
  special          = true
  override_special = "$."
}

resource "aws_db_subnet_group" "self" {
  name       = "${var.name}-${var.env}"
  subnet_ids = var.subnets

  tags = {
    Project = var.name
    Env = var.env
    Name = "DB subnet group"
  }
}

resource "aws_db_parameter_group" "dont_force_ssl" {
  name   = "${var.name}-${var.env}-dont-force-ssl"
  family = "postgres16"

  parameter {
    name  = "rds.force_ssl"
    value = "0"
  }
}

resource "aws_db_instance" "self" {
  identifier             = "${var.name}-${var.env}-db"
  publicly_accessible    = true
  allocated_storage      = 15
  max_allocated_storage  = 100
  storage_encrypted      = true
  engine                 = "postgres"
  instance_class         = var.instance_type
  username               = "master"
  db_name                = "backend"
  password               = random_string.random.result
  skip_final_snapshot    = true
  availability_zone      = var.azs[0]
  db_subnet_group_name   = aws_db_subnet_group.self.name
  vpc_security_group_ids = [aws_security_group.db.id]
  backup_retention_period = 7

  parameter_group_name = aws_db_parameter_group.dont_force_ssl.name

  tags = {
    Project = var.name
    persistent = "true"
  }

}

resource "aws_db_instance" "replica" {
  identifier             = "${var.name}-${var.env}-db-replica"
  replicate_source_db    = aws_db_instance.self.id
  publicly_accessible    = true
  storage_encrypted      = true
  engine                 = "postgres"
  max_allocated_storage  = 100
  instance_class         = "db.t3.small"
  skip_final_snapshot    = true
  availability_zone      = var.azs[0]
  vpc_security_group_ids = [aws_security_group.db.id]

  parameter_group_name = aws_db_parameter_group.dont_force_ssl.name
  backup_retention_period = 7

  tags = {
    Project = var.name
    persistent = "true"
  }

}
