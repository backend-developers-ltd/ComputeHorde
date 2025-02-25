output "connection_string" {
  value     = "postgres://${aws_db_instance.self.username}:${aws_db_instance.self.password}@${aws_db_instance.self.endpoint}/${aws_db_instance.self.db_name}"
  sensitive = true
}

output "user" {
  value     = aws_db_instance.self.username
}

output "password" {
  value     = aws_db_instance.self.password
  sensitive = true
}

output "endpoint" {
  value = aws_db_instance.self.endpoint
}

output "port" {
  value = aws_db_instance.self.port
}

output "name" {
  value = aws_db_instance.self.db_name
}