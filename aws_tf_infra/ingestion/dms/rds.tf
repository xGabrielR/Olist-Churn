resource "aws_db_instance" "pg_rds" {
  identifier = "${var.prefix}-postgres-rds"

  engine_version = "14"
  engine         = "postgres"
  db_name        = "postgres"
  instance_class = "db.t3.medium"

  multi_az               = false
  publicly_accessible    = true
  db_subnet_group_name   = aws_db_subnet_group.db_subnet_group.name
  vpc_security_group_ids = [aws_security_group.allow_postgres.id]

  depends_on = [module.vpc]

  username = var.rds_user
  password = var.rds_password

  allocated_storage   = 10
  storage_encrypted   = false
  skip_final_snapshot = true

  tags = local.common_tags
}
