module "dms" {
  source  = "terraform-aws-modules/dms/aws"
  version = "1.6.1" # The new module have issues, i need to minimal changes the .terraform module file

  # Subnet
  repl_subnet_group_name        = "grcDmsSubnetGroup"
  repl_subnet_group_description = "DMS Subnet group"
  repl_subnet_group_subnet_ids  = module.vpc.database_subnets

  # Instance
  repl_instance_allocated_storage      = 10
  repl_instance_apply_immediately      = true
  repl_instance_publicly_accessible    = true
  repl_instance_multi_az               = false
  repl_instance_class                  = "dms.t3.small"
  repl_instance_id                     = "grcReplicationInstance"
  repl_instance_vpc_security_group_ids = [aws_security_group.allow_postgres.id]

  endpoints = {
    source = {
      endpoint_id                 = "${var.prefix}-oltp-pg-source"
      port                        = 5432
      ssl_mode                    = "none"
      endpoint_type               = "source"
      extra_connection_attributes = "heartbeatFrequency=1;"
      database_name               = aws_db_instance.pg_rds.db_name
      engine_name                 = aws_db_instance.pg_rds.engine
      server_name                 = aws_db_instance.pg_rds.address
      username                    = aws_db_instance.pg_rds.username
      password                    = aws_db_instance.pg_rds.password
      tags                        = { EndpointType = "source" }
    }

    destination = {
      endpoint_id                 = "${var.prefix}-oltp-pg-target"
      endpoint_type               = "target"
      engine_name                 = "s3"
      extra_connection_attributes = "DataFormat=parquet;parquetVersion=PARQUET_2_0;"
      s3_settings = {
        bucket_folder           = "postgres"
        bucket_name             = "grc-lh-landing"
        compression_type        = "GZIP"
        service_access_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/dms-s3-role"
        data_format             = "parquet"
        timestamp_column_name   = "last_updated_dt"
      }
    }
  }

  replication_tasks = {
    s3_import = {
      replication_task_id = "postgresToS3"
      migration_type      = "full-load"
      table_mappings      = file("dms_configs/table_mappings.json")
      source_endpoint_key = "source"
      target_endpoint_key = "destination"
      tags                = { Task = "postgres-to-s3" }
    }
  }

  tags = local.common_tags

}
