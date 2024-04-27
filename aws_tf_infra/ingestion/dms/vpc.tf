data "aws_availability_zones" "available" {}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.0.0"

  name             = "${var.prefix}-vpc"
  cidr             = "10.0.0.0/16"
  private_subnets  = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets   = ["10.0.4.0/24", "10.0.5.0/24", "10.0.6.0/24"]
  database_subnets = ["10.0.7.0/24", "10.0.8.0/24", "10.0.9.0/24"]

  create_database_subnet_group = true
  enable_nat_gateway           = true
  single_nat_gateway           = true
  enable_dns_hostnames         = true

  azs = data.aws_availability_zones.available.names

  tags = local.common_tags
}

resource "aws_db_subnet_group" "db_subnet_group" {
  name       = "${var.prefix}-db-subnet"
  subnet_ids = module.vpc.public_subnets

  tags = local.common_tags
}

resource "aws_security_group" "allow_postgres" {
  name        = "grc_allow_rds_postgres"
  description = "SG for Postgres RDS"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name = "allow_rds_postgres"
  }
}
