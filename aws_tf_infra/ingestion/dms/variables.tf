variable "region" {
  type        = string
  default     = "us-east-1"
  description = "Default Aws Region for Infra"
}

variable "rds_password" {
  type        = string
  sensitive   = true
  description = "OLTP RDS Instance Password"
}

variable "rds_user" {
  type        = string
  sensitive   = true
  description = "OLTP RDS Instance User Name"
}

variable "prefix" {
  type        = string
  default     = "grc"
  description = "Aws default prefix"
}

locals {
  prefix = var.prefix
  common_tags = {
    Project     = "grc-olist-churn"
    Terraform   = true
    Environment = "dev"
  }
}
