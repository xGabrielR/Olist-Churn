variable "region" {
  type        = string
  default     = "us-east-1"
  description = "Default Aws Region for Infra"
}

variable "bucket_names" {
  type        = list(string)
  description = "Aws default buckets"
  default = [
    "landing",   # dms data store
    "bronze",    # bronze iceberg
    "silver",    # silver iceberg
    "gold",      # gold iceberg
    "analytics", # feature store and abt
    "scripts",   # scripts and features
    "artifacts"  # mlflow artifacts
  ]
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
