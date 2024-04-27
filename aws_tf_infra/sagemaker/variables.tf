variable "region" {
  type        = string
  default     = "us-east-1"
  description = "Default Aws Region for Infra"
}

variable "repository_url" {
  type        = string
  description = "Private git url link"
}

variable "repo_secret_json" {
  sensitive   = true
  type        = map(string)
  description = "User git credentials (name and github token)"
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
