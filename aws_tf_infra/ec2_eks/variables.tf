variable "region" {
  type        = string
  default     = "us-east-1"
  description = "Default Aws Region for Infra"
}

variable "prefix" {
  type        = string
  default     = "grc"
  description = "Aws default prefix"
}

variable "instance_type" {
  type        = string
  default     = "t3a.xlarge"
  description = "Aws default ec2 for softwares"
}


locals {
  prefix = var.prefix
  common_tags = {
    Project     = "grc-olist-churn"
    Terraform   = true
    Environment = "dev"
  }
}
