variable "aws_region" {
  description = "AWS region where the S3 bucket exists."
  type        = string
  default     = "ap-south-2"
}

variable "bucket_name" {
  description = "Existing S3 bucket used to host Power BI CSV exports."
  type        = string
}

variable "project_name" {
  description = "Project name used for tagging."
  type        = string
  default     = "batch-lakehouse-marketing-analytics"
}

variable "environment" {
  description = "Deployment environment label."
  type        = string
  default     = "portfolio"
}
