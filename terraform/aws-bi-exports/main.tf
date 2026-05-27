terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      Purpose     = "power-bi-csv-export-hosting"
    }
  }
}

locals {
  power_bi_export_files = toset([
    "campaign_performance.csv",
    "customer_lifetime_value.csv",
    "product_performance.csv",
    "marketing_funnel.csv",
    "data_quality_summary.csv"
  ])

  export_source_dir = "${path.module}/../../data/bi_exports"
}

data "aws_s3_bucket" "bi_exports" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_ownership_controls" "bi_exports" {
  bucket = data.aws_s3_bucket.bi_exports.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bi_exports" {
  bucket = data.aws_s3_bucket.bi_exports.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bi_exports" {
  bucket = data.aws_s3_bucket.bi_exports.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = false
  restrict_public_buckets = false
}

data "aws_iam_policy_document" "public_read_power_bi_exports" {
  statement {
    sid    = "PublicReadOnlySelectedPowerBIExportCsvFiles"
    effect = "Allow"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = [
      "s3:GetObject"
    ]

    resources = [
      for file_name in local.power_bi_export_files :
      "${data.aws_s3_bucket.bi_exports.arn}/${file_name}"
    ]
  }
}

resource "aws_s3_bucket_policy" "public_read_power_bi_exports" {
  bucket = data.aws_s3_bucket.bi_exports.id
  policy = data.aws_iam_policy_document.public_read_power_bi_exports.json

  depends_on = [
    aws_s3_bucket_public_access_block.bi_exports
  ]
}

resource "aws_s3_object" "power_bi_exports" {
  for_each = local.power_bi_export_files

  bucket       = data.aws_s3_bucket.bi_exports.id
  key          = each.value
  source       = "${local.export_source_dir}/${each.value}"
  etag         = filemd5("${local.export_source_dir}/${each.value}")
  content_type = "text/csv"

  server_side_encryption = "AES256"

  depends_on = [
    aws_s3_bucket_ownership_controls.bi_exports,
    aws_s3_bucket_server_side_encryption_configuration.bi_exports
  ]
}
