# Terraform — AWS Power BI Export Hosting

This Terraform module manages the AWS S3 configuration used to host Power BI CSV exports for Project 2: Batch Data Lakehouse.

## Purpose

Power BI Service could not upload local CSV files because the available Microsoft account did not include OneDrive for Business upload support.

To solve that cleanly, this project hosts small synthetic Power BI CSV exports in Amazon S3 and connects Power BI through public HTTPS object URLs.

## What This Module Manages

This module manages only the S3 layer required for Power BI file hosting:

- Existing S3 bucket lookup
- Bucket ownership controls
- Server-side encryption using SSE-S3
- Public access block settings required for selected public CSV URLs
- Bucket policy with public read access only for selected CSV files
- S3 object uploads for Power BI CSV exports

## Files Uploaded

The module uploads these dashboard-ready CSV files:

- `campaign_performance.csv`
- `customer_lifetime_value.csv`
- `product_performance.csv`
- `marketing_funnel.csv`
- `data_quality_summary.csv`

## Free-Safe Design

This step is designed to stay within free-tier-safe usage by using only small S3 objects.

This module does not create:

- EC2
- RDS
- Redshift
- Glue
- Athena
- EMR
- EKS
- NAT Gateway
- Lambda
- CloudFront

## Public Access Scope

The bucket policy grants public `s3:GetObject` only for the selected CSV files.

It does not grant:

- upload access
- delete access
- bucket list access
- write access
- access to unrelated files

## Directory

```text
terraform/aws-bi-exports/
