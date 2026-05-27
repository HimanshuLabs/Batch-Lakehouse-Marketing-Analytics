output "bucket_name" {
  description = "S3 bucket used for Power BI CSV exports."
  value       = data.aws_s3_bucket.bi_exports.id
}

output "power_bi_csv_urls" {
  description = "Public HTTPS URLs for Power BI Text/CSV link-to-file connections."
  value = {
    for file_name in local.power_bi_export_files :
    file_name => "https://${data.aws_s3_bucket.bi_exports.id}.s3.${var.aws_region}.amazonaws.com/${file_name}"
  }
}
