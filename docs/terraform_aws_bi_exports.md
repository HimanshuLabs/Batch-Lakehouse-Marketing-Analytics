# Terraform AWS BI Export Layer

Step 21 adds Infrastructure as Code for the AWS S3 layer used by the Power BI dashboard.

The project uses S3 because Power BI Service required stable file URLs and the available Microsoft account did not include OneDrive for Business upload support.

## Architecture

```text
Local Power BI CSV exports
        ↓
Terraform-managed S3 object upload
        ↓
Public HTTPS S3 object URLs
        ↓
Power BI Text/CSV semantic models
        ↓
Power BI report/dashboard
```

## What the module does

- The S3 bucket is created outside Terraform; the module looks it up by name.
- It sets bucket ownership controls, SSE-S3 encryption and the public access block settings.
- Its bucket policy grants public `s3:GetObject` on exactly five named CSV files: `campaign_performance.csv`, `customer_lifetime_value.csv`, `product_performance.csv`, `marketing_funnel.csv` and `data_quality_summary.csv`. It grants no listing and no write access.
- It uploads those five files from `data/bi_exports/`, where the DAG's `power_bi_export` task writes them.

See `terraform/aws-bi-exports/README.md` for the module details.
