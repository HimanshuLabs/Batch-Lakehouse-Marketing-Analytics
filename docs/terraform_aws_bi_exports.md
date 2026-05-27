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
