# Power BI Refresh Flow

## Purpose

This document explains how the Power BI Online dashboard receives updated reporting data from the merged lakehouse and warehouse platform.

The refresh design is Ubuntu-friendly. It does not require Power BI Desktop and does not require a mandatory `.pbix` file.

## Refresh architecture

```text
Airflow orchestration
    ↓
Raw / Bronze / Silver / Gold lakehouse processing
    ↓
Gold and SCD2 outputs
    ↓
PostgreSQL staging
    ↓
PostgreSQL warehouse star schema
    ↓
PostgreSQL marts
    ↓
scripts/export_power_bi_marts.py
    ↓
exports/power_bi/*.csv
    ↓
Power BI Online upload / refresh
    ↓
Dashboard visuals
```

## Source marts

The Power BI export process reads only from these PostgreSQL marts:

```text
marts.mart_revenue_daily
marts.mart_campaign_performance
marts.mart_product_sales
marts.mart_customer_360
marts.mart_marketing_funnel
```

The dashboard does not read directly from:

```text
data/raw/
data/bronze/
data/silver/
data/gold/
staging.*
warehouse.*
```

The reporting boundary is:

```text
PostgreSQL marts → CSV exports → Power BI Online dashboard
```

## Why CSV export is used

CSV export is used because:

- It works cleanly on Ubuntu.
- It avoids mandatory Windows tooling.
- It keeps Power BI as a thin presentation layer.
- It creates inspectable reporting datasets.
- It keeps business logic inside PostgreSQL marts instead of duplicating it in the dashboard.

## Export script

The export script is:

```text
scripts/export_power_bi_marts.py
```

It writes:

```text
exports/power_bi/revenue_daily.csv
exports/power_bi/campaign_performance.csv
exports/power_bi/product_sales.csv
exports/power_bi/customer_360.csv
exports/power_bi/marketing_funnel.csv
exports/power_bi/export_manifest.json
```

## Export manifest

Every export run creates:

```text
exports/power_bi/export_manifest.json
```

Each manifest record includes:

| Field | Meaning |
|---|---|
| `table_name` | Source PostgreSQL mart |
| `csv_file_path` | Exported CSV path |
| `row_count` | Number of exported data rows |
| `exported_at` | UTC timestamp for the export |
| `status` | Export status |

Expected healthy state:

```text
status = SUCCESS
row_count > 0
```

## Standard refresh sequence

Run the full warehouse pipeline first so the PostgreSQL marts are current.

```bash
cd ~/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics

./scripts/run_warehouse_bi_pipeline.sh
```

Then export fresh Power BI CSV datasets:

```bash
cd ~/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics

source venv/bin/activate

export PGHOST=localhost
export PGPORT=5434
export PGDATABASE=marketing_analytics
export PGUSER=project2
export PGPASSWORD="${PGPASSWORD:-project2}"

python scripts/export_power_bi_marts.py
```

Then validate the outputs:

```bash
ls -lh exports/power_bi

wc -l exports/power_bi/*.csv

python -m json.tool exports/power_bi/export_manifest.json
```

## Power BI Online refresh process

### Initial dashboard setup

1. Open Power BI Service in the browser.
2. Open the target workspace.
3. Upload the CSV files from `exports/power_bi/`.
4. Create a semantic model or report from the uploaded CSV datasets, depending on the workspace features available.
5. Confirm data types:
   - Date columns as Date
   - Revenue/spend fields as Decimal number or Currency
   - Count fields as Whole number
   - Rate fields as Decimal number or Percentage
   - ID/name fields as Text
6. Create report pages:
   - Executive Overview
   - Revenue Trends
   - Campaign Performance
   - Product Performance
   - Customer 360
   - Marketing Funnel
7. Pin key visuals to a dashboard.

### Recurring manual refresh

For the local demo flow:

1. Run or confirm the Airflow/warehouse pipeline.
2. Run `scripts/export_power_bi_marts.py`.
3. Upload or replace the CSV files in Power BI Online.
4. Refresh the report/dataset if required by the workspace flow.
5. Confirm dashboard visuals reflect the latest exported data.

## Validation commands

### Check PostgreSQL container

```bash
docker ps --filter "name=project2_postgres" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

docker exec project2_postgres pg_isready -U project2 -d marketing_analytics
```

### Check marts exist

```bash
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT
    schemaname,
    viewname
FROM pg_views
WHERE schemaname = 'marts'
  AND viewname IN (
      'mart_revenue_daily',
      'mart_campaign_performance',
      'mart_product_sales',
      'mart_customer_360',
      'mart_marketing_funnel'
  )
ORDER BY viewname;
"
```

### Check mart row counts

```bash
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT 'marts.mart_revenue_daily' AS mart_name, COUNT(*) AS row_count FROM marts.mart_revenue_daily
UNION ALL
SELECT 'marts.mart_campaign_performance', COUNT(*) FROM marts.mart_campaign_performance
UNION ALL
SELECT 'marts.mart_product_sales', COUNT(*) FROM marts.mart_product_sales
UNION ALL
SELECT 'marts.mart_customer_360', COUNT(*) FROM marts.mart_customer_360
UNION ALL
SELECT 'marts.mart_marketing_funnel', COUNT(*) FROM marts.mart_marketing_funnel
ORDER BY mart_name;
"
```

### Check CSV headers

```bash
for file in \
  exports/power_bi/revenue_daily.csv \
  exports/power_bi/campaign_performance.csv \
  exports/power_bi/product_sales.csv \
  exports/power_bi/customer_360.csv \
  exports/power_bi/marketing_funnel.csv
do
  echo "----- $file -----"
  head -n 1 "$file"
done
```

### Check CSV row counts

```bash
wc -l \
  exports/power_bi/revenue_daily.csv \
  exports/power_bi/campaign_performance.csv \
  exports/power_bi/product_sales.csv \
  exports/power_bi/customer_360.csv \
  exports/power_bi/marketing_funnel.csv
```

### Check manifest

```bash
python -m json.tool exports/power_bi/export_manifest.json
```

## Troubleshooting

### PostgreSQL container not running

Symptom:

```text
container project2_postgres is not running
```

Fix:

```bash
docker start project2_postgres

sleep 5

docker exec project2_postgres pg_isready -U project2 -d marketing_analytics
```

### Wrong PostgreSQL port

Symptom:

```text
connection refused
```

Fix:

```bash
export PGHOST=localhost
export PGPORT=5434
export PGDATABASE=marketing_analytics
export PGUSER=project2
export PGPASSWORD="${PGPASSWORD:-project2}"
```

Then retry:

```bash
python scripts/export_power_bi_marts.py
```

### Missing marts

Symptom:

```text
relation "marts.mart_revenue_daily" does not exist
```

Fix:

```bash
./scripts/run_warehouse_bi_pipeline.sh
```

Then verify the marts:

```bash
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT schemaname, viewname
FROM pg_views
WHERE schemaname = 'marts'
ORDER BY viewname;
"
```

### Empty exports

Symptom:

```text
FAILED_EMPTY
```

Fix:

1. Check source mart row counts.
2. Rebuild the warehouse/marts.
3. Re-run the export script.

```bash
./scripts/run_warehouse_bi_pipeline.sh

python scripts/export_power_bi_marts.py
```

### Missing psycopg2

Symptom:

```text
Missing dependency: psycopg2
```

Fix:

```bash
source venv/bin/activate

python -m pip install "pip>=25,<26"

python -m pip install "psycopg2-binary>=2.9,<3"
```

### Power BI CSV upload issues

Possible causes:

- CSV schema changed after the first upload.
- Date columns were inferred as text.
- Numeric fields were inferred as text.
- Browser upload used an old cached file.
- A CSV file was replaced while still being read.

Fix:

1. Re-run the export script.
2. Confirm file timestamps with `ls -lh exports/power_bi`.
3. Confirm headers with `head -n 1`.
4. Confirm row counts with `wc -l`.
5. Re-upload the CSV files.
6. Correct inferred data types in Power BI Online.

## Optional `.pbix` note

A `.pbix` file can be created later using Power BI Desktop on Windows and published to Power BI Service.

That is optional only.

The required demo path remains:

```text
PostgreSQL marts → CSV exports → Power BI Online dashboard
```
