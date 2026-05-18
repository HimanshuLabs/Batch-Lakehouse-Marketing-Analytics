# Batch Data Lakehouse for Marketing Analytics

## 1. Overview

This project is an end-to-end batch data engineering lakehouse pipeline that simulates marketing analytics data, processes it through Bronze, Silver, and Gold Medallion layers, applies data quality checks and reconciliation at every stage, publishes business-ready Gold marts to PostgreSQL, and provides reusable SQL analytics through DuckDB and PostgreSQL.

The goal is to demonstrate a production-style batch lakehouse pipeline, not a one-shot notebook.

---

## 2. Architecture

```text
Synthetic Marketing Data Generator
        ↓
Raw Layer
data/raw/batch_date=YYYY-MM-DD
        ↓
Bronze Ingestion Layer
Raw CSV/JSON → Bronze Parquet + ingestion metadata
        ↓
Bronze Quality Checks
Row count validation + metadata validation
        ↓
Silver Transformation Layer
Cleaning + type casting + deduplication + quarantine handling
        ↓
Silver Quality Checks
Bronze = Silver + Quarantine + Duplicates Dropped
        ↓
Gold Transformation Layer
Dimensions + facts + analytical marts
        ↓
Gold Quality Checks
Dim/fact/mart reconciliation + metric validation
        ↓
DuckDB Analytics Layer
SQL directly over Gold Parquet
        ↓
PostgreSQL Serving Layer
Gold marts published to PostgreSQL
        ↓
PostgreSQL Analytical Query Pack
Business-ready SQL queries
```

---

## 3. Tech Stack

| Layer | Technology |
|---|---|
| Programming language | Python 3.14.4 |
| Batch processing | PySpark 4.1.1 |
| Local analytics engine | DuckDB 1.5.2 |
| Storage format | Parquet |
| Lakehouse pattern | Bronze, Silver, Gold Medallion Architecture |
| Serving database | PostgreSQL 16 |
| Data generation | Faker, pandas |
| PostgreSQL publishing | SQLAlchemy, psycopg |
| Container runtime | Docker |
| Version control | Git |
| Development environment | VS Code |
| Operating system | Ubuntu |

---

## 4. Project Structure

```text
.
├── airflow/
│   └── dags/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── quarantine/
├── data-generator/
│   └── generate_data.py
├── dbt/
│   ├── models/
│   └── tests/
├── docs/
├── great_expectations/
├── images/
├── logs/
├── spark-batch/
│   ├── bronze_ingestion.py
│   ├── bronze_quality_checks.py
│   ├── silver_transformations.py
│   ├── silver_quality_checks.py
│   ├── gold_transformations.py
│   └── gold_quality_checks.py
├── sql/
│   ├── duckdb_gold_analytics.py
│   ├── publish_gold_to_postgres.py
│   └── postgres_analytics_queries.sql
├── terraform/
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 5. Data Sources

`data-generator/generate_data.py` generates synthetic marketing analytics data.

Generated raw files:

| File | Description |
|---|---|
| `customers.csv` | Customer profile data |
| `products.csv` | Product catalog data |
| `campaigns.csv` | Marketing campaign master data |
| `orders.csv` | Order-level transaction data |
| `order_items.csv` | Product-level order line items |
| `ad_spend.csv` | Campaign spend, impressions, and clicks |
| `web_events.json` | User behavioral event data |

The behavioral event source contains a 59-field schema including:

- user details
- product details
- session details
- recommendation features
- campaign attribution
- device and browser information
- engagement metrics
- fraud score
- event timestamps
- source metadata

---

## 6. Data Flow

### 6.1 Raw Layer

Raw data is generated under:

```text
data/raw/batch_date=YYYY-MM-DD/
```

The Raw layer stores the original generated source files exactly as received.

No cleaning is performed in Raw.

Generated files:

```text
customers.csv
products.csv
campaigns.csv
orders.csv
order_items.csv
ad_spend.csv
web_events.json
```

---

### 6.2 Bronze Layer

`spark-batch/bronze_ingestion.py` reads the Raw files and writes Bronze Parquet tables.

Output path:

```text
data/bronze/
```

Bronze keeps source fidelity and adds ingestion metadata.

Bronze metadata columns:

| Column | Meaning |
|---|---|
| `ingestion_timestamp` | Timestamp when the record entered Bronze |
| `ingestion_date` | Batch ingestion date |
| `batch_id` | Unique batch identifier |
| `source_system` | Source table name |
| `source_file_name` | Original input file path |
| `record_hash` | Hash of business columns |

Bronze tables:

| Bronze Table | Rows |
|---|---:|
| `bronze_customers` | 502 |
| `bronze_products` | 152 |
| `bronze_campaigns` | 41 |
| `bronze_orders` | 2000 |
| `bronze_order_items` | 5000 |
| `bronze_ad_spend` | 1000 |
| `bronze_web_events` | 8001 |

---

### 6.3 Bronze Quality Checks

`spark-batch/bronze_quality_checks.py` validates the Bronze layer.

Checks performed:

- Bronze table exists
- Bronze table has rows
- Raw row count equals Bronze row count
- Required metadata columns exist
- `web_events` has the expected column count

For `web_events`:

```text
59 business fields + 6 Bronze metadata fields = 65 columns
```

---

### 6.4 Silver Layer

`spark-batch/silver_transformations.py` reads Bronze data and creates clean Silver tables.

Output paths:

```text
data/silver/
data/quarantine/
```

Silver transformations include:

- type casting
- timestamp parsing
- email validation
- numeric range validation
- invalid record quarantine
- duplicate removal
- ID standardization
- Silver metadata generation

Silver output counts:

| Table | Silver Rows | Quarantine Rows |
|---|---:|---:|
| `customers` | 497 | 3 |
| `products` | 147 | 3 |
| `campaigns` | 37 | 3 |
| `orders` | 1997 | 2 |
| `order_items` | 4999 | 1 |
| `ad_spend` | 997 | 3 |
| `web_events` | 7895 | 106 |

---

### 6.5 Quarantine Layer

Invalid records are written to:

```text
data/quarantine/
```

Invalid data is not silently deleted.

Examples of quarantined records:

- invalid email
- negative age
- invalid price
- invalid discount percentage
- invalid campaign source
- broken timestamp
- invalid fraud score
- invalid device type
- invalid IP address
- invalid order amount

This keeps bad data visible and traceable.

---

### 6.6 Silver Quality Checks

`spark-batch/silver_quality_checks.py` validates the Silver layer.

Checks performed:

- Silver tables exist
- Quarantine tables exist
- Required Silver columns exist
- Key fields are not null
- Bronze rows reconcile with Silver, Quarantine, and dropped duplicates

Silver reconciliation:

| Table | Bronze | Silver | Quarantine | Duplicates Dropped | Reconciled |
|---|---:|---:|---:|---:|---:|
| `customers` | 502 | 497 | 3 | 2 | 502 |
| `products` | 152 | 147 | 3 | 2 | 152 |
| `campaigns` | 41 | 37 | 3 | 1 | 41 |
| `orders` | 2000 | 1997 | 2 | 1 | 2000 |
| `order_items` | 5000 | 4999 | 1 | 0 | 5000 |
| `ad_spend` | 1000 | 997 | 3 | 0 | 1000 |
| `web_events` | 8001 | 7895 | 106 | 0 | 8001 |

---

### 6.7 Gold Layer

`spark-batch/gold_transformations.py` reads clean Silver data and creates business-ready Gold tables.

Output path:

```text
data/gold/
```

Gold tables:

| Gold Table | Rows | Type |
|---|---:|---|
| `dim_customers` | 497 | Dimension |
| `dim_products` | 147 | Dimension |
| `dim_campaigns` | 37 | Dimension |
| `fact_orders` | 1997 | Fact |
| `fact_order_items` | 4999 | Fact |
| `fact_ad_spend` | 997 | Fact |
| `fact_web_events` | 7895 | Fact |
| `mart_campaign_performance` | 37 | Analytics mart |
| `mart_product_performance` | 147 | Analytics mart |
| `mart_customer_value` | 497 | Analytics mart |
| `mart_marketing_funnel` | 54 | Analytics mart |

---

### 6.8 Gold Quality Checks

`spark-batch/gold_quality_checks.py` validates the Gold layer.

Checks performed:

- Gold tables exist
- Gold tables have rows
- Gold dimensions reconcile with Silver dimensions
- Gold facts reconcile with Silver facts
- Campaign mart matches campaign dimension count
- Product mart matches product dimension count
- Customer mart matches customer dimension count
- Required columns exist
- Key fields are not null
- Metrics are non-negative

---

## 7. Analytics Layers

### 7.1 DuckDB Analytics

`sql/duckdb_gold_analytics.py` queries Gold Parquet files directly using DuckDB.

This proves that Gold Parquet marts are usable without loading them into a database first.

Generated reports are written to:

```text
logs/duckdb_gold_reports/
```

DuckDB analytics include:

- top campaigns by revenue
- best ROAS campaigns
- top products by revenue
- customer lifetime value leaders
- marketing funnel by channel
- device performance
- revenue by payment method
- fraud-risk orders
- category performance
- A/B test performance

---

### 7.2 PostgreSQL Serving Layer

`sql/publish_gold_to_postgres.py` publishes Gold tables into PostgreSQL.

PostgreSQL container:

```text
postgres:16
```

Database:

```text
marketing_analytics
```

Schema:

```text
gold
```

Published PostgreSQL tables:

| PostgreSQL Table | Rows |
|---|---:|
| `gold.dim_customers` | 497 |
| `gold.dim_products` | 147 |
| `gold.dim_campaigns` | 37 |
| `gold.fact_orders` | 1997 |
| `gold.fact_order_items` | 4999 |
| `gold.fact_ad_spend` | 997 |
| `gold.fact_web_events` | 7895 |
| `gold.mart_campaign_performance` | 37 |
| `gold.mart_product_performance` | 147 |
| `gold.mart_customer_value` | 497 |
| `gold.mart_marketing_funnel` | 54 |
| `gold.gold_publish_audit` | 11 |

The publish audit table records:

- table name
- row count
- publish timestamp
- PostgreSQL schema
- source path

---

### 7.3 PostgreSQL Analytical Query Pack

`sql/postgres_analytics_queries.sql` contains reusable SQL analytics.

Queries included:

- top campaigns by revenue
- best ROAS campaigns
- campaigns wasting spend
- top products by revenue
- category performance
- customer lifetime value leaders
- high-engagement low-value customers
- funnel conversion by traffic source
- device performance
- A/B test performance
- revenue by payment method
- fraud-risk order review
- daily revenue trend
- daily event trend
- prime vs non-prime customer value
- membership tier value
- publish audit

Example PostgreSQL query:

```sql
SELECT
    campaign_id,
    campaign_name,
    total_revenue,
    total_ad_spend,
    roas
FROM gold.mart_campaign_performance
ORDER BY total_revenue DESC
LIMIT 5;
```

Example result:

| campaign_id | campaign_name | total_revenue | total_ad_spend | roas |
|---:|---|---:|---:|---:|
| 38 | Instagram Campaign 38 | 10637314.85 | 1341804.62 | 7.9276 |
| 8 | Organic Campaign 8 | 9396479.70 | 1111565.80 | 8.4534 |
| 32 | Instagram Campaign 32 | 8174960.12 | 1851820.81 | 4.4146 |
| 21 | Organic Campaign 21 | 8172440.42 | 1500054.76 | 5.4481 |
| 12 | Organic Campaign 12 | 7884326.68 | 710277.05 | 11.1004 |

---

## 8. Partitioning Strategy

| Layer | Partition Column |
|---|---|
| Bronze | `ingestion_date` |
| Silver | `processed_date` |
| Gold | `gold_processed_date` |

This provides batch-level traceability and efficient date-based reads.

---

## 9. Data Quality Strategy

This project uses quality gates at every major stage.

| Layer | Quality Strategy |
|---|---|
| Bronze | File existence, row count reconciliation, metadata validation |
| Silver | Type casting, validation, deduplication, quarantine, reconciliation |
| Gold | Dim/fact/mart validation, metric validation, not-null checks |
| PostgreSQL | Publish audit and analytical SQL verification |

The pipeline is designed to fail loudly when quality checks fail.

---

## 10. How to Run

### 10.1 Open Project in VS Code

```bash
cd ~/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics
code .
```

---

### 10.2 Create Virtual Environment

```bash
python3 -m venv venv
```

---

### 10.3 Activate Virtual Environment

```bash
source venv/bin/activate
```

---

### 10.4 Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 10.5 Generate Raw Data

```bash
python data-generator/generate_data.py
```

---

### 10.6 Run Bronze Ingestion

```bash
python spark-batch/bronze_ingestion.py
```

---

### 10.7 Run Bronze Quality Checks

```bash
python spark-batch/bronze_quality_checks.py
```

---

### 10.8 Run Silver Transformations

```bash
python spark-batch/silver_transformations.py
```

---

### 10.9 Run Silver Quality Checks

```bash
python spark-batch/silver_quality_checks.py
```

---

### 10.10 Run Gold Transformations

```bash
python spark-batch/gold_transformations.py
```

---

### 10.11 Run Gold Quality Checks

```bash
python spark-batch/gold_quality_checks.py
```

---

### 10.12 Run DuckDB Analytics

```bash
python sql/duckdb_gold_analytics.py
```

---

### 10.13 Start PostgreSQL

```bash
docker rm -f project2_postgres || true

docker run --name project2_postgres \
  -e POSTGRES_USER=project2 \
  -e POSTGRES_PASSWORD=project2 \
  -e POSTGRES_DB=marketing_analytics \
  -p 5434:5432 \
  -d postgres:16
```

---

### 10.14 Publish Gold Tables to PostgreSQL

```bash
python sql/publish_gold_to_postgres.py
```

---

### 10.15 Run PostgreSQL Analytical Queries

```bash
docker exec -i project2_postgres psql -U project2 -d marketing_analytics < sql/postgres_analytics_queries.sql
```

---

## 11. Key Commands for Verification

### Check Git History

```bash
git log --oneline
```

### Check Project Status

```bash
git status
```

### Check PostgreSQL Container

```bash
docker ps
```

### Open PostgreSQL Shell

```bash
docker exec -it project2_postgres psql -U project2 -d marketing_analytics
```

### List Gold Tables in PostgreSQL

```sql
\dt gold.*
```

### Query Publish Audit

```sql
SELECT
    table_name,
    row_count,
    published_at
FROM gold.gold_publish_audit
ORDER BY table_name;
```

---

## 12. Git Commit History

```text
ecef405 Add PostgreSQL analytical query pack
ade1c08 Publish gold marts to PostgreSQL
3552b3c Add DuckDB SQL analytics over gold marts
c27e2d9 Add gold quality checks and mart reconciliation
0d5b07a Build gold analytics marts
6b10d49 Add silver quality checks and reconciliation
16e7240 Build silver transformation layer with quarantine handling
a04b80e Add bronze quality checks
a9b8c11 Build bronze ingestion layer with metadata
ec2e2f8 Add synthetic marketing analytics data generator
f4dbc2a Lock Python dependency versions
74fb0a9 Add Python dependencies for batch lakehouse
60c6a2f Initialize batch lakehouse project structure
```

---
