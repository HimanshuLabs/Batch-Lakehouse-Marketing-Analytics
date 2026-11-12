# Marketing Lakehouse & Customer Warehouse Reporting Platform

## 1. Overview

End-to-end marketing analytics platform combining lakehouse processing, PostgreSQL warehouse modeling, dbt analytics engineering, Airflow orchestration, Power BI Online reporting, Terraform-backed BI exports, and GitHub Actions CI/CD validation.

This repository started as a batch marketing lakehouse and has been extended into one merged production-style analytics platform. The lakehouse engine remains the foundation: it generates synthetic marketing/customer data, processes it through Raw, Bronze, Silver, and Gold layers, applies quality checks, handles quarantined records, and produces business-ready marts. The warehouse/reporting layer then promotes trusted Gold outputs into PostgreSQL schemas, models SCD Type 2 dimensions and fact tables, validates point-in-time correctness, builds BI-ready marts, exports dashboard datasets, and documents Power BI Online proof assets.

The result is one coherent data product:

```text
Lakehouse engine → PostgreSQL warehouse → dbt validation → Airflow orchestration → Power BI Online reporting → GitHub Actions quality gate
```

All data is synthetic.

---

## 2. What This Platform Proves

This project demonstrates practical Data Engineering and Analytics Engineering capabilities across the full analytics lifecycle:

| Capability | What this repo proves |
|---|---|
| Lakehouse processing | Raw, Bronze, Silver, and Gold Medallion layers using PySpark and Parquet |
| Data quality | Row-count checks, metadata checks, null checks, metric checks, quarantine handling, row conservation, and reconciliation |
| Warehouse modeling | PostgreSQL staging, warehouse, marts, and audit schemas |
| Historical correctness | SCD Type 2 customer/product/campaign dimensions and point-in-time fact joins |
| Reproducibility | Deterministic, incremental batches; reruns of a batch leave the lake and the warehouse unchanged |
| BI readiness | Revenue, campaign, product, customer, and funnel marts built for reporting |
| Analytics engineering | 21 dbt models and 67 dbt tests, docs, lineage, and exposures |
| Orchestration | One Airflow 3 DAG of 23 tasks covering lakehouse, warehouse, dbt, and reporting |
| Reporting | Power BI Online dashboard assets, screenshots, CSV exports, and refresh flow documentation |
| Infrastructure | Terraform for the S3 layer that serves the Power BI CSVs |
| CI/CD | GitHub Actions: static checks, unit tests in two time zones, and a full end-to-end integration run |

---

## 3. End-to-End Architecture

```text
Synthetic Marketing + Customer Data
        ↓
Raw Lakehouse Layer
        ↓
Bronze Lakehouse Layer
        ↓
Bronze Quality Checks
        ↓
Silver Lakehouse Layer
        ↓
Silver Quality Checks + Quarantine
        ↓
Gold Lakehouse Layer
        ↓
Gold Quality Checks
        ↓
SCD2 Historical Modeling Layer
        ↓
Point-in-Time Fact Join Layer
        ↓
PostgreSQL Warehouse
        ↓
dbt Analytics Engineering Layer
        ↓
PostgreSQL BI Marts
        ↓
CSV Exports / S3 HTTPS Route
        ↓
Power BI Online Reports and Dashboard
        ↓
GitHub Actions CI/CD Validation
```

Project 2 remains the lakehouse engine. The former Project 3 scope now lives inside the same repository as the PostgreSQL warehouse, dbt, BI, reconciliation, and reporting layer.

---

## 4. Mermaid Architecture Diagram

```mermaid
flowchart TD
    A["Synthetic Marketing + Customer Data<br/>customers, products, campaigns, orders,<br/>order_items, ad_spend, web_events"] --> B["Raw Layer<br/>data/raw/batch_date=YYYY-MM-DD"]
    B --> C["Bronze Layer<br/>Parquet + ingestion metadata"]
    C --> D["Bronze Quality Checks<br/>row counts + metadata validation"]

    D --> E["Silver Layer<br/>cleaning, typing, dedupe, quarantine"]
    E --> F["Silver Quality Checks<br/>Bronze = Silver + Quarantine + Dropped Duplicates"]

    F --> G["Gold Lakehouse Layer<br/>dimensions, facts, analytical marts"]
    G --> H["Gold Quality Checks<br/>dim/fact/mart reconciliation"]

    H --> I["SCD2 Dimensions<br/>customer, product, campaign history"]
    I --> J["Point-in-Time Fact Joins<br/>facts join to correct historical versions"]

    J --> K["PostgreSQL Staging Schema<br/>loaded Gold/SCD2 extracts"]
    K --> L["PostgreSQL Warehouse Schema<br/>star schema facts + dimensions"]
    L --> M["PostgreSQL Marts Schema<br/>BI-ready reporting marts"]
    L --> N["PostgreSQL Audit Schema<br/>reconciliation + validation"]

    M --> O["dbt Layer<br/>models, tests, docs, lineage, exposures"]
    M --> P["Route A exports<br/>exports/power_bi/*.csv"]
    J --> P2["Route B exports<br/>data/bi_exports/*.csv"]
    P2 --> Q["S3 public HTTPS URLs<br/>five CSVs"]
    P --> R["Power BI Online<br/>semantic model, reports, dashboard"]
    Q --> R

    S["Airflow DAG"] --> C
    S --> E
    S --> G
    S --> K
    S --> L
    S --> O
    S --> P2

    T["GitHub Actions CI/CD"] --> C
    T --> L
    T --> O
    T --> S
    T --> P
    T --> U["Terraform BI Export Infra"]
```

---

## 5. What Each Layer Owns

| Layer | Path / System | Ownership |
|---|---|---|
| Synthetic source | `data-generator/generate_data.py` | Generates marketing, customer, campaign, order, product, ad spend, and web event data |
| Raw | `data/raw/` | Stores original generated files without cleaning |
| Bronze | `data/bronze/` | Preserves source fidelity and adds ingestion metadata |
| Silver | `data/silver/` | Cleans, standardizes, deduplicates, validates, and prepares trusted records |
| Quarantine | `data/quarantine/` | Stores invalid records instead of silently dropping them |
| Gold | `data/gold/` | Produces business-ready facts, dimensions, and lakehouse marts |
| SCD2 bridge | Gold/SCD2 outputs | Preserves customer/product/campaign history for warehouse joins |
| PostgreSQL staging | `staging` schema | Loads trusted Gold/SCD2 extracts into relational staging |
| PostgreSQL warehouse | `warehouse` schema | Stores star-schema facts and SCD2 dimensions |
| PostgreSQL marts | `marts` schema | Provides BI-ready reporting objects |
| PostgreSQL audit | `audit` schema | Stores reconciliation and validation outputs |
| dbt | `dbt/` | Adds analytics engineering models, tests, docs, lineage, and exposures |
| Airflow | `airflow/dags/` | Orchestrates lakehouse, warehouse, dbt, and export tasks |
| Power BI exports | `exports/power_bi/` (route A), `data/bi_exports/` (route B) | CSV datasets consumed by Power BI Online |
| Power BI screenshots | `screenshots/power_bi/`, `docs/screenshots/power_bi/` | Provides dashboard proof assets |
| Terraform | `terraform/aws-bi-exports/` | Manages the S3 layer that serves the route B CSVs |
| CI/CD | `.github/workflows/ci.yml` | Validates the merged analytics platform before merge |

---

## 6. Tech Stack

| Area | Technology |
|---|---|
| Language | Python 3.14 (pipeline), Python 3.13 (dbt) |
| Batch processing | PySpark 4.1.1 on Java 17 |
| Storage format | Parquet |
| Lakehouse pattern | Raw, Bronze, Silver, Gold Medallion Architecture |
| Local analytics | DuckDB 1.5.2, pandas 3.0.3 |
| Warehouse database | PostgreSQL 16.14 |
| Analytics engineering | dbt-core 1.10.15, dbt-postgres 1.10.0 |
| Orchestration | Apache Airflow 3.2.0 |
| BI/reporting | Power BI Online |
| BI exports | CSV datasets; five of them served from S3 over public HTTPS URLs |
| Infrastructure | Terraform (CI validates with 1.17.1) |
| CI/CD | GitHub Actions |
| Container runtime | Docker |
| Development environment | Ubuntu, VS Code |

Exact Python pins are in `requirements.txt` (runtime) and `requirements-dev.txt` (pytest, ruff, dbt).

---

## 7. Data Sources

The synthetic generator creates marketing and customer behavior data.

Generated raw files:

| File | Description |
|---|---|
| `customers.csv` | Customer profile data |
| `products.csv` | Product catalog data |
| `campaigns.csv` | Marketing campaign master data |
| `orders.csv` | Order-level transaction data |
| `order_items.csv` | Product-level order line items |
| `ad_spend.csv` | Campaign spend, impressions, and clicks |
| `web_events.json` | User behavior and engagement events |

The behavioral event source includes customer, product, session, recommendation, campaign attribution, device/browser, engagement, fraud, schema version, and timestamp fields.

---

## 8. Lakehouse Engine

The lakehouse engine processes generated source data through Medallion layers.

### Batches and BATCH_DATE

Every run processes one batch, named by `BATCH_DATE` (`YYYY-MM-DD`). Each job reads `BATCH_DATE` from the environment and falls back to today's date when it is unset, so always set it explicitly. The Airflow DAG sets it to the India (Asia/Kolkata) date of the run's logical date, or of its `run_after` time when the run has no logical date. The batch day is the business day: a run at 00:30 IST processes the day that has just started.

The generator is incremental and deterministic:

- `2026-05-18` is the anchor batch. It holds the base master data and a 120-day history of facts.
- Each later batch adds a 14-day increment of facts plus a few changed customers, products, and campaigns. These changes drive the SCD2 history.
- All randomness, UUIDs included, is seeded from fixed seeds and `BATCH_DATE`. Rerunning a date produces the same files.
- Fact IDs are unique across batches.
- Files are written to a temporary folder and renamed into place. If the seven raw files for a date already exist, the DAG's `generate_data` task skips generation.

The local data in this project covers batches `2026-05-18`, `2026-06-04`, and `2026-06-18`.

### Raw

```text
data/raw/batch_date=YYYY-MM-DD/
```

Raw stores the original generated source files exactly as received.

### Bronze

```text
data/bronze/
```

Bronze converts raw files into Parquet, partitioned per batch, and adds ingestion metadata such as ingestion timestamp, ingestion date, batch ID, source system, source file name, and record hash.

### Silver

```text
data/silver/
data/quarantine/
```

Silver performs cleaning, type casting, timestamp parsing, validation, deduplication, and quarantine handling. Customer, product, and campaign changes are kept in Silver history tables, which feed SCD2.

Invalid records are written to quarantine instead of being silently removed. Every Bronze row is accounted for: Bronze = Silver + quarantine + dropped duplicates.

### Gold

```text
data/gold/
```

Gold creates business-ready facts, dimensions, and analytical marts.

Gold outputs include:

```text
dim_customers                    dim_customers_scd2
dim_products                     dim_products_scd2
dim_campaigns                    dim_campaigns_scd2
fact_orders                      fact_orders_scd2
fact_order_items                 fact_order_items_scd2
fact_ad_spend                    fact_campaign_spend_scd2
fact_web_events
mart_campaign_performance        mart_campaign_performance_scd2
mart_product_performance         mart_product_performance_scd2
mart_customer_value              mart_customer_lifetime_value_scd2
mart_marketing_funnel            mart_marketing_funnel_scd2
```

The `_scd2` facts join each row to the dimension version that was valid at the event time. A row that has no such version is rejected to `data/quarantine/` rather than joined to the current version. The point-in-time report checks that Silver rows = valid rows + rejected rows.

---

## 9. Warehouse Layer

The warehouse layer promotes trusted Gold/SCD2 outputs into PostgreSQL.

Main schemas:

| Schema | Purpose |
|---|---|
| `staging` | Loaded Gold/SCD2 extracts from the lakehouse |
| `warehouse` | Star-schema dimensions and facts |
| `marts` | BI-ready reporting marts |
| `audit` | Reconciliation and validation checks |

Warehouse capabilities:

- SCD Type 2 customer, product, and campaign dimensions.
- Surrogate keys for historical dimension versions. Dimension keys are deterministic: a rebuild from the same data gives the same keys in any load order.
- Point-in-time fact joins that only accept a version whose validity range covers the event.
- Unknown-member handling where needed.
- BI-ready marts for revenue, campaign, product, customer, and funnel reporting.
- Reconciliation checks stored under the audit layer.
- Query performance tuning through indexes and `EXPLAIN ANALYZE` documentation.

Main SQL assets:

```text
sql/warehouse/create_schemas.sql
sql/warehouse/load_gold_to_staging.sql
sql/warehouse/create_warehouse_tables.sql
sql/warehouse/create_marts.sql
sql/warehouse/create_indexes.sql
sql/warehouse/reconciliation_report.sql
sql/warehouse/scd2_validation_queries.sql
sql/warehouse/performance_explain_analyze.sql
sql/warehouse/dashboard_query_pack.sql
```

Warehouse documentation:

```text
docs/warehouse/merged_architecture.md
docs/warehouse/warehouse_data_model.md
docs/warehouse/scd2_design.md
docs/warehouse/reconciliation.md
docs/warehouse/performance_tuning.md
docs/warehouse/runbook.md
```

---

## 10. dbt Analytics Engineering Layer

The dbt layer organizes warehouse models, marts, tests, docs, lineage, and dashboard exposures: 21 models and 67 tests.

Main dbt areas:

```text
dbt/models/staging/      7 models
dbt/models/warehouse/    8 models
dbt/models/marts/        5 models
dbt/models/audit/        1 model
dbt/models/exposures.yml
dbt/models/schema.yml
```

dbt validates:

- not-null constraints,
- uniqueness constraints,
- relationships,
- accepted values,
- mart-level business logic,
- dashboard-facing exposures.

dbt runs from `venv-dbt` (see section 15), from the repository root:

```bash
cp dbt/profiles.yml.example dbt/profiles.yml   # git-ignored; reads WAREHOUSE_DB_* with local defaults

cd dbt
../venv-dbt/bin/dbt debug --profiles-dir .
../venv-dbt/bin/dbt parse --profiles-dir . --no-partial-parse
../venv-dbt/bin/dbt run --profiles-dir .              # PASS=21
../venv-dbt/bin/dbt test --profiles-dir .             # PASS=65
../venv-dbt/bin/dbt docs generate --profiles-dir .
cd ..
```

Keep `dbt/profiles.yml` in place: the DAG's dbt tasks need it. `dbt/target/`, `dbt/logs/`, and `dbt/profiles.yml` are git-ignored.

---

## 11. Airflow Orchestration Layer

Airflow coordinates the platform execution flow. One DAG, `batch_lakehouse_marketing_analytics` in `airflow/dags/batch_lakehouse_pipeline.py`, runs 23 `BashOperator` tasks through `scripts/run_airflow_task.sh`:

```text
generate_data → bronze_ingestion → bronze_quality_checks → silver_transformations →
silver_quality_checks → gold_transformations → gold_quality_checks → duckdb_gold_analytics →
publish_gold_to_postgres → scd2_dimensions → point_in_time_fact_joins → scd2_gold_marts →
scd2_duckdb_analytics → create_postgresql_schemas → load_gold_scd2_to_staging →
build_warehouse_tables → run_scd2_validation → build_reporting_marts →
run_warehouse_reconciliation → dbt_run → dbt_test → dbt_docs → power_bi_export
```

The DAG has no schedule and allows one active run. Its tasks use `venv/bin/python` and `venv-dbt/bin/dbt`, and each task gets `BATCH_DATE = {{ (dag_run.logical_date or dag_run.run_after).astimezone(macros.dateutil.tz.gettz('Asia/Kolkata')) | ds }}`: the India date of the run. Every task also runs under a run lock (`scripts/pipeline_lock.py`, lock file `logs/pipeline_run.lock`): a task of a second run started by any route fails at once with exit code 75 until the first run finishes or fails; `python scripts/pipeline_lock.py --release --force` clears the lock of a run that died.

**Trigger from the UI.** Set the logical date to the day to process or rerun, at 12:00 on that day, or leave it at now to process today. The CLI's `-l` works the same way. Section 15 also has a scripted REST trigger with a render gate that shows the date before any task runs.

The full setup, verified with a 23-of-23 successful run, is in:

```text
docs/warehouse/airflow_working_state.md
```

---

## 12. Power BI Online Reporting Layer

The reporting layer turns warehouse and Gold marts into Power BI Online assets. There are two export routes:

| Route | Script | Source | Output | How Power BI Online gets it |
|---|---|---|---|---|
| A. Warehouse marts | `scripts/export_power_bi_marts.py` (run by hand) | PostgreSQL `marts.*` | `exports/power_bi/*.csv` and `export_manifest.json` (committed) | upload the files in Power BI Online |
| B. Gold SCD2 marts | `sql/export_power_bi_dataset.py` (the DAG's `power_bi_export` task) | Gold SCD2 marts and facts, plus verified pipeline checks | `data/bi_exports/*.csv` (git-ignored) | five files uploaded to S3; Power BI reads them through public HTTPS URLs |

Route A exported datasets:

```text
exports/power_bi/campaign_performance.csv
exports/power_bi/customer_360.csv
exports/power_bi/marketing_funnel.csv
exports/power_bi/product_sales.csv
exports/power_bi/revenue_daily.csv
exports/power_bi/export_manifest.json
```

**Route B and S3.** Power BI Service needed stable file URLs, and the available Microsoft account had no OneDrive for Business upload. So five small synthetic CSVs are hosted in Amazon S3:

- `campaign_performance.csv`
- `customer_lifetime_value.csv`
- `product_performance.csv`
- `marketing_funnel.csv`
- `data_quality_summary.csv`

`terraform/aws-bi-exports` manages this layer:

- the bucket itself is created outside Terraform and looked up by name;
- the module sets ownership controls, SSE-S3 encryption, and the public access block settings;
- the bucket policy grants public `s3:GetObject` on exactly those five objects, with no listing and no write;
- the module uploads the five files from `data/bi_exports/`.

`terraform output power_bi_csv_urls` prints the HTTPS URLs that Power BI uses. `data_quality_summary.csv` is built only from verified checks: warehouse reconciliation, SCD2 validation, Silver quarantine, point-in-time conservation, and the web events the warehouse rejects. See `docs/power_bi_dashboard_spec.md` and `docs/terraform_aws_bi_exports.md`.

Power BI docs:

```text
docs/warehouse/bi_reporting_layer.md
docs/warehouse/power_bi_dashboard.md
docs/warehouse/power_bi_data_dictionary.md
docs/warehouse/power_bi_refresh_flow.md
```

Dashboard pages:

| Page | Purpose |
|---|---|
| Executive Overview | Top-level revenue, customer, campaign, and funnel health |
| Revenue Trends | Daily revenue and sales movement |
| Campaign Performance | Campaign revenue, ROAS, spend, impressions, and clicks |
| Product Performance | Product/category sales and performance |
| Customer 360 | Customer value, segmentation, and lifecycle reporting |
| Marketing Funnel | Funnel conversion and behavior analytics |

---

## 13. CI/CD Quality Gate

GitHub Actions (`.github/workflows/ci.yml`) runs on pushes and pull requests to `main`. Actions are pinned by commit SHA.

| Job | What it checks |
|---|---|
| Static checks | required files; no committed local paths, private keys, AWS keys, or dbt credentials; Python compile; shell syntax; warehouse SQL assets; Airflow DAG source; ruff; dbt project structure and `dbt parse` |
| Unit tests | `pytest tests/unit` on Python 3.14 with Java 17, run twice: once with `TZ=UTC` and once with `TZ=Asia/Kolkata` |
| Integration | the whole pipeline end to end against PostgreSQL 16.14; see below |
| Power BI artifact validation | `scripts/validate_powerbi_assets.py`: exports, manifest, docs, screenshots |
| Terraform validation | Terraform 1.17.1: `fmt -check`, `init -backend=false`, `validate`, and no tracked state or tfvars |

The integration job (`tests/integration/run_integration.sh`):

1. runs every DAG task in DAG order, including `dbt run` and `dbt test`, for `BATCH_DATE=2026-05-18` and then `2026-06-18`;
2. asserts that all quality reports pass, that every Silver row is conserved, that SCD2 validation and reconciliation are all PASS, and that the counts equal `tests/integration/expected_counts.json`;
3. reruns `2026-06-18` and requires the lake, the warehouse, and the marts to be identical (determinism);
4. rebuilds the warehouse and checks that load order does not change the results (`tests/integration/rebuild_warehouse.sh`).

Run the integration test locally without touching `data/`, `logs/`, or `project2_postgres`:

```bash
bash tests/integration/run_local.sh            # add --keep to keep the work folder
```

It runs a copy of the working tree under `/tmp` against a throwaway PostgreSQL 16.14 container, `p2it_postgres_<pid>` on port 5440. It removes only that container afterwards. Locally it skips the three dbt tasks; CI runs them.

Run the unit tests locally the way CI does:

```bash
TZ=UTC venv/bin/python -m pytest tests/unit -p no:cacheprovider
TZ=Asia/Kolkata venv/bin/python -m pytest tests/unit -p no:cacheprovider
```

---

## 14. Proof and Demo Assets

Proof assets are committed so the project can be reviewed without rerunning the full platform.

Power BI screenshots:

```text
screenshots/power_bi/01_final_dashboard.png
screenshots/power_bi/02_revenue_trends_report.png
screenshots/power_bi/03_campaign_performance_report.png
screenshots/power_bi/04_product_performance_report.png
screenshots/power_bi/05_customer_360_report.png
screenshots/power_bi/06_marketing_funnel_report.png
```

Additional dashboard screenshots:

```text
docs/screenshots/power_bi/01_executive_overview.png
docs/screenshots/power_bi/02_campaign_performance.png
docs/screenshots/power_bi/03_data_quality_reconciliation.png
```

The screenshots were taken before the latest data fixes, so their numbers differ from the current exports (see Known Limitations).

Export proof:

```text
exports/power_bi/*.csv
exports/power_bi/export_manifest.json
```

Operational proof docs:

```text
docs/warehouse/airflow_working_state.md
docs/warehouse/reconciliation.md
docs/warehouse/scd2_design.md
docs/warehouse/bi_reporting_layer.md
```

---

## 15. Environments

The project uses three Python environments. All are git-ignored and sit at the repository root, because the DAG refers to them by these paths. Create each one under its final name: a renamed venv keeps the old path in its scripts.

### `venv`: pipeline (Python 3.14)

```bash
python3.14 -m venv venv
venv/bin/python -m pip install -r requirements.txt
venv/bin/python -m pip install "$(grep -E '^pytest==' requirements-dev.txt)"   # for the tests
cp .env.example .env                                                         # git-ignored
```

Spark needs Java 17 (`java -version`).

### `venv-dbt`: dbt (Python 3.13 via uv)

dbt-core 1.10 does not run on Python 3.14, so dbt gets its own Python 3.13 environment. `UV_NO_MODIFY_PATH=1` installs uv to `~/.local/bin` without editing shell startup files.

```bash
curl -LsSf https://astral.sh/uv/install.sh | env UV_NO_MODIFY_PATH=1 sh
~/.local/bin/uv python install 3.13
~/.local/bin/uv venv --python 3.13 --seed venv-dbt
venv-dbt/bin/python -m pip install -r requirements-dev.txt
venv-dbt/bin/dbt --version          # dbt-core 1.10.15, dbt-postgres 1.10.0

cp dbt/profiles.yml.example dbt/profiles.yml
cd dbt
../venv-dbt/bin/dbt debug --profiles-dir .
../venv-dbt/bin/dbt parse --profiles-dir . --no-partial-parse
../venv-dbt/bin/dbt run --profiles-dir .
../venv-dbt/bin/dbt test --profiles-dir .
../venv-dbt/bin/dbt docs generate --profiles-dir .
cd ..
```

### `venv-airflow`: Airflow 3.2.0 (Python 3.14)

```bash
python3.14 -m venv venv-airflow
venv-airflow/bin/python -m pip install --upgrade pip
venv-airflow/bin/python -m pip install "apache-airflow[postgres]==3.2.0" apache-airflow-providers-standard \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-3.14.txt
venv-airflow/bin/airflow version    # 3.2.0
```

**Metadata database.** It lives in a separate container, `airflow_postgres`. It never shares `project2_postgres`.

```bash
source scripts/airflow_local_env.sh          # create it first (below)
docker volume create airflow_pgdata
docker run -d --name airflow_postgres --restart unless-stopped \
  -p 127.0.0.1:5435:5432 -v airflow_pgdata:/var/lib/postgresql/data \
  -e POSTGRES_DB=airflow_metadata -e POSTGRES_USER=airflow \
  -e POSTGRES_PASSWORD="$AIRFLOW_PG_PASSWORD" postgres:16.14
```

**Environment file.** Create `scripts/airflow_local_env.sh`; it is git-ignored. Replace each `<SECRET>` with `openssl rand -hex 16` for the password and `openssl rand -hex 32` for the two keys.

```bash
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export AIRFLOW_PG_PASSWORD='<SECRET>'
export AIRFLOW_HOME="${REPO_ROOT}/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="${REPO_ROOT}/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:${AIRFLOW_PG_PASSWORD}@127.0.0.1:5435/airflow_metadata"
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_PRE_PING=True
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://127.0.0.1:8080/execution/
export AIRFLOW__API__BASE_URL=http://127.0.0.1:8080
export AIRFLOW__API_AUTH__JWT_SECRET='<SECRET>'
export AIRFLOW__API__SECRET_KEY='<SECRET>'
export PATH="${REPO_ROOT}/venv-airflow/bin:${PATH}"
```

LocalExecutor with parallelism 1 runs one task at a time, so Spark jobs never overlap.

**Before any DAG run**, `dbt/profiles.yml` must exist (`cp dbt/profiles.yml.example dbt/profiles.yml`). Without it, `dbt_run` fails with `Could not find profile named 'marketing_lakehouse_warehouse'`.

**Initialise once, then start the three processes:**

```bash
source scripts/airflow_local_env.sh
airflow db migrate
airflow dags list-import-errors

mkdir -p "$AIRFLOW_HOME/run"
nohup airflow dag-processor > "$AIRFLOW_HOME/run/dag-processor.log" 2>&1 < /dev/null &
nohup airflow scheduler     > "$AIRFLOW_HOME/run/scheduler.log"     2>&1 < /dev/null &
nohup airflow api-server --host 127.0.0.1 --port 8080 > "$AIRFLOW_HOME/run/api-server.log" 2>&1 < /dev/null &
until curl -sf http://127.0.0.1:8080/api/v2/monitor/health >/dev/null; do sleep 2; done

# Record the real PIDs from ps; the api-server renames itself "airflow api_server ..."
ps -eo pid,args | awk '$2 ~ /venv-airflow\/bin\/python$/ && $3 ~ /venv-airflow\/bin\/airflow$/ && ($4=="scheduler" || $4=="dag-processor") {print $1 > ENVIRON["AIRFLOW_HOME"]"/run/"$4".pid"}'
ss -ltnp | grep ':8080 ' | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2 > "$AIRFLOW_HOME/run/api-server.pid"
```

**Trigger from the UI.** Open the DAG, choose Trigger, and set **Logical Date** to the day you want to process or rerun, at 12:00 on that day. Noon is the same day in India, whose date `BATCH_DATE` uses, whichever timezone the UI shows. To process today, leave the field at its default, now: that is today in India, also between 00:00 and 05:30 IST. The CLI works the same way: `airflow dags trigger batch_lakehouse_marketing_analytics -l 2026-06-18T12:00:00+00:00`, or no `-l` for today. Each logical date can have one run. To rerun a day that already has one, clear that run in the UI or use another time on the same day.

**Scripted option: REST trigger and render gate.** With the DAG paused, trigger through the REST API. For the first run of a date, set `logical_date`. For a rerun of a date that already has a run, send `"logical_date": null` with `run_after` set: without a logical date, `BATCH_DATE` comes from `run_after`.

```bash
TOKEN=$(curl -s http://127.0.0.1:8080/auth/token | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
RUN_ID=manual__2026-06-18
curl -s -X POST http://127.0.0.1:8080/api/v2/dags/batch_lakehouse_marketing_analytics/dagRuns \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"dag_run_id\":\"$RUN_ID\",\"logical_date\":\"2026-06-18T12:00:00Z\",\"run_after\":\"2026-06-18T12:00:00Z\",\"conf\":{}}"
# rerun of the same date: new RUN_ID and "logical_date":null
```

**Render gate.** With the DAG still paused, both lines must print exactly `'BATCH_DATE': '2026-06-18'`. Unpause only then:

```bash
for t in generate_data power_bi_export; do
  airflow tasks render batch_lakehouse_marketing_analytics $t "$RUN_ID" 2>/dev/null | grep -o "'BATCH_DATE': '[^']*'"
done
airflow dags unpause batch_lakehouse_marketing_analytics
```

If either line prints anything else, keep the DAG paused and delete the run with `curl -X DELETE` on `.../dagRuns/$RUN_ID`.

**Stop** with SIGTERM to the recorded PIDs:

```bash
airflow dags pause batch_lakehouse_marketing_analytics
for n in api-server scheduler dag-processor; do kill -TERM "$(cat "$AIRFLOW_HOME/run/$n.pid")"; done
```

Details and troubleshooting are in `docs/warehouse/airflow_working_state.md`.

---

## 16. PostgreSQL

The warehouse runs in the `project2_postgres` container, which holds the `marketing_analytics` database on host port 5434. Create it once, with a named volume so the data survives restarts:

```bash
docker volume create project2_pgdata
docker run -d --name project2_postgres --restart unless-stopped \
  -e POSTGRES_USER=project2 \
  -e POSTGRES_PASSWORD=project2 \
  -e POSTGRES_DB=marketing_analytics \
  -v project2_pgdata:/var/lib/postgresql/data \
  -p 5434:5432 \
  postgres:16.14
docker exec project2_postgres pg_isready -U project2 -d marketing_analytics
```

If the container is stopped, start it again; never recreate it:

```bash
docker start project2_postgres
```

> **Warning:** removing or recreating `project2_postgres` (for example with `docker rm -f`) deletes the warehouse. If the container uses an anonymous volume, the data is lost. Back up first:
>
> ```bash
> docker exec project2_postgres pg_dump -U project2 -d marketing_analytics -Fc > marketing_analytics.dump
> ```

The `project2` password is a local development default; it is also the default in `.env.example` and `dbt/profiles.yml.example`.

**Read-only access.** Consumers (BI tools, Project 4, Project 5) connect as `p2_reader`, not as the owner. The role is created by `sql/warehouse/create_schemas.sql` on every run. It can `SELECT` from the `warehouse`, `marts` and `audit` schemas and from the dbt schemas (`dbt_staging`, `dbt_warehouse`, `dbt_marts`, `dbt_audit`); it cannot read `staging` and cannot write. Its password comes from `P2_READER_PASSWORD` in `.env` (the local default in `.env.example` is `p2_reader`); without one the role exists but cannot log in.

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5434` |
| Database | `marketing_analytics` |
| User | `p2_reader` |
| Password | the value of `P2_READER_PASSWORD` in `.env` |

```bash
PGPASSWORD=<SECRET> psql -h localhost -p 5434 -U p2_reader -d marketing_analytics -c "SELECT count(*) FROM marts.mart_revenue_daily;"
```

---

## 17. Local Runbook

All commands run from the repository root, with the environments from section 15 and `project2_postgres` running.

### Choose the batch

```bash
export BATCH_DATE=2026-06-18
```

### Option 1: Airflow

Trigger the DAG from the UI with the logical date set to the batch date, or use the scripted REST trigger and render gate in section 15.

### Option 2: the DAG's tasks without Airflow

`tests/integration/run_dag.py` loads the real DAG file and runs its tasks in DAG order. Each task gets exactly the DAG's environment, with `BATCH_DATE` rendered:

```bash
venv/bin/python tests/integration/run_dag.py --batch-date 2026-06-18 --log-dir /tmp/p2-runs/0618 --dry-run   # list the tasks
venv/bin/python tests/integration/run_dag.py --batch-date 2026-06-18 --log-dir /tmp/p2-runs/0618
```

### Option 3: task by task

These are the same commands the DAG runs, in DAG order:

```bash
for task in generate_data bronze_ingestion bronze_quality_checks silver_transformations \
    silver_quality_checks gold_transformations gold_quality_checks duckdb_gold_analytics \
    publish_gold_to_postgres scd2_dimensions point_in_time_fact_joins scd2_gold_marts \
    scd2_duckdb_analytics create_postgresql_schemas load_gold_scd2_to_staging \
    build_warehouse_tables run_scd2_validation build_reporting_marts \
    run_warehouse_reconciliation dbt_run dbt_test dbt_docs power_bi_export; do
  bash scripts/run_airflow_task.sh "$task" || break
done
```

Rerunning a batch date is safe. Generation is skipped when the raw files exist, and the lake and the warehouse come out identical.

### Export route A Power BI datasets

```bash
PGHOST=localhost PGPORT=5434 PGDATABASE=marketing_analytics PGUSER=project2 PGPASSWORD=project2 \
  venv/bin/python scripts/export_power_bi_marts.py
venv/bin/python scripts/validate_powerbi_assets.py
```

### Upload the route B files to S3

The run's `power_bi_export` task writes the five files to `data/bi_exports/`. To upload them, run `terraform plan` and `terraform apply` in `terraform/aws-bi-exports`, with a `terraform.tfvars` based on `terraform.tfvars.example`.

---

## 18. Validation Commands

### Git state

```bash
git branch --show-current
git status -sb
git log --oneline --decorate -10
```

### Required docs and files

```bash
bash scripts/validate_required_files.sh
```

### Warehouse reconciliation

```bash
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT status, COUNT(*) AS check_count
FROM audit.v_latest_reconciliation_report
GROUP BY status
ORDER BY status;
"
```

Expected: `PASS | 16`.

### Warehouse marts

```bash
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT schemaname, viewname
FROM pg_views
WHERE schemaname = 'marts'
ORDER BY viewname;
"
```

### SCD2 validation

```bash
bash scripts/run_warehouse_sql.sh sql/warehouse/scd2_validation_queries.sql
```

### Data quality summary

```bash
python -c "import csv; print([r for r in csv.DictReader(open('data/bi_exports/data_quality_summary.csv', newline='')) if r['status'] != 'PASS'])"   # prints [] when every check passes
```

### Power BI exports

```bash
find exports/power_bi -maxdepth 1 -type f | sort
cat exports/power_bi/export_manifest.json
```

---

## 19. Design Decisions and Known Limitations

Deliberate trade-offs for a portfolio project built on synthetic data:

- **Public dashboard files.** Power BI Online reads five synthetic CSVs from S3 through public, read-only object URLs (no listing, no write access). The bucket policy does not enforce TLS, which is acceptable for synthetic data; a production setup would serve the files privately, for example through CloudFront with origin access control.
- **Reproducibility scope.** Direct dependencies are pinned; transitive dependencies of the dev tools are not.
- **Screenshots** show the dashboard layout; the current figures are in `exports/power_bi/`.

---

## 20. Repository Hygiene

Runtime files are git-ignored and must not be committed:

```text
venv/  venv-dbt/  venv-airflow/
.airflow/
scripts/airflow_local_env.sh
.env
dbt/target/  dbt/logs/  dbt/dbt_packages/  dbt/profiles.yml
data/raw/  data/bronze/  data/silver/  data/gold/  data/quarantine/  data/bi_exports/
logs/  (everything except .gitkeep placeholders)
__pycache__/  *.pyc
terraform.tfvars  *.tfstate  tfplan
```

CI's static job fails on committed local machine paths, private keys, AWS access keys, a tracked `.env`, `dbt/profiles.yml`, or dbt build output.

Before committing, check that only intentional changes are staged:

```bash
git status -sb
```

---

## 21. License

MIT; see `LICENSE`.
