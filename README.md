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
| Historical correctness | Customer/product/campaign dimensions with SCD Type 0, 1 and 2 attributes, and point-in-time fact joins |
| Reproducibility | Deterministic, incremental batches; reruns of a batch leave the lake and the warehouse unchanged |
| BI readiness | Revenue, campaign, product, customer, and funnel marts built for reporting |
| Analytics engineering | 24 dbt models and 82 dbt tests, docs, lineage, and exposures |
| Orchestration | A single Airflow DAG (Airflow 3.2) with 20 tasks covering the lakehouse, warehouse, dbt and reporting |
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
| Airflow screenshot | `docs/screenshots/airflow/` | Shows two successful scheduler runs of the 20-task DAG |
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

### Where each tool is used, and how that is proven

"CI" is `.github/workflows/ci.yml`; its jobs are `static`, `unit`, `integration`, `powerbi-assets` and `terraform`. The `integration` job runs every task of the Airflow DAG, in DAG order and with the DAG's own commands and environment, for two batches against PostgreSQL in Docker.

| Tool | What it does here | Where | How it is proven to run |
|---|---|---|---|
| Python | Generator, Spark jobs, loaders, exports, validators, tests | `data-generator/`, `spark-batch/`, `scripts/`, `sql/*.py`, `tests/` | CI `static` compiles every file; `unit` and `integration` execute them |
| Faker | Synthetic people: Indian names that agree with gender, and visit IP addresses, seeded per record and batch | `data-generator/generate_data.py` (DAG task `generate_data`) | CI `unit`: `tests/unit/test_coherent_source.py` and the pinned digests in `tests/unit/test_generator.py`; CI `integration` generates both batches |
| PySpark | Bronze, Silver, Gold, the dimensions (SCD Types 0, 1, 2), point-in-time fact joins, Gold marts | `spark-batch/*.py` (DAG tasks `bronze_ingestion` to `scd2_gold_marts`) | CI `unit` runs each job's functions on small frames; CI `integration` runs the jobs on two batches and compares row counts with `tests/integration/expected_counts.json` |
| DuckDB | SQL analytics straight on the Gold Parquet files, and the route B export query engine | `sql/duckdb_gold_analytics.py`, `sql/run_scd2_duckdb_analytics.py`, `sql/export_power_bi_dataset.py` (DAG tasks `duckdb_gold_analytics`, `scd2_duckdb_analytics`, `power_bi_export`) | CI `integration` runs the three tasks; the tests also read the lake with DuckDB |
| PostgreSQL | Staging, star-schema warehouse, marts, audit; each build is published atomically | `sql/warehouse/*.sql`, `scripts/run_warehouse_sql.sh` (DAG task `build_warehouse`) | CI `unit` (database tests in both time zones) and CI `integration` (29 reconciliation and 20 mart checks must pass; a reader loop sees no error during rebuilds) |
| dbt | Models over the warehouse and marts, 82 tests (the SCD type tests included), docs, exposures | `dbt/` (DAG tasks `dbt_run`, `dbt_test`, `dbt_docs`) | CI `integration`: `dbt run` PASS=25, `dbt test` PASS=83 |
| Airflow | The DAG: task order, one run at a time, `BATCH_DATE` from the logical date | `airflow/dags/batch_lakehouse_pipeline.py` | The DAG's tasks run in CI, but not under Airflow: `static` checks the DAG file, `unit` renders its `BATCH_DATE` template with Jinja2, and `integration` executes the DAG's own task commands through `tests/integration/run_dag.py` with Airflow stubbed. A real scheduler run is manual and local, recorded in `docs/warehouse/airflow_working_state.md`; a successful run of the 20-task DAG is shown in `docs/screenshots/airflow/01_dag_run_success.png` |
| Docker | Runs PostgreSQL 16.14 (`project2_postgres` locally, throwaway containers in tests). The pipeline itself is not containerized: there is no Dockerfile | `scripts/run_warehouse_sql.sh` (`docker exec`), `tests/integration/run_local.sh` | CI `unit` runs PostgreSQL as a service container; CI `integration` starts one with `docker run` |
| pytest | Unit tests in two time zones with the database tests required; integration assertions on the finished pipeline | `tests/unit/`, `tests/integration/` | CI `unit` and `integration`; locally `venv/bin/python -m pytest tests/unit` and `bash tests/integration/run_local.sh` |
| GitHub Actions | The quality gate on every push and pull request | `.github/workflows/ci.yml` | The workflow runs themselves (five jobs) |
| Power BI | The dashboard, built by hand in Power BI Online from the exported CSVs | `exports/power_bi/`, `data/bi_exports/`, `docs/warehouse/power_bi_*.md`, screenshots in `docs/screenshots/power_bi/` | Not run by the DAG or CI. CI `powerbi-assets` validates the CSVs, their column order, the manifest and the docs; CI `integration` produces the route B CSVs. The dashboard itself is shown by screenshots only |
| AWS S3 | Hosts five route B CSVs that Power BI Online reads over HTTPS | `terraform/aws-bi-exports/` | Not touched by the DAG or CI (no AWS credentials there). Upload is a manual `terraform apply`; the evidence is the bucket and the dashboard |
| Terraform | Declares the S3 bucket, its policy and the five objects | `terraform/aws-bi-exports/*.tf` | CI `terraform`: `fmt -check`, `init -backend=false`, `validate`. `plan` and `apply` are manual and never run in CI |

Listed in this README but not exercised by the DAG or CI: Power BI Online, AWS S3 and `terraform apply` (manual, as above), the Airflow scheduler itself (manual, local), and the development environment (Ubuntu, VS Code). pandas, Parquet (pyarrow), psycopg and ruff are used by the code or the CI jobs above; uv only builds the local `venv-dbt`.

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
| `ad_spend.csv` | The ad platform's feed per campaign, day and region: spend, impressions, clicks, conversions, and attributed revenue |
| `web_events.json` | User behavior and engagement events |

The behavioral event source includes customer, product, session, recommendation, campaign attribution, device/browser, engagement, fraud, schema version, and timestamp fields.

Each batch folder also holds `generation_truth.json`: the generator version, the data contract, the hidden simulation labels and the row index of every deliberately dirty row. It is not a source file and no pipeline task reads it as data.

---

## 8. Lakehouse Engine

The lakehouse engine processes generated source data through Medallion layers.

### Batches and BATCH_DATE

Every run processes one batch, named by `BATCH_DATE` (`YYYY-MM-DD`). Each job reads `BATCH_DATE` from the environment and falls back to today's date when it is unset, so always set it explicitly. The Airflow DAG sets it to the India (Asia/Kolkata) date of the run's logical date, or of its `run_after` time when the run has no logical date. The batch day is the business day: a run at 00:30 IST processes the day that has just started.

The generator is incremental and deterministic:

- `2026-05-18` is the anchor batch. It holds the base master data and a 120-day history of facts.
- Each later batch adds a 14-day increment of facts plus a few changed customers, products, and campaigns. These changes drive the SCD2 history.
- A batch continues from the batches before it. Its master files hold the current state of every customer, product and campaign (the base rows with the changes of all earlier raw batches applied), followed by the versions its own day brought, and it delivers again any earlier order that was cancelled or returned since.
- Each raw batch records the generator version that wrote it (first line of `generation_truth.json`). The generator refuses to build on raw batches of a different version and names them: regenerate from the first batch in date order.
- Batches are therefore generated in date order. The generator refuses to create a batch that is older than the newest raw batch on disk and says which date is in the way. Reprocessing existing raw batches, in any order, is unaffected: a complete raw batch is never regenerated.
- All randomness, UUIDs and Faker's names and IP addresses included, comes from named streams seeded from a fixed seed and `BATCH_DATE`. Generating the same dates in the same order produces the same files.
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

The `_scd2` facts join each row to the dimension version that was valid at the event time. A fact is never dropped because its dimension reference is bad. A missing key (an organic order has no campaign) is the unknown member. A key that is present but has no valid dimension row, for example a campaign that Silver quarantined, is the invalid-reference member (`INVALID` in Gold, key `-1` in the warehouse): the fact is kept with its own natural ID and resolves to the real member as soon as the dimension row is valid. A fact is rejected to `data/quarantine/` only for its own defect, such as an item whose order does not exist. The point-in-time report counts the invalid references per fact and key and checks that Silver rows = valid rows + rejected rows.

**Currency.** Every monetary value in this project is in Indian rupees (INR): prices, order values, discounts, tax, shipping, refunds, budgets, ad spend, the ad platform's attributed revenue, and every metric built from them (revenue, net sales, average order value, lifetime value, cost per click, cost per order). ROAS and the rates are ratios without a unit. There is no currency column because there is only one currency.

**A coherent source.** The fields of a source record agree with each other. A customer's city is in its real state, the preferred language is the state's language, Hindi or English, the name is one of Faker's Indian names and its first name agrees with the gender, the email is derived from the name (on reserved `example` domains), the signup date precedes everything else, `membership_tier` follows `loyalty_points` by fixed thresholds (Silver from 5,000, Gold from 15,000, Platinum from 30,000), the segment is plausible for the age, and about a quarter are prime members, more in the higher tiers. A product is one brand's version of one catalogue item, so name, category and brand agree, no two products share a name, and the price is in the item's range; the brand names are invented for this project. A campaign is paid marketing on one channel: its name starts with the channel, its budget is a daily amount for the days it runs, and its status follows its dates (`planned` before the start, `active` while it runs, `completed` after the end, `paused` by a revision). Organic traffic is nobody's campaign: it has no budget and no ad spend, and an organic session or order has no campaign and stays on the unknown member. A visit has one device with an operating system, browser and network that exist for it, one place (usually the customer's home city) and one traffic source, and only a campaign running that day can bring it; ad spend exists only for days a campaign runs. A visit has a cart: an order is the cart of its purchase event, with that event's value, payment method, place and campaign, and its items show the products' own names and prices. Between batches a tier changes because points were earned, a move changes city and state together, and nothing reverts. `tests/unit/test_coherent_source.py` lists every rule and scans generated batches for all of them; the deliberately dirty rows are unchanged.

**Real source attributes.** The generator delivers, and every layer carries: an order's status (`completed`, `cancelled`, `returned`), tax (18% of the net value), shipping (free from INR 4,999, else INR 99) and `total_amount` (`net + tax + shipping`, the same name and meaning in Silver, Gold, the warehouse, the marts and the exports; the net value is `cart_value` in the lake and `net_revenue` in the warehouse); the ad platform's feed per campaign, day and region (spend, impressions, clicks, and the platform's own conversions and attributed revenue, related by realistic cost per click, click-through rate and return on ad spend); a product's brand and status (`active`, `discontinued`; a status change opens a new SCD2 version); and a campaign's objective (`campaign_type`: awareness, consideration, conversion, retention). Silver validates each one, and each batch contains one deliberately dirty row per attribute that must be quarantined. The marts show cancelled and returned orders in columns of their own.

**Order lifecycle and refunds.** An order is placed at `order_timestamp`. A cancelled order has a cancellation time within hours; a returned order has a return time 2 to 20 days later and a refund, full or partial. `status_timestamp` is when the current status took effect and `refund_amount` is what was refunded; `updated_at` is when the source last changed the order. A batch knows only what had happened by the end of its day, so an order that is cancelled or returned later is first delivered as `completed`, and the batch that covers the change delivers the order again with its new status and a later `updated_at`, as an order system does. Silver keeps the latest version of each order (by `updated_at`, then the later batch) and quarantines impossible lifecycles: a return before the order, a refund above the order's net value, a refund on an order that was not returned. Gold and the warehouse carry `order_status`, `status_timestamp` and `refund_amount`; an order's refund is split over its items in whole paise (`fact_order_items.refund_amount`).

**Revenue with refunds.** A sale is an order that was not cancelled, recognised on the order date. A refund is recognised on the date of the return. `mart_revenue_daily` shows `gross_revenue`, `discount_amount`, `net_revenue` (sales), `refund_amount` and `net_revenue_after_refunds` per business date; its returned orders and amounts are those of the date of the return. The customer, campaign and product marts use the same definitions: `net_sales`, `refund_amount`, and `lifetime_value`, `attributed_revenue` and `product_revenue` as net sales less refunds. The reconciliation and the mart validation require the refunds in every mart to equal the refunds in the facts exactly.

**Daily snapshots (no leakage).** `marts.mart_customer_daily_snapshot`, `marts.mart_product_daily_snapshot` and `marts.mart_campaign_daily_snapshot` have one row per entity and business date, from the entity's first activity to the last date in the data. A row holds the columns of the lifetime mart (`mart_customer_360`, `mart_product_sales`, `mart_campaign_performance`) as they were at the end of `snapshot_date`: orders placed up to that date, each order's status as of that date (an order returned later is still a completed sale), refunds up to it, spend and web events up to it, and the dimension versions in force on it. `days_since_last_order` is counted to `snapshot_date`. Each snapshot is built by running the lifetime mart's own query on as-of copies of the facts, so a measure has one definition, and the build checks that the last date's snapshot equals the lifetime mart. For a time-based split, take features from the snapshot at the cutoff date and outcomes from what happened after it; the lifetime marts contain the whole history and must not be used as features for a past date. The snapshots are in the consumer contract as available relations and in dbt.

**Regional ad spend.** Each campaign targets two to five states. `fact_campaign_spend` has one row per campaign, day and region; a campaign's spend, and what is compared with its budget, is the sum over its regions. Each feed row repeats the platform's total for the campaign-day, and a reconciliation check requires the regional rows to sum to it. Spend and return on ad spend by channel and by region are two queries at the end of `sql/warehouse/dashboard_query_pack.sql`; the route B exports of the spend and order facts carry the same columns. Product prices are in a realistic INR range per catalogue item and most orders are one or two lines, so the average order value is a few thousand rupees and platform and warehouse ROAS are both in low single digits.

**Marts tie out.** Every mart has one row for the unknown member and one for the invalid-reference member, flagged in `member_type` (`known`, `unknown_member`, `invalid_reference`); filter `member_type = 'known'` for real customers, products or campaigns only. With those rows each mart's revenue (sales less refunds), refunds, spend and units equal the fact totals, and the mart validation checks exactly that.

**Consumer contract.** `tests/integration/test_warehouse.py` records every relation and column that Project 4 and Project 5 read (name, type, grain, key) and the integration run asserts it: adding a column is free; removing, renaming or retyping a recorded one, emptying the relation or breaking its key fails the run. The Power BI dashboards are protected the same way, by column position (section 12).

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

- Customer, product, and campaign dimensions with SCD Type 0, Type 1 and Type 2 attributes (table below).
- Surrogate keys for historical dimension versions. Dimension keys are deterministic: a rebuild from the same data gives the same keys in any load order.
- Point-in-time fact joins that only accept a version whose validity range covers the event.
- An unknown member (key 0) for a missing reference and an invalid-reference member (key -1) for a reference with no valid dimension row, so no fact is dropped for a bad reference.
- BI-ready marts for revenue, campaign, product, customer, and funnel reporting.
- Reconciliation checks stored under the audit layer.
- Query performance tuning through indexes and `EXPLAIN ANALYZE` documentation.

### SCD Types 0, 1 and 2

Each dimension attribute is one slowly-changing-dimension type. Type 0 (fixed) keeps the first value forever; a later source change is ignored and counted in `logs/scd2_quality_report.json`. Type 1 (overwrite) puts the latest value on every version, with no history. Type 2 (history) closes the current version and opens a new one with effective dates and a current flag. Only a Type 2 change creates a version or a surrogate key, so facts always keep the version of their time. Every later batch contains at least one change of each type. Details and the checks are in `docs/warehouse/scd2_design.md`.

| Dimension | Attribute | SCD type | Reason |
|---|---|---|---|
| Customer (`dim_customer`) | `signup_date` | Type 0 (fixed) | The day the account was opened cannot change; a different value later is a source error. |
| Customer (`dim_customer`) | `customer_name` | Type 1 (overwrite) | A change is a spelling correction or a new name of the same person; the old one has no reporting value. |
| Customer (`dim_customer`) | `email` | Type 1 (overwrite) | A contact detail: only the current address is of use, and an old one should not be kept. |
| Customer (`dim_customer`) | `gender` | Type 1 (overwrite) | A correction of the record, not an event in time. |
| Customer (`dim_customer`) | `age` | Type 1 (overwrite) | It moves on every year; a version per birthday would be noise. |
| Customer (`dim_customer`) | `preferred_language` | Type 1 (overwrite) | A current preference, used for contact, not for history. |
| Customer (`dim_customer`) | `loyalty_points` | Type 1 (overwrite) | A running balance that moves with every order; its history is the tier (Type 2). |
| Customer (`dim_customer`) | `is_prime_user` | Type 1 (overwrite) | The current membership flag; this project does not report on its history. |
| Customer (`dim_customer`) | `city` | Type 2 (history) | A move changes where sales are counted; orders before it belong to the old city. |
| Customer (`dim_customer`) | `state` | Type 2 (history) | Changes with the city: the same reason. |
| Customer (`dim_customer`) | `country` | Type 2 (history) | Changes with the city: the same reason. |
| Customer (`dim_customer`) | `customer_segment` | Type 2 (history) | Segment reporting must show the segment the customer was in at the time of each order. |
| Customer (`dim_customer`) | `loyalty_tier` | Type 2 (history) | Tier reporting must show the tier the customer had at the time of each order. |
| Product (`dim_product`) | `category` | Type 0 (fixed) | A product is listed in one category for good; a different value later is a source error. |
| Product (`dim_product`) | `product_name` | Type 1 (overwrite) | A renamed or corrected title is the same product; reports should show the current name. |
| Product (`dim_product`) | `brand` | Type 1 (overwrite) | A corrected brand applies to the product as a whole. |
| Product (`dim_product`) | `original_price` | Type 1 (overwrite) | The list price is reference information; the price that was paid is the Type 2 price. |
| Product (`dim_product`) | `inventory_remaining` | Type 1 (overwrite) | A level that moves with every sale and restock; only the latest is meaningful. |
| Product (`dim_product`) | `price` | Type 2 (history) | The selling price at the time of a sale explains its revenue. |
| Product (`dim_product`) | `status` | Type 2 (history) | Whether the product was on sale or withdrawn at a given time is history. |
| Campaign (`dim_campaign`) | `channel` | Type 0 (fixed) | A campaign runs on the channel it was created for; a different value later is a source error. |
| Campaign (`dim_campaign`) | `campaign_type` | Type 0 (fixed) | The objective is set when the campaign is created and defines it. |
| Campaign (`dim_campaign`) | `campaign_name` | Type 1 (overwrite) | A renamed campaign is the same campaign; reports should show the current name. |
| Campaign (`dim_campaign`) | `ab_test_group` | Type 1 (overwrite) | An assignment label; a correction applies to the whole campaign. |
| Campaign (`dim_campaign`) | `target_segment` | Type 2 (history) | Retargeting changes who the campaign reaches from that day on. |
| Campaign (`dim_campaign`) | `budget` | Type 2 (history) | Spend is judged against the budget that was authorized at the time. |
| Campaign (`dim_campaign`) | `campaign_status` | Type 2 (history) | Planned, active, paused and completed periods are the campaign's history. |
| Campaign (`dim_campaign`) | `start_date` | Type 2 (history) | A rescheduled period changes which days count as the campaign's. |
| Campaign (`dim_campaign`) | `end_date` | Type 2 (history) | A rescheduled period changes which days count as the campaign's. |

Main SQL assets:

```text
sql/warehouse/create_schemas.sql
sql/warehouse/load_gold_to_staging.sql
sql/warehouse/begin_generation.sql
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

The dbt layer organizes warehouse models, marts, tests, docs, lineage, and dashboard exposures: 24 models and 82 tests.

Main dbt areas:

```text
dbt/models/staging/      7 models
dbt/models/warehouse/    8 models
dbt/models/marts/        8 models (5 marts, 3 daily snapshots)
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
../venv-dbt/bin/dbt run --profiles-dir .              # 24 models: PASS=25
../venv-dbt/bin/dbt test --profiles-dir .             # 82 tests:  PASS=83
../venv-dbt/bin/dbt docs generate --profiles-dir .
cd ..
```

dbt's `PASS=` total is one more than the 24 models or the 82 tests (77 in `schema.yml`, 5 in `dbt/tests/`). The extra result is the project's `on-run-end` hook in `dbt/dbt_project.yml`, which grants the read-only role `p2_reader` USAGE on the dbt schemas; dbt runs it at the end of both `dbt run` and `dbt test` and counts it.

Keep `dbt/profiles.yml` in place: the DAG's dbt tasks need it. `dbt/target/`, `dbt/logs/`, and `dbt/profiles.yml` are git-ignored.

---

## 11. Airflow Orchestration Layer

Airflow coordinates the platform execution flow. One DAG, `batch_lakehouse_marketing_analytics` in `airflow/dags/batch_lakehouse_pipeline.py`, runs 20 `BashOperator` tasks through `scripts/run_airflow_task.sh`:

```text
generate_data → bronze_ingestion → bronze_quality_checks → silver_transformations →
silver_quality_checks → gold_transformations → gold_quality_checks → duckdb_gold_analytics →
publish_gold_to_postgres → scd2_dimensions → point_in_time_fact_joins → scd2_gold_marts →
scd2_duckdb_analytics → create_postgresql_schemas → load_gold_scd2_to_staging →
build_warehouse → dbt_run → dbt_test → dbt_docs → power_bi_export
```

**A rebuild is invisible to readers.** `build_warehouse` builds the dimensions and facts, the SCD2 validation, the marts and the reconciliation as one transaction (`scripts/run_warehouse_sql.sh` given the five files, starting with `sql/warehouse/begin_generation.sql`). It renames the live `warehouse`, `marts`, `audit` and `staging` schemas aside, builds new ones under the same names, and commits. Until the commit a reader still gets the old warehouse, with no error, no empty mart and no waiting; after it, the new one. If the build fails, nothing changes. That includes its checks: the quality gate (reconciliation, SCD2 validation, mart validation) runs before the commit, so a build with a failing check is rolled back, the task fails, and the failing checks (name, expected, actual, details) are written to the task log and to `logs/warehouse/failed_build_<timestamp>.json`. The staging loader fills `staging_next`, which the same transaction publishes as `staging`. The dbt views keep reading the generation they were created on until `dbt run` replaces them one by one, so they still exist and still answer if dbt or the export fails. A retired generation (`warehouse_retired_<n>` and its siblings) is dropped by a later build once no view outside it reads it.

**The build files run only as one build.** `create_warehouse_tables.sql`, `scd2_validation_queries.sql`, `create_marts.sql` and `reconciliation_report.sql` each start by checking that they are inside the transaction that `begin_generation.sql` opened; run on their own they fail with a message and change nothing. Rebuild the warehouse with `bash scripts/run_airflow_task.sh build_warehouse`.

**A consumer's own view cannot go stale unnoticed.** A view that someone creates on a warehouse or mart table is bound to the generation it was created on, and nothing would ever replace it. So the build first looks for views and materialized views outside the pipeline's schemas (`staging`, `warehouse`, `marts`, `audit` and the dbt schemas) that read the warehouse. If it finds any it stops before changing anything and names each view and what it reads: drop them, run the build, and create them again. Consumers who only query `marts.*` or the dbt views are not affected.

The DAG has no schedule and allows one active run. Its tasks use `venv/bin/python` and `venv-dbt/bin/dbt`, and each task gets `BATCH_DATE = {{ (dag_run.logical_date or dag_run.run_after).astimezone(macros.dateutil.tz.gettz('Asia/Kolkata')) | ds }}`: the India date of the run. Every task also runs under a run lock (`scripts/pipeline_lock.py`, lock file `logs/pipeline_run.lock`): a task of a second run started by any route fails at once with exit code 75 until the first run finishes or fails; `python scripts/pipeline_lock.py --release --force` clears the lock of a run that died. The scripts that write to the lake or the warehouse take the same lock when they are started by hand: `scripts/run_airflow_task.sh <task>`, `scripts/run_warehouse_sql.sh` (and so `run_warehouse_bi_pipeline.sh` and `run_warehouse_reconciliation_pipeline.sh`), and `tests/integration/rebuild_warehouse.sh`.

**Volume checks.** The Bronze checks fail a feed (one raw file) that is empty, unless the batch is declared empty by a file named `_EMPTY_BATCH` in its raw folder. They also fail a feed with fewer rows than half the median of the earlier batches (`P2_MIN_VOLUME_RATIO`, default 0.5). The first batch ever loaded is the initial load: it has no baseline and is never used as one, so the second batch only has to be non-empty. How many delivered rows must survive Silver is a separate check (the per-batch pass ratio).

**An empty first batch.** When the very first batch is declared empty and its raw files have no rows, Bronze ingests and checks them, and every later task succeeds without running and prints `NOTHING TO PROCESS`: no lake table and no warehouse is created, and the first batch with rows builds everything. An empty batch after an earlier one is processed normally.

How to trigger a run (UI, CLI, or a scripted REST call with a render gate that shows the date before any task runs) is in section 15. The full setup and troubleshooting are in `docs/warehouse/airflow_working_state.md`. Two successful scheduler runs of the 20-task DAG (13 and 14 November 2026, no failed task) are shown in:

```text
docs/screenshots/airflow/01_dag_run_success.png
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

**The column order is a contract.** Power BI reads these CSVs by column position. `scripts/power_bi_dashboard_contract.json` records the columns each file of both routes was published with, in order. Both export scripts write those columns first, in that order, and append every newer column after them; `scripts/validate_powerbi_assets.py` (CI job `powerbi-assets`) and the tests fail when a published column is moved, renamed or dropped.

`terraform output power_bi_csv_urls` prints the HTTPS URLs that Power BI uses. `data_quality_summary.csv` is built only from verified checks: warehouse reconciliation, SCD2 validation, Silver quarantine, point-in-time conservation, and the number of facts kept on the invalid-reference member per fact and dimension. See `docs/power_bi_dashboard_spec.md` and `docs/terraform_aws_bi_exports.md`.

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
| Unit tests | `pytest tests/unit` on Python 3.14 with Java 17, run twice: once with `TZ=UTC` and once with `TZ=Asia/Kolkata`. The job starts a PostgreSQL 16.14 service container on port 5546 for the tests that build a warehouse, and sets `P2_REQUIRE_DB=1`, so those tests fail instead of being skipped if the database is missing |
| Integration | the whole pipeline end to end against PostgreSQL 16.14; see below |
| Power BI artifact validation | `scripts/validate_powerbi_assets.py`: exports, their published column order, manifest, docs, screenshots |
| Terraform validation | Terraform 1.17.1: `fmt -check`, `init -backend=false`, `validate`, and no tracked state or tfvars |

The integration job (`tests/integration/run_integration.sh`):

1. runs every DAG task in DAG order, including `dbt run` and `dbt test`, for `BATCH_DATE=2026-05-18` and then `2026-06-18`;
2. asserts that all quality reports pass, that every Silver row is conserved, that SCD2 validation and reconciliation are all PASS, and that the counts equal `tests/integration/expected_counts.json`;
3. reruns `2026-06-18` and requires the lake, the warehouse, and the marts to be identical (determinism);
4. rebuilds the warehouse and checks that load order does not change the results (`tests/integration/rebuild_warehouse.sh`).

During steps 3 and 4 a reader (`tests/integration/reader_loop.py`, connected as `p2_reader`) keeps counting the rows of every mart and every dbt view. Across those four rebuilds, and a fifth that is made to fail its reconciliation and must publish nothing, it must get no error, no empty result and no row count other than the one it started with.

Run the integration test locally without touching `data/`, `logs/`, or `project2_postgres`:

```bash
bash tests/integration/run_local.sh            # add --keep to keep the work folder
```

It runs a copy of the working tree under `/tmp` against a throwaway PostgreSQL 16.14 container, `p2it_postgres_<pid>` on port 5440. It removes only that container afterwards. Locally it skips the three dbt tasks; CI runs them.

Run the unit tests locally the way CI does. The tests that build a warehouse need a throwaway PostgreSQL on port 5546; they drop and rebuild its schemas, so they only run when `P2_F_SANDBOX=1` says that database is theirs to use:

```bash
docker run -d --name p2-part-f-sandbox -p 127.0.0.1:5546:5432 \
  -e POSTGRES_USER=project2 -e POSTGRES_PASSWORD=project2 -e POSTGRES_DB=marketing_analytics postgres:16.14
P2_F_SANDBOX=1 P2_REQUIRE_DB=1 TZ=UTC venv/bin/python -m pytest tests/unit -p no:cacheprovider -rs
P2_F_SANDBOX=1 P2_REQUIRE_DB=1 TZ=Asia/Kolkata venv/bin/python -m pytest tests/unit -p no:cacheprovider -rs
docker rm -f p2-part-f-sandbox
```

Without `P2_F_SANDBOX=1` those tests are skipped; with `P2_REQUIRE_DB=1` a missing database is a failure, never a skip.

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

The dashboard screenshots were taken on 14 November 2026 from the current data; for example the executive overview's totals (revenue INR 3,984,959.66, spend INR 1,434,179.14) are those of `data/bi_exports/campaign_performance.csv`.

Airflow screenshot:

```text
docs/screenshots/airflow/01_dag_run_success.png
```

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
```

The dbt commands are in section 10.

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

**Read-only access.** Consumers (BI tools, Project 4, Project 5) connect as `p2_reader`, not as the owner. The role is created by `sql/warehouse/create_schemas.sql` on every run. It can `SELECT` from the `warehouse`, `marts` and `audit` schemas and from the dbt schemas (`dbt_staging`, `dbt_warehouse`, `dbt_marts`, `dbt_audit`); it cannot read `staging` and cannot write. Timestamps in the warehouse and the marts are `timestamptz`, shown in UTC (the database's time zone); business dates and date keys are Asia/Kolkata dates. Its password comes from `P2_READER_PASSWORD` in `.env` (the local default in `.env.example` is `p2_reader`); without one the role exists but cannot log in.

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

These are the same commands the DAG runs, in DAG order. A task started by hand takes the run lock for itself, so the loop runs under the lock as one run:

```bash
P2_RUN_ID=manual-$(date +%s) venv/bin/python scripts/pipeline_lock.py --last manual_run -- bash -c '
for task in generate_data bronze_ingestion bronze_quality_checks silver_transformations \
    silver_quality_checks gold_transformations gold_quality_checks duckdb_gold_analytics \
    publish_gold_to_postgres scd2_dimensions point_in_time_fact_joins scd2_gold_marts \
    scd2_duckdb_analytics create_postgresql_schemas load_gold_scd2_to_staging \
    build_warehouse dbt_run dbt_test dbt_docs power_bi_export; do
  bash scripts/run_airflow_task.sh "$task" || exit 1
done'
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

Expected: `PASS | 29`.

### Warehouse marts

```bash
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT schemaname, viewname
FROM pg_views
WHERE schemaname = 'marts'
ORDER BY viewname;
"
```

### SCD2 and mart validation

Both run inside every warehouse build (the build fails if one fails); `scd2_validation_queries.sql` refuses to run on its own. Read the results of the latest build:

```bash
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT 'scd2' AS checks, status, COUNT(*) FROM audit.scd2_validation_results
WHERE validation_run_id = (SELECT validation_run_id FROM audit.scd2_validation_results ORDER BY checked_at DESC LIMIT 1)
GROUP BY status
UNION ALL
SELECT 'marts', status, COUNT(*) FROM audit.mart_validation_report GROUP BY status;
"
```

Expected: `scd2 | PASS | 9` and `marts | PASS | 20`.

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

**Known characteristics of the synthetic data.** The source is made coherent enough that nothing contradicts itself, not realistic. Left as they are, on purpose: the funnel converts far more often than a real shop (event types are drawn almost evenly, so about one event in six is a purchase); most orders are a single line; loyalty points do not grow with orders and stock does not fall with sales; the model-score fields (engagement, purchase and abandonment probability, fraud score) and the recommendation fields are random and unrelated to what the visitor did, which also keeps them from leaking outcomes; an IP address is not tied to the visit's city and names are not regional; every web event carries a product, page views and searches included; prime members pay shipping like everyone else, and the free-shipping value is high for the typical order; there is no price or campaign history before the first batch; the ad platform's conversions are independent of the shop's orders; and the channel names (`google`, `facebook`, `instagram`) are the real platforms, while product brands are invented.

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
