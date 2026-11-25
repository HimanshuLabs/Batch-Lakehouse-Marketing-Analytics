# Scale run evidence

What was measured when the pipeline ran at 4,00,000 customers, and what was not. Everything here was **measured by hand, once, on one machine**, on synthetic data; it is not a production system. CI runs the default 500-customer shop only: it proves that the default size works and that the speed-ups change no output, and it proves nothing at this size.

## The run

| | |
|---|---|
| Shop | `P2_NUM_CUSTOMERS=400000`; batches 2026-05-18, 2026-06-04, 2026-06-18; synthetic data. Web events, the order ceiling, the campaign budgets and the ad platform's figures, the stock levels and the customer history follow the shop size; the catalogue of 150 products and the 40 campaigns do not |
| Size | about 79 lakh web events, about 13 lakh orders, 152 days of daily snapshots; `dim_customer` 4,04,800 versions, 4,787 customers with more than one; the PostgreSQL database about 36 GB live, 73 GB right after a rebuild (the previous generation is kept until the next build) |
| Machine | one Linux desktop, 16 hardware threads, about 15 GB RAM, SSD; PostgreSQL 16.14 in Docker; `/tmp` a 7.3 GB RAM disk |
| How | `tests/integration/run_dag.py`, one task at a time, as in the README's "Scale run"; every task but `power_bi_export`; `publish_gold_to_postgres` last |
| When | 24-25 November 2026, commit `79e0921` |
| Settings | `SPARK_LOCAL_DIRS` and `TMPDIR` on the SSD; Spark driver heap 4 GB (`SPARK_DRIVER_MEMORY=4g`); `P2_BUILD_WORK_MEM=256MB`, `P2_BUILD_HASH_MEM_MULTIPLIER=4`, `P2_BUILD_MAINTENANCE_WORK_MEM=1GB`, `P2_BUILD_TEMP_BUFFERS=1GB`; server `shared_buffers=2GB`, `max_wal_size=8GB`, `work_mem=128MB`, `maintenance_work_mem=1GB`, `dynamic_shared_memory_type=mmap`, reverted afterwards (README, "Running at scale") |
| Free disk and memory | lowest free disk 68 GB, lowest available memory 2,708 MB (sampled once a minute) |

## Every task (`79e0921`, minutes)

| Batch | Task | Minutes |
|---|---|---|
| 2026-05-18 | generate_data | 12.3 |
| 2026-06-04 | generate_data | 2.6 |
| 2026-06-18 | generate_data | 3.6 |
| 2026-05-18 | bronze_ingestion | 2.0 |
| 2026-05-18 | bronze_quality_checks | 0.5 |
| 2026-06-04 | bronze_ingestion | 0.6 |
| 2026-06-04 | bronze_quality_checks | 0.3 |
| 2026-06-18 | bronze_ingestion | 0.6 |
| 2026-06-18 | bronze_quality_checks | 0.3 |
| 2026-06-18 | silver_transformations | 35.7 |
| all three | silver_quality_checks | 1.5 + 1.5 + 1.5 |
| 2026-06-18 | gold_transformations | 5.3 |
| all three | gold_quality_checks | 0.6 + 0.6 + 0.6 |
| 2026-06-18 | duckdb_gold_analytics | 0.0 |
| 2026-06-18 | scd2_dimensions | 0.5 |
| 2026-06-18 | point_in_time_fact_joins | 1.1 |
| 2026-06-18 | scd2_gold_marts | 2.0 |
| 2026-06-18 | scd2_duckdb_analytics | 0.0 |
| 2026-06-18 | create_postgresql_schemas | 0.0 |
| 2026-06-18 | load_gold_scd2_to_staging | 14.0 |
| 2026-06-18 | **build_warehouse** | **88.6** |
| 2026-06-18 | dbt_run | 0.1 |
| 2026-06-18 | dbt_test | 0.6 |
| 2026-06-18 | dbt_docs | 0.1 |
| 2026-06-18 | publish_gold_to_postgres | 86.5 |
| | **all tasks** | **263.6** |

The build's checks all passed: 29 reconciliation checks, 20 mart checks and 9 SCD2 checks (the build fails otherwise), and dbt built its 24 models and passed its 82 tests. The missing-dates report of the build: 2026-05-19 to 2026-05-21 (3 days), in both orders and web events, as expected.

## Compared with the scale warehouse before it

Before the run, the scale warehouse built from the earlier data was recorded: `tests/sql/equivalence_fingerprint.sql` (row counts and order-independent hashes of every table, the audit reports' rows, and the catalog) and, for the tables where only some rows may change, one hash per row. After the run the same was taken again and compared table by table and column by column with a written list of allowed differences:

- the campaign budgets and everything computed from them (the ad platform's spend, impressions, clicks, conversions and attributed revenue, ROAS and the other spend ratios);
- the stock levels;
- the customer history: the versions of the customers changed in a later batch, and the profile columns of those customers in the customer marts;
- the campaign of the few visits that run past a campaign's last day, and of their orders.

Every other row and column had to be identical, every row count the same except `dim_customer`'s and the funnel mart's (17 rows fewer: the days after a campaign's end that only those visits had reached), and every audit check PASS. Result: PASS.

## How the build got faster (earlier data, 19-23 November 2026)

These measurements were made on the scale data as it was before budgets, stock and customer history followed the shop size. They stay valid for what they show: how long each version of the build took on the same data, and that the speed-ups did not change its output.

**The earlier full run.** 19-20 November 2026, commit `01fdaae` (the run itself used a working-branch commit with the identical tree), every task but `power_bi_export`. It was stopped and resumed three times: `silver_transformations` ran out of space in the 7.3 GB `/tmp` and resumed with Spark scratch on the SSD; `point_in_time_fact_joins` ran out of its 1 GB heap and resumed with 4 GB; `build_warehouse` resumed on `01fdaae`'s tree, which added the build settings. Its tasks took 570.7 minutes, of which `build_warehouse` 397.4.

**How the times were taken.** Each build statement is timed by `\timing` in `scripts/run_warehouse_sql.sh`, and each time is assigned to its statement. Three builds were timed, all on the same machine, settings and data:

- **`01fdaae`**: the full run's build (20 November 2026).
- **`6a19f35`**: a timed rebuild on a copy of that database (22 November 2026), after the first three speed-ups. Its output was identical to the full run's (fingerprint below).
- **`b223266`**: the timed rebuild on the same copy after all eight speed-ups and the clean-up (23 November 2026).

| Build file | `01fdaae` | `6a19f35` | `b223266` |
|---|---|---|---|
| begin_generation.sql | 0.1 | 0.1 | 0.1 |
| create_warehouse_tables.sql | 96.3 | 84.0 | 33.7 |
| scd2_validation_queries.sql | 0.8 | 0.6 | 0.6 |
| create_marts.sql | 294.0 | 283.5 | 47.3 |
| reconciliation_report.sql | 6.2 | 5.9 | 5.6 |
| **build (wall clock)** | **397.4** | **374** | **88** |

### The steps the speed-ups changed (minutes)

| Step | Change | `01fdaae` | `6a19f35` | `b223266` |
|---|---|---|---|---|
| dim_date load | (d) `1502186`: one row per date before its attributes | 38.1 | 37.9 | 5.4 |
| dim_region load | (d): group the rows before choosing one | 5.3 | 5.3 | 3.8 |
| dim_channel load | (d) | 3.7 | 3.8 | 2.9 |
| fact_orders load | (h) `54e03fc`: foreign keys after the loads | 3.1 | 3.0 | 1.8 |
| fact_order_items load | (h) | 3.8 | 3.8 | 1.9 |
| fact_campaign_spend load | none (it keeps its four foreign keys inline) | under 0.1 | 0.0 | 0.0 |
| fact_conversions load | (h) | 1.6 | 1.7 | 0.6 |
| the 24 foreign keys, added after the loads | (h) | (in the loads) | (in the loads) | 0.5 |
| web events load | (j) `fa6a425`: deduplicated events materialised first; (b) `b075324`: casts without subtransactions; (h) | 23.6 | 24.0 | 14.4 |
| record_hash | (e) `9e9fd7a`: computed in the INSERTs of four facts; fact_orders keeps its UPDATE | 13.7 | 1.3 (fact_orders' UPDATE) | the same statement, unchanged since `6a19f35` |
| daily snapshots | (a) `eaab432`: one set-based pass instead of a loop per day | 266.5 + 1.5 | 267.8 | 28.1 |

(g) `96a02f6` removed dead `ON CONFLICT` clauses; (c) `6a19f35` makes the mart checks read one copy of each lifetime mart. Both changes are spread over many statements, so they have no row of their own.

**One statement got slower.** Check 4j of the mart validation (`create_marts.sql`, the `INSERT INTO audit.mart_validation_report` after `-- 4j.`, whose text did not change) took 5.1 min on `6a19f35` and 8.7 min on `b223266` (8.3 min on `01fdaae`, before `6a19f35` made it read a copy of `mart_customer_360`). The cause was not measured. A likely one, read from the code only: `b223266` writes `mart_customer_daily_snapshot` in one statement, most likely grouped by customer, where the old loop wrote it one day at a time, so the check's lookups of each customer's last day touch many more pages.

The `01fdaae` step times were read from that build's log by hand; the other two columns come from the same statement-to-time pairing.

### The output is the same

Each fingerprint is `tests/sql/equivalence_fingerprint.sql`: row counts and order-independent hashes of every table and view in `warehouse` and `marts`, the audit reports' rows, and the catalog (columns, constraints, indexes, views, functions, comments, grants).

1. **`6a19f35` against the full run.** A copy of the full run's database was rebuilt with `6a19f35`. The fingerprint after equals the one before: 965 lines, identical.
2. **`b223266` against `6a19f35`.** On that same copy, the fingerprint before the timed build equals step 1's result byte for byte, so the copy was unchanged. After the build with `b223266`, every one of the 965 lines is identical except one line each for five functions: `warehouse.safe_bigint`, `safe_int`, `safe_numeric` and `safe_date` (now SQL functions, STABLE, PARALLEL SAFE) and `safe_timestamp` (PARALLEL SAFE), changed on purpose by `b075324`. `safe_bool` and everything else is identical.

So the scale warehouse built by `b223266` has the same rows, marts, reports and catalog as the one built in the full run, apart from those five function definitions.

### Disk and temporary space (the `b223266` build)

| | |
|---|---|
| Free on Docker's disk at the start | 93 GB |
| Lowest free during the build | 32 GB (the `6a19f35` build: 54 GB) |
| Peak temporary files | 16.2 GB |
| Peak temporary tables | 15.4 GB |
| Peak of both at once | 31.6 GB |
| Temporary files written in all | 41.1 GB |

The peaks were sampled once a minute, so a shorter peak can have been higher.

## Power BI and S3

On 25 November 2026, from the scale lake and warehouse above, on commit `79e0921`.

**The exports.** Both export scripts were run once, by hand, writing to a folder outside the checkout:
- route B with `BATCH_DATE=2026-06-18`;
- route A in a read-only session with parallel query off.

Afterwards:
- the live database's row counts and server settings were the same as before;
- nothing in the checkout had changed;
- the committed `exports/power_bi/` files are still the default-size run.

| Route B file | Rows | Route A file | Rows |
|---|---|---|---|
| `campaign_performance.csv` | 55 | `revenue_daily.csv` | 152 |
| `customer_lifetime_value.csv` | 3,84,062 | `campaign_performance.csv` | 39 |
| `product_performance.csv` | 141 | `product_sales.csv` | 149 |
| `marketing_funnel.csv` | 34 | `customer_360.csv` | 3,99,999 |
| `data_quality_summary.csv` | 87, every check PASS | `marketing_funnel.csv` | 1,324 |
| `fact_orders.csv` | 12,89,899 | | |
| `fact_order_items.csv` | 14,29,440 | | |
| `fact_campaign_spend.csv` | 4,284 | | |

The route B report's overall status was PASS. All five route A manifest records were SUCCESS. The three fact files are not published.

**Route A: the second S3 bucket.** Route A's five semantic models read their CSVs by web URL from an S3 bucket that was set up by hand and is not in Terraform's state. Until 23 November 2026 its five objects were byte-identical to the committed `exports/power_bi/` files; since then they hold the scale run's files, replaced by hand in the S3 console, last on 25 November 2026. Two checks followed, for each object:
- it was read back from S3 with the account's own credentials, and its md5 equals the export's;
- a public HTTPS request for it, without credentials, returned HTTP 200 with the right Content-Length.

| File | Bytes, default size (committed) | Bytes, 25 November 2026 | Public read | md5, 25 November 2026 |
|---|---|---|---|---|
| `campaign_performance.csv` | 12,362 | 14,581 | HTTP 200 | `5965c99061aa21651280eeb06987c2b6` |
| `customer_360.csv` | 1,43,947 | 11,73,25,125 | HTTP 200 | `d02507d92989a4679fc225449d8444b5` |
| `marketing_funnel.csv` | 1,79,789 | 2,45,364 | HTTP 200 | `12b7df62a6fe99ab4f9e0ec18e3804da` |
| `product_sales.csv` | 30,039 | 36,085 | HTTP 200 | `fe2b7d80c916bb3b0e2c2ddb83ff7e13` |
| `revenue_daily.csv` | 23,173 | 30,739 | HTTP 200 | `a9421b2d5087e965f98dfc7ac6904556` |

**Route B: the Terraform bucket.** Terraform v1.17.1 in AWS CloudShell, with the state as the apply of 23 November 2026 left it. Four summary files went up at scale. `customer_lifetime_value.csv` stays the default-size file of 14 November (1,36,893 bytes), for two reasons:
- **Size:** at scale it is 11,23,96,740 bytes.
- **ETag:** the AWS provider uploads objects over 5 MiB in parts, and S3's ETag is then not an MD5. `etag = filemd5(...)` in `main.tf` would therefore show one change on every later plan.

`main.tf` is unchanged. The steps:

1. `terraform plan` with the files of the previous apply: `No changes. Your infrastructure matches the configuration.`
2. The four files placed, md5 checked; `terraform fmt -check -recursive` passed.

   | File | Bytes | md5 |
   |---|---|---|
   | `campaign_performance.csv` | 26,505 | `fbf137498360f712cabb62cb2111e8f0` |
   | `product_performance.csv` | 44,129 | `f47d71f6a363a76e04003edd948cf054` |
   | `marketing_funnel.csv` | 10,261 | `870c4a0bb6120db5c1328632c49e5d71` |
   | `data_quality_summary.csv` | 7,124 | `2804f30b71fecaf0ddf74cb2256dde32` |

3. `terraform plan`: four `aws_s3_object.power_bi_exports[...] will be updated in-place`, then `Plan: 0 to add, 4 to change, 0 to destroy.`
4. `terraform apply`: four `Modifications complete`, then `Apply complete! Resources: 0 added, 4 changed, 0 destroyed.`
5. `terraform plan` again: `No changes. Your infrastructure matches the configuration.`
6. The bucket:
   - for all five objects, the S3 ETag equals the local md5;
   - a public read of each returns HTTP 200 with Content-Length 26,505, 44,129, 10,261, 7,124 and 1,36,893;
   - another key returns HTTP 403, and so does listing the bucket;
   - four objects are dated 25 November 2026 and `customer_lifetime_value.csv` 14 November 2026.

Only `campaign_performance.csv` and `data_quality_summary.csv` of this bucket are connected to a Power BI semantic model.

**Power BI Online.** All seven semantic models were refreshed on 25 November 2026, with no warnings. Money columns and measures are shown in rupees (₹); counts, ratios and percentages are unchanged. The cards, at scale and as they were on 14 November at the default size:

| Page | Card | 4,00,000 customers | Default size |
|---|---|---|---|
| Revenue Trends | Total Net Revenue | ₹3.28bn | 4.22M |
| Revenue Trends | Total Orders | 1M | 2K |
| Revenue Trends | Average Order Value | ₹2.74K | 2.80K |
| Customer 360 | Total Customers | 400K | 497 |
| Customer 360 | Average Customer Value | ₹7.86K | 7.92K |
| Customer 360 | Total Lifetime Value | ₹3.14bn | 3.94M |
| Customer 360 | Total Orders | 1M | 2K |
| Campaign Performance | Total Campaign Spend | ₹1.11bn | 1.39M |
| Campaign Performance | Campaign Revenue | ₹1.81bn | 2.25M |
| Campaign Performance | Campaign ROAS | 1.63 | 1.62 |
| Product Performance | Total Product Revenue | ₹3.06bn | 3.82M |
| Product Performance | Product Web Events | 8M | 10K |
| Product Performance | Units Sold | 2M | 3K |
| Product Performance | Average Selling Price | ₹1.45K | 1.45K |
| Marketing Funnel | Total Events | 8M | 10K |
| Marketing Funnel | Total Sessions | 3M | 4K |
| Marketing Funnel | Total Purchases | 1M | 2K |
| Marketing Funnel | Funnel Conversion Rate | 42.20% | 43.10% |
| Executive Overview | Total Revenue | ₹3.14bn | 3.98M |
| Executive Overview | Total Spend | ₹1.15bn | 1.43M |
| Executive Overview | Average ROAS | 1.01 | 1.07 |
| Executive Overview | Total Orders | 1M | 2K |

The Executive Overview table's totals are spend ₹1,147,343,344.01 and revenue ₹3,142,927,795.52. The Data Quality page shows 87 checks, every one PASS. The nine Power BI screenshots in the README (section 14) were taken after this refresh.

## What was not measured

- **The `power_bi_export` task at this size:** not run through `run_dag.py`, by design, because it writes into the checkout. Its script was run by hand instead (see "Power BI and S3").
- **Airflow at this size.** The tasks ran through `run_dag.py`; Airflow's fixed task environment cannot pass the settings above.
- **The jobs' own 4 GB Spark heap.** `point_in_time_fact_joins`, `scd2_dimensions` and `scd2_gold_marts` set it themselves since `66d77e0`, but the run also passed `SPARK_DRIVER_MEMORY=4g`, so it does not show that their own setting is enough.
- **The build with its default settings, or after the server settings were reverted:** not timed at this size.
- **Variation between runs:** each task was timed once.
- **The integration suite at this size:** not run. Equality of output at scale rests on the fingerprints and the comparison above. Every speed-up was also proven at the default size by the full integration suite and an old-against-new build comparison.
