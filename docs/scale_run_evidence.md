# Scale run evidence

What was measured when the pipeline ran at 4,00,000 customers, and what was not. Everything here was **measured by hand, once, on one machine**. CI runs the default 500-customer shop only and proves nothing at this size.

## The run

| | |
|---|---|
| Shop | `P2_NUM_CUSTOMERS=400000`; batches 2026-05-18, 2026-06-04, 2026-06-18; synthetic data |
| Size | about 79 lakh web events, about 13 lakh orders, 152 days of daily snapshots; the PostgreSQL database about 42 GB on disk |
| Machine | one Linux desktop, 16 hardware threads, about 15 GB RAM, SSD; PostgreSQL 16.14 in Docker; `/tmp` a 7.3 GB RAM disk |
| How | `tests/integration/run_dag.py`, one task at a time, as in the README's "Scale run"; every task but `power_bi_export` |
| Full run | 19-20 November 2026, commit `01fdaae` (the same tree as `3ebaf50`, the commit the run used on a working branch) |
| Build timed again | the warehouse build only, on `b223266`, 23 November 2026, on a copy of the same database |
| Settings | `SPARK_LOCAL_DIRS` and `TMPDIR` on the SSD; Spark driver heap 4 GB (`SPARK_DRIVER_MEMORY=4g`); `P2_BUILD_WORK_MEM=256MB`, `P2_BUILD_HASH_MEM_MULTIPLIER=4`, `P2_BUILD_MAINTENANCE_WORK_MEM=1GB`, `P2_BUILD_TEMP_BUFFERS=1GB`; server `shared_buffers=2GB`, `max_wal_size=8GB`, `work_mem=128MB`, `maintenance_work_mem=1GB`, `dynamic_shared_memory_type=mmap`, reverted afterwards (README, "Running at scale") |

The full run was stopped and resumed three times:
1. `silver_transformations` ran out of space in the 7.3 GB `/tmp`, and resumed with Spark scratch on the SSD.
2. `point_in_time_fact_joins` ran out of its 1 GB heap, and resumed with 4 GB.
3. `build_warehouse` resumed on `01fdaae`'s tree, which added the build settings.

Each task's time below is from its successful attempt.

## Every task of the full run (`01fdaae`, minutes)

| Batch | Task | Minutes |
|---|---|---|
| 2026-05-18 | generate_data | 12.3 |
| 2026-05-18 | bronze_ingestion | 2.1 |
| 2026-05-18 | bronze_quality_checks | 0.5 |
| 2026-06-04 | generate_data | 2.6 |
| 2026-06-04 | bronze_ingestion | 0.6 |
| 2026-06-04 | bronze_quality_checks | 0.3 |
| 2026-06-18 | generate_data | 2.9 |
| 2026-06-18 | bronze_ingestion | 0.6 |
| 2026-06-18 | bronze_quality_checks | 0.3 |
| 2026-06-18 | silver_transformations | 35.3 |
| all three | silver_quality_checks | 1.4 + 1.4 + 1.4 |
| 2026-06-18 | gold_transformations | 5.2 |
| all three | gold_quality_checks | 0.6 + 0.5 + 0.5 |
| 2026-06-18 | duckdb_gold_analytics | 0.0 |
| 2026-06-18 | publish_gold_to_postgres | 85.8 |
| 2026-06-18 | scd2_dimensions | 0.5 |
| 2026-06-18 | point_in_time_fact_joins | 1.1 |
| 2026-06-18 | scd2_gold_marts | 2.0 |
| 2026-06-18 | scd2_duckdb_analytics | 0.0 |
| 2026-06-18 | create_postgresql_schemas | 0.0 |
| 2026-06-18 | load_gold_scd2_to_staging | 14.1 |
| 2026-06-18 | **build_warehouse** | **397.4** |
| 2026-06-18 | dbt_run | 0.1 |
| 2026-06-18 | dbt_test | 1.3 |
| 2026-06-18 | dbt_docs | 0.1 |
| | **all tasks** | **570.7** |

## The warehouse build, before and after

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

## The output is the same

Each fingerprint is `tests/sql/equivalence_fingerprint.sql`: row counts and order-independent hashes of every table and view in `warehouse` and `marts`, the audit reports' rows, and the catalog (columns, constraints, indexes, views, functions, comments, grants).

1. **`6a19f35` against the full run.** A copy of the full run's database was rebuilt with `6a19f35`. The fingerprint after equals the one before: 965 lines, identical.
2. **`b223266` against `6a19f35`.** On that same copy, the fingerprint before the timed build equals step 1's result byte for byte, so the copy was unchanged. After the build with `b223266`, every one of the 965 lines is identical except one line each for five functions: `warehouse.safe_bigint`, `safe_int`, `safe_numeric` and `safe_date` (now SQL functions, STABLE, PARALLEL SAFE) and `safe_timestamp` (PARALLEL SAFE), changed on purpose by `b075324`. `safe_bool` and everything else is identical.

So the scale warehouse built by `b223266` has the same rows, marts, reports and catalog as the one built in the full run, apart from those five function definitions.

## Disk and temporary space (the `b223266` build)

| | |
|---|---|
| Free on Docker's disk at the start | 93 GB |
| Lowest free during the build | 32 GB (the `6a19f35` build: 54 GB) |
| Peak temporary files | 16.2 GB |
| Peak temporary tables | 15.4 GB |
| Peak of both at once | 31.6 GB |
| Temporary files written in all | 41.1 GB |

The peaks were sampled once a minute, so a shorter peak can have been higher.

## What was not measured

- **The tasks other than the build were not re-run after the speed-ups.** Their times are the full run's. The speed-ups changed only the build's SQL, and `66d77e0` the Spark heap of three jobs, which has not run at scale.
- **dbt at scale after the speed-ups.** `dbt_run`, `dbt_test` and `dbt_docs` were timed only in the full run.
- **`power_bi_export`, the route A export, `terraform apply` and a Power BI refresh at this size:** never run, by design. They would publish scaled files.
- **Airflow at this size.** The tasks ran through `run_dag.py`; Airflow's fixed task environment cannot pass the settings above.
- **The build with its default settings, or after the server settings were reverted:** not timed at this size.
- **Variation between runs:** each build was timed once.
- **The integration suite at this size:** not run. Equality of output at scale rests on the fingerprints above. Every speed-up was also proven at the default size by the full integration suite and an old-against-new build comparison.
- **Free disk during the full run:** not recorded.
