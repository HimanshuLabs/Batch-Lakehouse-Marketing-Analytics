# Known Limitations in the Code

These are known weaknesses in the pipeline code. They are not fixed yet, and none of them affects the numbers the pipeline produces today. Each entry gives the file, what can go wrong, and why that is acceptable for now.

The design-level limitations are listed in the README under "Known Limitations": public S3 read, organic orders, UTC business dates, unpinned dev dependencies, and screenshots.

## Staging loader

**An audit write can hide the original error.**
`scripts/load_gold_to_postgres_staging.py`

When a source fails to load, the loader writes a `FAILED` row to its audit table. If PostgreSQL itself is down, that audit write fails too, the loader stops, and it skips the remaining sources.

*Why it is acceptable for now:* the loader still exits with an error, so the Airflow task fails and nothing downstream runs. Once the database is back, a rerun loads every source again.

**A staging table can be left half-written.**
`scripts/load_gold_to_postgres_staging.py`

Each table is replaced, and its audit row is written, in two separate steps rather than one transaction. A failure between them can leave a staging table partly loaded.

*Why it is acceptable for now:* any failed source makes the loader exit with an error, which stops the run before the warehouse is built from staging. A rerun replaces every staging table in full.

## Spark jobs

**Fact checks look at the data before it is written, not after.**
`spark-batch/point_in_time_fact_joins.py`

The point-in-time job validates each fact table in memory and then writes it. It does not read the written files back to check them.

*Why it is acceptable for now:* the next steps read the written files. The staging load reads them, and the warehouse reconciliation compares those counts and amounts with the warehouse tables, so a bad write would show up as a failed check.

## Exports and analytics

**No report when DuckDB cannot start.**
`sql/run_scd2_duckdb_analytics.py`, `sql/export_power_bi_dataset.py`

Both jobs open their in-memory DuckDB connection before their error handling starts. If opening it fails, the job stops without writing its usual report.

*Why it is acceptable for now:* an in-memory DuckDB connection needs no server or file, so this is very unlikely. If it does happen, the job still exits with an error and the Airflow task fails visibly.

**Each Power BI export query runs twice.**
`sql/export_power_bi_dataset.py`

Every export query runs once to write the CSV and a second time to count its rows.

*Why it is acceptable for now:* the exports are small. The largest is about 5,100 rows, so running each query a second time adds almost no time to the task.

**The SQL splitter does not understand quotes.**
`sql/run_scd2_duckdb_analytics.py`

The script splits its query file into statements at every `;`, including any `;` inside a quoted string.

*Why it is acceptable for now:* the query file it reads has no `;` inside any string, so every statement splits correctly. Adding one would break the job loudly, not silently.

## Orchestration

**An optional step is skipped silently if its script is missing.**
`scripts/run_airflow_task.sh`

Two tasks run their script through `run_optional_python_file`: `duckdb_gold_analytics` and `publish_gold_to_postgres`. If the script file is missing, the task logs a line and succeeds without doing anything.

*Why it is acceptable for now:* both scripts are in the repository, and a skip prints `Optional file not found, skipping` in the task log. Neither step feeds the warehouse, the dbt models, or the reconciliation checks. One writes DuckDB report CSVs, and the other writes a separate `gold` schema in PostgreSQL that only the legacy `sql/postgres_analytics_queries.sql` reads.

## Generator

**The batch folder rename is not flushed to disk.**
`data-generator/generate_data.py`

The generator writes a batch's files into a temporary folder, flushes each file to disk, and then renames the folder into place. It does not flush the parent directory after the rename. So after a power cut, the new folder could be missing even though the run finished.

*Why it is acceptable for now:* the generator is deterministic. If the folder is missing, the next run of the same batch date recreates identical files.

## Warehouse keys

**Fact row keys depend on load order.**
`sql/warehouse/create_warehouse_tables.sql`

The fact tables number their rows with auto-incrementing identity keys: `order_sk`, `order_item_sk`, `campaign_spend_sk`, `event_sk` and `conversion_sk`. Rebuilding the warehouse in a different order can give the same rows different numbers.

*Why it is acceptable for now:* no mart, export, or dbt model reads these keys. `order_sk` is used only as the last tie-breaker when ordering one customer's orders in `mart_revenue_daily`. The dimension keys that facts join on are deterministic, so joins and reports do not change.
