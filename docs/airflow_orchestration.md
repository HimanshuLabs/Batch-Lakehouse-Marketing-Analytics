# Airflow Orchestration

## Purpose

This layer converts the batch lakehouse scripts into an orchestrated pipeline with explicit task ordering, retry behavior, and failure boundaries.

The DAG runs 20 tasks, in this order (the two analytics and publish tasks after the Gold quality checks run in parallel):

1. Synthetic data generation or raw data availability check (`generate_data`)
2. Bronze ingestion (`bronze_ingestion`)
3. Bronze quality checks (`bronze_quality_checks`)
4. Silver transformations (`silver_transformations`)
5. Silver quality checks (`silver_quality_checks`)
6. Gold transformations (`gold_transformations`)
7. Gold quality checks (`gold_quality_checks`)
8. DuckDB analytics over Gold marts (`duckdb_gold_analytics`)
9. PostgreSQL publish step (`publish_gold_to_postgres`), in parallel with 8
10. SCD Type 2 dimension build (`scd2_dimensions`)
11. Point-in-time fact joins (`point_in_time_fact_joins`)
12. SCD2-aware Gold marts (`scd2_gold_marts`)
13. SCD2 DuckDB analytics (`scd2_duckdb_analytics`)
14. PostgreSQL schemas (`create_postgresql_schemas`)
15. Gold to PostgreSQL staging load (`load_gold_scd2_to_staging`)
16. Warehouse build: dimensions, facts, SCD2 validation, marts and reconciliation in one transaction (`build_warehouse`)
17. dbt run (`dbt_run`)
18. dbt tests (`dbt_test`)
19. dbt docs (`dbt_docs`)
20. Power BI CSV export (`power_bi_export`)

## DAG

DAG file:

```text
airflow/dags/batch_lakehouse_pipeline.py
