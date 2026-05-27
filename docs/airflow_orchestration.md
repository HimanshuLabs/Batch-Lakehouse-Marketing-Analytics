# Airflow Orchestration

## Purpose

This layer converts the batch lakehouse scripts into an orchestrated pipeline with explicit task ordering, retry behavior, and failure boundaries.

The DAG coordinates:

1. Synthetic data generation or raw data availability check
2. Bronze ingestion
3. Bronze quality checks
4. Silver transformations
5. Silver quality checks
6. Gold transformations
7. Gold quality checks
8. DuckDB analytics over Gold marts
9. PostgreSQL publish step
10. SCD Type 2 dimension build
11. Point-in-time fact joins
12. SCD2-aware Gold marts
13. SCD2 DuckDB analytics
14. dbt run
15. dbt tests
16. dbt docs
17. Power BI CSV export

## DAG

DAG file:

```text
airflow/dags/batch_lakehouse_pipeline.py
