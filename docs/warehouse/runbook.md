# Warehouse Runbook

## Purpose

This runbook validates the merged warehouse/reporting integration structure.

The warehouse layer begins after Project 2 Gold/SCD2 outputs are available.

## Required Environment Variable

Set this before running live PostgreSQL validation:

```bash
export WAREHOUSE_DATABASE_URL="postgresql://user:password@localhost:5432/database_name"
