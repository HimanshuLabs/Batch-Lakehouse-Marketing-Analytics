# Merged Lakehouse and Warehouse Architecture

## Purpose

This repository now combines the batch lakehouse engine with the warehouse/reporting layer that was first planned as a separate repository.

The lakehouse remains responsible for:

- Raw ingestion
- Bronze processing
- Silver cleaning and deduplication
- Gold business-ready outputs
- SCD2 lakehouse outputs
- Existing quality checks and orchestration

The warehouse/reporting layer starts after trusted Gold/SCD2 data exists.

## Data Flow

```text
Synthetic marketing and customer data
        ↓
Raw lakehouse
        ↓
Bronze lakehouse
        ↓
Silver lakehouse
        ↓
Gold lakehouse
        ↓
Gold/SCD2 outputs
        ↓
PostgreSQL staging schema
        ↓
PostgreSQL warehouse schema
        ↓
PostgreSQL marts schema
        ↓
Audit and reconciliation checks
        ↓
Dashboard query pack / BI reporting layer
