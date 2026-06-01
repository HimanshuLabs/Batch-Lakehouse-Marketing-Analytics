# SCD Type 2 Design

## Purpose

SCD Type 2 preserves historical changes in dimensional attributes.

In this project, SCD2 keeps customer, product, and campaign history accurate so facts can join to the correct historical version of each dimension.

## SCD2 Dimensions

| Dimension | Natural Key | Surrogate Key |
|---|---|---|
| warehouse.dim_customer | customer_id | customer_sk |
| warehouse.dim_product | product_id | product_sk |
| warehouse.dim_campaign | campaign_id | campaign_sk |

## Required SCD2 Columns

Each SCD2 dimension should contain:

- natural key
- surrogate key
- tracked business attributes
- effective_start_date
- effective_end_date
- is_current
- record_hash
- loaded_at

## Why Surrogate Keys Matter

A natural key identifies the business entity.

A surrogate key identifies one historical version of that entity.

Example:

```text
customer_id = 101
customer_sk = 1 -> Silver tier from January to March
customer_sk = 2 -> Gold tier from March onward
