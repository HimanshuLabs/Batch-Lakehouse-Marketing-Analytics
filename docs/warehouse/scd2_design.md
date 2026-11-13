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
- business attributes, each of SCD Type 0, 1 or 2 (see below); only the Type 2 attributes are hashed into `record_hash`
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

## SCD Types 0, 1 and 2

Not every attribute deserves history. Each attribute of the three dimensions is classified as one type, in one table in the code (`SCD_ATTRIBUTES` in `spark-batch/scd2_dimensions.py`); the job's configuration is derived from it.

| Type | Behaviour | What a change in the source does |
|---|---|---|
| Type 0 (fixed) | The first value the source delivered is kept forever. | Ignored. The entity is counted in `logs/scd2_quality_report.json` under `type0_changes_ignored`, because a change to a fixed attribute is a source error. |
| Type 1 (overwrite) | The latest value is on every version of the entity. | Overwrites the old value everywhere. No new version, no history, no new surrogate key. |
| Type 2 (history) | Each value has its own version with `effective_from`, `effective_to` and `is_current`. | Closes the current version and opens a new one with a new surrogate key. Facts keep the key of the version that covered their time. |

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

How the job applies them, per entity, over all its source rows in time order: the Type 2 attributes are hashed, and a row whose hash differs from the previous one opens a version; each Type 1 attribute takes the value of the entity's last source row on all versions; each Type 0 attribute takes the first non-null value on all versions. Because only Type 2 attributes are in the hash and the surrogate key, a Type 0 or Type 1 change never moves a key, so facts are unaffected.

Every later batch of the synthetic source contains at least one change of each type, so each behaviour runs on real pipeline data: two customers reach a higher tier and one moves (Type 2), one email address is corrected (Type 1), and one signup date is stated differently (Type 0); two products change price and one changes status (Type 2), and one is restocked (Type 1); campaigns start, end, are paused or get more budget (Type 2).

Where it is checked:

- `tests/unit/test_scd2_dimensions.py`: a Type 0 change is ignored and counted; a Type 1 change updates every version and creates none; a Type 2 change creates exactly one version with the right dates; facts still take the version of their time.
- `tests/integration/test_warehouse.py`: the second batch's own changes show all three behaviours in `warehouse.dim_customer`.
- dbt: `dbt/tests/assert_scd_type0_attributes_never_change.sql`, `assert_scd_type1_attributes_are_overwritten_on_every_version.sql`, `assert_one_current_row_per_natural_key.sql` and `assert_no_overlapping_scd2_versions.sql` (Type 2); the type of each attribute is in the column descriptions of the dimension models.

