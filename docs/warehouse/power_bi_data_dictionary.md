# Power BI Data Dictionary

## Purpose

This document defines the CSV datasets used by the Power BI Online dashboard demo.

The datasets are exported from PostgreSQL `marts` views. They are business-ready reporting outputs, not raw lakehouse files.

**Currency.** Every monetary field in every dataset is in Indian rupees (INR): revenue, sales, discounts, refunds, tax, shipping, order totals, prices, average order value, lifetime value, budgets, spend, cost per click and cost per order. No field holds another currency and there is no currency column. In Power BI, format these fields as Currency with the symbol ₹ (English (India)).

## Exported dataset contract

| Dataset | Source mart | CSV path | Primary dashboard usage |
|---|---|---|---|
| Revenue Daily | `marts.mart_revenue_daily` | `exports/power_bi/revenue_daily.csv` | Revenue trends, order volume, average order value |
| Campaign Performance | `marts.mart_campaign_performance` | `exports/power_bi/campaign_performance.csv` | Campaign spend, revenue attribution, ROAS |
| Product Sales | `marts.mart_product_sales` | `exports/power_bi/product_sales.csv` | Product/category revenue and units sold |
| Customer 360 | `marts.mart_customer_360` | `exports/power_bi/customer_360.csv` | Customer segmentation, lifetime value, purchase behavior |
| Marketing Funnel | `marts.mart_marketing_funnel` | `exports/power_bi/marketing_funnel.csv` | Funnel stages, conversion behavior, drop-off analysis |

## Export manifest

Every export run writes:

```text
exports/power_bi/export_manifest.json
```

The manifest records the health of each dataset export.

| Manifest field | Meaning |
|---|---|
| `table_name` | Source PostgreSQL mart used for the export |
| `csv_file_path` | Relative path to the exported CSV file |
| `row_count` | Number of data rows exported, excluding the header |
| `exported_at` | UTC timestamp for that dataset export |
| `status` | Export status, expected to be `SUCCESS` |

Expected healthy state:

```text
status = SUCCESS
row_count > 0
```

---

## Dataset: `revenue_daily.csv`

### Source

```text
marts.mart_revenue_daily
```

### Grain

One row per reporting date or daily revenue aggregation unit exposed by the mart.

### Business purpose

Supports daily revenue reporting, order volume tracking, and average order value analysis.

### Column groups

| Column group | Meaning |
|---|---|
| Date column | Reporting date used for trend analysis |
| Revenue metrics | Gross, discount and net revenue of the orders placed that day that were not cancelled (sales are recognised on the order date) |
| Refund metrics | `refund_amount`: refunds of the returns that happened that day; `net_revenue_after_refunds` = `net_revenue` - `refund_amount` |
| Charge metrics | `tax_amount`, `shipping_amount` and `total_amount` (net + tax + shipping) of those orders |
| Order metrics | Count of the orders that were not cancelled, and supporting sales volume metrics |
| Cancelled and returned | `cancelled_order_count`, `cancelled_amount` (by order date; not sales); `returned_order_count`, `returned_amount` (by the date of the return) |
| Average value metrics | Average order value or similar derived KPI fields |
| Audit/support fields | Optional mart-generated metadata or supporting fields |

### Recommended Power BI data types

| Field type | Power BI type |
|---|---|
| Date fields | Date |
| Revenue fields | Decimal number / Currency |
| Count fields | Whole number |
| Ratio fields | Decimal number / Percentage |

### Recommended visuals

- KPI card: Total revenue
- KPI card: Total orders
- KPI card: Average order value
- Line chart: Revenue by date
- Column chart: Orders by date
- Table/matrix: Daily revenue summary

### Suggested slicers

- Date range

---

## Dataset: `campaign_performance.csv`

### Source

```text
marts.mart_campaign_performance
```

### Grain

One row per campaign-level reporting unit exposed by the mart.

### Business purpose

Supports marketing performance reporting across campaign spend, campaign-attributed revenue, conversions, and ROAS.

### Column groups

| Column group | Meaning |
|---|---|
| Campaign identifiers | Campaign ID, campaign name, `campaign_type` (the objective: awareness, consideration, conversion, retention), status and A/B group; `member_type` as in the customer dataset |
| Channel/source fields | Marketing channel, traffic source, or attribution fields |
| Spend metrics | Campaign cost/spend values |
| Revenue metrics | `attributed_revenue`: `net_sales` (net value of the campaign's orders that were not cancelled) less `refund_amount` |
| Ad platform metrics | `ad_platform_conversions`, `platform_attributed_revenue`, `platform_roas`: what the ad platform itself reports, shown beside the warehouse-measured figures |
| Conversion metrics | Conversion counts, conversion rates, or funnel outcomes |
| Cancelled and returned | Cancelled and returned order counts and amounts; a returned order is a sale whose refund is subtracted |
| Efficiency metrics | ROAS, cost per conversion, or related performance metrics |

### Recommended Power BI data types

| Field type | Power BI type |
|---|---|
| Campaign IDs/names | Text |
| Channel/source fields | Text |
| Spend fields | Decimal number / Currency |
| Revenue fields | Decimal number / Currency |
| Count fields | Whole number |
| ROAS/rate fields | Decimal number |

### Recommended visuals

- KPI card: Total campaign spend
- KPI card: Campaign-attributed revenue
- KPI card: Average ROAS
- Bar chart: Revenue by campaign
- Bar chart: Spend by campaign
- Bar chart: ROAS by campaign
- Table: Campaign performance summary

### Suggested slicers

- Campaign
- Channel
- Date, if available

---

## Dataset: `product_sales.csv`

### Source

```text
marts.mart_product_sales
```

### Grain

One row per product or product/category reporting unit exposed by the mart.

### Business purpose

Supports product sales reporting, category performance analysis, and merchandising decisions.

### Column groups

| Column group | Meaning |
|---|---|
| Product identifiers | Product ID, product name, or product grouping fields; `member_type` as in the customer dataset |
| Category fields | Product category, `brand` and `product_status` (active or discontinued) |
| Quantity metrics | Units sold in orders that were not cancelled; `cancelled_units`, `returned_units` and `returned_amount` beside them |
| Revenue metrics | `product_revenue`: `net_sales` less `refund_amount` (the product's share of each order's refund); category revenue rank and share |
| Price/value metrics | Average price, average item value, or supporting sales metrics |

### Recommended Power BI data types

| Field type | Power BI type |
|---|---|
| Product IDs/names | Text |
| Category fields | Text |
| Quantity/count fields | Whole number |
| Revenue fields | Decimal number / Currency |
| Average value fields | Decimal number / Currency |

### Recommended visuals

- KPI card: Total product revenue
- KPI card: Units sold
- Bar chart: Revenue by product
- Bar chart: Revenue by category
- Table: Product sales summary

### Suggested slicers

- Product category
- Product name

---

## Dataset: `customer_360.csv`

### Source

```text
marts.mart_customer_360
```

### Grain

One row per customer-level reporting unit exposed by the mart.

### Business purpose

Supports customer analytics, segmentation, lifetime value reporting, and purchase behavior analysis.

### Column groups

| Column group | Meaning |
|---|---|
| Customer identifiers | Customer ID or customer-level keys; `member_type` is `known` for a real customer (the mart also has one row each for the unknown and the invalid-reference member) |
| Segment fields | Customer segment, membership tier, or grouping attributes |
| Value metrics | `lifetime_value`: `net_sales` (orders that were not cancelled) less `refund_amount` |
| Purchase metrics | Count of orders that were not cancelled, repeat purchase indicators, frequency metrics |
| Cancelled and returned | `cancelled_order_count`, `cancelled_amount`, `returned_order_count`, `returned_amount` |
| Recency fields | Last order/activity date or freshness indicators |
| Geography fields | City, state, country, or regional grouping fields if present |

### Recommended Power BI data types

| Field type | Power BI type |
|---|---|
| Customer IDs | Text |
| Segment/tier fields | Text |
| Date fields | Date |
| Revenue/value fields | Decimal number / Currency |
| Count fields | Whole number |
| Rate fields | Decimal number / Percentage |

### Recommended visuals

- KPI card: Total customers
- KPI card: Average lifetime value
- KPI card: Repeat customer count
- Bar chart: Revenue by customer segment
- Table: Customer 360 summary
- Distribution chart: Customers by segment

### Suggested slicers

- Customer segment
- Membership tier
- City/state if present

---

## Dataset: `marketing_funnel.csv`

### Source

```text
marts.mart_marketing_funnel
```

### Grain

One row per funnel-stage, campaign, channel, or event-level reporting unit exposed by the mart.

### Business purpose

Supports funnel progression reporting, conversion analysis, and drop-off identification.

### Column groups

| Column group | Meaning |
|---|---|
| Funnel stage fields | User journey or funnel stage |
| Event fields | Event type or behavioral action |
| Volume metrics | Event count, visitor count, or session count |
| Conversion metrics | Conversion count or converted-user count |
| Rate metrics | Conversion rate, drop-off rate, or stage progression rate |
| Attribution fields | Campaign, channel, or traffic source if present |

### Recommended Power BI data types

| Field type | Power BI type |
|---|---|
| Funnel stage fields | Text |
| Event fields | Text |
| Campaign/channel fields | Text |
| Count fields | Whole number |
| Rate fields | Decimal number / Percentage |

### Recommended visuals

- Funnel chart: Stage progression
- KPI card: Total events
- KPI card: Total conversions
- KPI card: Funnel conversion rate
- Bar chart: Event count by stage
- Table: Funnel stage summary

### Suggested slicers

- Funnel stage
- Campaign
- Channel
- Date, if available

---

## Data quality expectations

Before CSV files are uploaded to Power BI Online:

1. Every required CSV file must exist.
2. Every CSV file must contain a header row.
3. Every CSV file must contain at least one data row.
4. `export_manifest.json` must contain five records.
5. Every manifest record must have `status = SUCCESS`.
6. Every manifest record must have `row_count > 0`.

## Validation commands

Run from the project root:

```bash
ls -lh exports/power_bi

head -n 1 exports/power_bi/revenue_daily.csv
head -n 1 exports/power_bi/campaign_performance.csv
head -n 1 exports/power_bi/product_sales.csv
head -n 1 exports/power_bi/customer_360.csv
head -n 1 exports/power_bi/marketing_funnel.csv

wc -l exports/power_bi/*.csv

python -m json.tool exports/power_bi/export_manifest.json
```

## Modeling notes for Power BI Online

- Use CSV files as imported datasets.
- Keep base business logic in PostgreSQL marts.
- Use Power BI measures only for presentation-level calculations when needed.
- Correct inferred data types after upload.
- Prefer slicers for date, campaign, product category, and customer segment.
- Keep dashboard visuals aligned to the mart grain.
