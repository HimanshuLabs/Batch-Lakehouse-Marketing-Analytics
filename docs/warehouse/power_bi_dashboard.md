# Power BI Online Dashboard Demo

## Purpose

This dashboard demo proves that the PostgreSQL warehouse marts are BI-ready.

Power BI Online consumes CSV exports generated from the trusted `marts` schema. The dashboard is intentionally a thin reporting layer. It does not rewrite lakehouse logic, warehouse logic, dbt models, or Airflow orchestration.

## Source contract

Power BI consumes these exported files:

| PostgreSQL mart | Exported CSV | Dashboard usage |
|---|---|---|
| `marts.mart_revenue_daily` | `exports/power_bi/revenue_daily.csv` | Executive KPIs and revenue trend |
| `marts.mart_campaign_performance` | `exports/power_bi/campaign_performance.csv` | Campaign ROAS and spend efficiency |
| `marts.mart_product_sales` | `exports/power_bi/product_sales.csv` | Product/category revenue analysis |
| `marts.mart_customer_360` | `exports/power_bi/customer_360.csv` | Customer segmentation and lifetime value |
| `marts.mart_marketing_funnel` | `exports/power_bi/marketing_funnel.csv` | Funnel conversion analysis |

## Dashboard pages

### Executive Overview

**Purpose**

Provide a fast summary of business health across revenue, orders, customers, campaigns, products, and funnel performance.

**Dataset / CSV used**

- `revenue_daily.csv`
- `campaign_performance.csv`
- `product_sales.csv`
- `customer_360.csv`
- `marketing_funnel.csv`

**Suggested visuals**

- KPI card: Total revenue
- KPI card: Total orders
- KPI card: Average order value
- KPI card: Total customers
- KPI card: Overall conversion rate
- Line chart: Daily revenue trend
- Bar chart: Top product categories by revenue
- Table: Top campaigns by ROAS

**Fields used**

From `revenue_daily.csv`:

- date / order date field
- revenue metric
- order count metric
- average order value metric

From `campaign_performance.csv`:

- campaign identifier/name
- spend
- revenue
- ROAS
- conversion metrics

From `product_sales.csv`:

- product identifier/name
- category
- units sold
- revenue

From `customer_360.csv`:

- customer identifier
- customer segment
- lifetime value
- order count

From `marketing_funnel.csv`:

- funnel stage
- event count
- conversion count/rate

**Filters / slicers**

- Date
- Campaign
- Product category
- Customer segment

**KPIs**

- Total revenue
- Total orders
- Average order value
- Customer count
- Overall conversion rate
- Campaign ROAS

**Operational notes**

- The dashboard reads from PostgreSQL marts, not raw files.
- Revenue and KPI metrics are reconciled upstream.
- SCD2 warehouse logic preserves historical correctness before reporting.
- Power BI is only the presentation layer; trust comes from warehouse validation.

---

### Revenue Trends

**Purpose**

Show how revenue changes over time and prove that the warehouse supports time-series reporting.

**Dataset / CSV used**

- `revenue_daily.csv`

**Suggested visuals**

- Line chart: Daily revenue
- Column chart: Daily order count
- KPI card: Total revenue
- KPI card: Average order value
- Matrix: Revenue by date

**Fields used**

- date / order date
- total revenue
- order count
- average order value

**Filters / slicers**

- Date range
- Optional customer segment if joined manually in Power BI
- Optional campaign if model is extended later

**KPIs**

- Total revenue
- Daily revenue
- Total orders
- Average order value

**Operational notes**

- This page uses a pre-shaped revenue mart.
- Power BI does not calculate base revenue from raw transactions.
- The reporting mart already encodes consistent metric definitions.

---

### Campaign Performance

**Purpose**

Measure which campaigns generate revenue efficiently.

**Dataset / CSV used**

- `campaign_performance.csv`

**Suggested visuals**

- Bar chart: Revenue by campaign
- Bar chart: Spend by campaign
- Bar chart: ROAS by campaign
- Table: Campaign, spend, revenue, conversions, ROAS
- KPI card: Total campaign spend
- KPI card: Total campaign revenue
- KPI card: Average ROAS

**Fields used**

- campaign id/name
- channel
- campaign spend
- campaign revenue
- conversions
- ROAS
- click/conversion metrics if available

**Filters / slicers**

- Campaign
- Channel
- Date range if the mart exposes date
- Campaign status/type if available

**KPIs**

- Total spend
- Total campaign-attributed revenue
- ROAS
- Conversion rate
- Cost per conversion if present

**Operational notes**

- Spend and revenue are joined at the warehouse/mart layer.
- ROAS is exposed as a business-ready KPI.
- The page connects marketing activity to revenue performance.

---

### Product Performance

**Purpose**

Show which products and categories drive revenue.

**Dataset / CSV used**

- `product_sales.csv`

**Suggested visuals**

- Bar chart: Revenue by product
- Bar chart: Revenue by category
- Table: Product, category, units sold, revenue
- KPI card: Total product revenue
- KPI card: Units sold
- KPI card: Average unit revenue

**Fields used**

- product id/name
- category
- units sold
- revenue
- average price / average order value if available

**Filters / slicers**

- Product category
- Product name
- Date range if available

**KPIs**

- Total revenue
- Units sold
- Top product revenue
- Top category revenue

**Operational notes**

- Product reporting comes from `marts.mart_product_sales`.
- Category-level reporting is already prepared for BI.
- This page supports merchandising and sales analytics.

---

### Customer 360

**Purpose**

Provide a customer-level analytical view for segmentation, lifetime value, and purchase behavior.

**Dataset / CSV used**

- `customer_360.csv`

**Suggested visuals**

- Table: Customer, segment, lifetime value, order count
- Bar chart: Revenue by customer segment
- KPI card: Total customers
- KPI card: Average lifetime value
- KPI card: Repeat customer count
- Distribution chart: Customers by segment

**Fields used**

- customer id
- customer segment
- membership tier
- lifetime value
- total orders
- last order date
- city/state if present

**Filters / slicers**

- Customer segment
- Membership tier
- City/state
- Date range if available

**KPIs**

- Customer count
- Average lifetime value
- Repeat purchase rate
- Segment revenue
- High-value customer count

**Operational notes**

- Customer 360 uses SCD2-aware warehouse dimensions before reporting.
- The page supports customer analytics and segmentation.
- Segment-level metrics are served from a trusted mart, not hand-built in the BI layer.

---

### Marketing Funnel

**Purpose**

Show user movement through the marketing funnel and identify drop-off points.

**Dataset / CSV used**

- `marketing_funnel.csv`

**Suggested visuals**

- Funnel chart: Stage progression
- Bar chart: Event count by funnel stage
- KPI card: Total visitors/events
- KPI card: Total conversions
- KPI card: Funnel conversion rate
- Table: Stage, event count, conversion count/rate

**Fields used**

- funnel stage
- event type
- event count
- conversion count
- conversion rate
- campaign/channel if present

**Filters / slicers**

- Funnel stage
- Campaign
- Channel
- Date range if present

**KPIs**

- Event count
- Conversion count
- Conversion rate
- Drop-off rate

**Operational notes**

- This page connects customer behavior to marketing outcomes.
- The data originates from lakehouse behavioral events but is served through trusted marts.
- BI does not read raw event data directly.

## Power BI Online manual build instructions

### Upload CSV files

1. Open Power BI Service in the browser.
2. Go to the target workspace.
3. Choose upload/import options for local files.
4. Upload these CSV files from `exports/power_bi/`:
   - `revenue_daily.csv`
   - `campaign_performance.csv`
   - `product_sales.csv`
   - `customer_360.csv`
   - `marketing_funnel.csv`

### Create semantic model/report

Depending on available Power BI Service workspace features:

1. Create a semantic model from the uploaded CSV files, or create a report directly from each uploaded dataset.
2. Confirm column data types:
   - date fields as Date
   - revenue/spend/order metrics as decimal or whole number
   - IDs and names as text
3. Create report pages matching the dashboard design:
   - Executive Overview
   - Revenue Trends
   - Campaign Performance
   - Product Performance
   - Customer 360
   - Marketing Funnel

### Create visuals

Use:

- KPI cards
- Line charts
- Bar/column charts
- Tables/matrices
- Funnel chart
- Slicers

Suggested slicers:

- Date
- Campaign
- Product category
- Customer segment

### Pin visuals to dashboard

1. Open the report page.
2. Pin key visuals to a dashboard.
3. Create dashboard tiles for:
   - Total revenue
   - Total orders
   - Average order value
   - Campaign ROAS
   - Top products/categories
   - Funnel conversion rate
4. Arrange tiles from executive summary at the top to detailed analysis below.

### Refresh / re-upload flow

For this Ubuntu-friendly demo:

1. Airflow runs the lakehouse and warehouse pipeline.
2. PostgreSQL marts are refreshed.
3. `scripts/export_power_bi_marts.py` writes fresh CSV files into `exports/power_bi/`.
4. The CSVs are uploaded/refreshed in Power BI Online.
5. Dashboard visuals reflect the latest warehouse marts.

## What this demo proves

This demo proves:

- PostgreSQL marts are BI-ready.
- Power BI consumes curated reporting datasets.
- Raw/Bronze/Silver/Gold lakehouse layers are not exposed directly to BI.
- Warehouse validation and reconciliation happen before dashboard consumption.
- The reporting layer is thin, explainable, and reproducible.
