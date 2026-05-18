from __future__ import annotations

from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
REPORTS_DIR = PROJECT_ROOT / "logs" / "duckdb_gold_reports"


TABLES = {
    "dim_customers": GOLD_DIR / "dim_customers" / "**" / "*.parquet",
    "dim_products": GOLD_DIR / "dim_products" / "**" / "*.parquet",
    "dim_campaigns": GOLD_DIR / "dim_campaigns" / "**" / "*.parquet",
    "fact_orders": GOLD_DIR / "fact_orders" / "**" / "*.parquet",
    "fact_order_items": GOLD_DIR / "fact_order_items" / "**" / "*.parquet",
    "fact_ad_spend": GOLD_DIR / "fact_ad_spend" / "**" / "*.parquet",
    "fact_web_events": GOLD_DIR / "fact_web_events" / "**" / "*.parquet",
    "mart_campaign_performance": GOLD_DIR / "mart_campaign_performance" / "**" / "*.parquet",
    "mart_product_performance": GOLD_DIR / "mart_product_performance" / "**" / "*.parquet",
    "mart_customer_value": GOLD_DIR / "mart_customer_value" / "**" / "*.parquet",
    "mart_marketing_funnel": GOLD_DIR / "mart_marketing_funnel" / "**" / "*.parquet",
}


ANALYTICS_QUERIES = {
    "01_top_campaigns_by_revenue": """
        SELECT
            campaign_id,
            campaign_name,
            traffic_source,
            total_ad_spend,
            total_revenue,
            roas,
            conversion_rate,
            total_orders,
            total_sessions
        FROM mart_campaign_performance
        ORDER BY total_revenue DESC
        LIMIT 10;
    """,
    "02_best_roas_campaigns": """
        SELECT
            campaign_id,
            campaign_name,
            traffic_source,
            total_ad_spend,
            total_revenue,
            roas,
            ctr,
            cpc
        FROM mart_campaign_performance
        WHERE total_ad_spend > 0
        ORDER BY roas DESC
        LIMIT 10;
    """,
    "03_top_products_by_revenue": """
        SELECT
            product_id,
            product_name,
            category,
            total_units_sold,
            total_product_revenue,
            product_views,
            add_to_cart_events,
            purchase_events,
            view_to_cart_rate,
            cart_to_purchase_rate
        FROM mart_product_performance
        ORDER BY total_product_revenue DESC
        LIMIT 10;
    """,
    "04_customer_lifetime_value_leaders": """
        SELECT
            customer_id,
            user_name,
            email,
            membership_tier,
            user_segment,
            is_prime_user,
            total_orders,
            customer_lifetime_value,
            avg_order_value,
            total_sessions,
            avg_engagement_score
        FROM mart_customer_value
        ORDER BY customer_lifetime_value DESC
        LIMIT 10;
    """,
    "05_marketing_funnel_by_channel": """
        SELECT
            traffic_source,
            SUM(total_events) AS total_events,
            SUM(total_sessions) AS total_sessions,
            SUM(page_views) AS page_views,
            SUM(product_views) AS product_views,
            SUM(add_to_cart_events) AS add_to_cart_events,
            SUM(checkout_events) AS checkout_events,
            SUM(purchase_events) AS purchase_events,
            ROUND(
                CASE
                    WHEN SUM(total_sessions) > 0
                    THEN SUM(purchase_events)::DOUBLE / SUM(total_sessions)
                    ELSE 0
                END,
                6
            ) AS session_conversion_rate
        FROM mart_marketing_funnel
        GROUP BY traffic_source
        ORDER BY session_conversion_rate DESC;
    """,
    "06_device_performance": """
        SELECT
            device_type,
            SUM(total_sessions) AS total_sessions,
            SUM(product_views) AS product_views,
            SUM(add_to_cart_events) AS add_to_cart_events,
            SUM(checkout_events) AS checkout_events,
            SUM(purchase_events) AS purchase_events,
            ROUND(AVG(avg_api_latency_ms), 2) AS avg_api_latency_ms,
            ROUND(AVG(avg_page_load_time_ms), 2) AS avg_page_load_time_ms,
            ROUND(AVG(session_conversion_rate), 6) AS avg_session_conversion_rate
        FROM mart_marketing_funnel
        GROUP BY device_type
        ORDER BY avg_session_conversion_rate DESC;
    """,
    "07_revenue_by_payment_method": """
        SELECT
            payment_method,
            COUNT(DISTINCT order_id) AS total_orders,
            ROUND(SUM(order_amount), 2) AS total_revenue,
            ROUND(AVG(order_amount), 2) AS avg_order_value
        FROM fact_orders
        GROUP BY payment_method
        ORDER BY total_revenue DESC;
    """,
    "08_fraud_risk_orders": """
        SELECT
            order_id,
            customer_id,
            campaign_id,
            order_timestamp,
            payment_method,
            order_amount,
            fraud_score,
            city,
            country
        FROM fact_orders
        WHERE fraud_score >= 0.80
        ORDER BY fraud_score DESC, order_amount DESC
        LIMIT 25;
    """,
    "09_category_performance": """
        SELECT
            category,
            COUNT(DISTINCT product_id) AS product_count,
            SUM(total_units_sold) AS total_units_sold,
            ROUND(SUM(total_product_revenue), 2) AS total_revenue,
            SUM(product_views) AS product_views,
            SUM(add_to_cart_events) AS add_to_cart_events,
            SUM(purchase_events) AS purchase_events,
            ROUND(AVG(view_to_cart_rate), 6) AS avg_view_to_cart_rate,
            ROUND(AVG(cart_to_purchase_rate), 6) AS avg_cart_to_purchase_rate
        FROM mart_product_performance
        GROUP BY category
        ORDER BY total_revenue DESC;
    """,
    "10_ab_test_performance": """
        SELECT
            ab_test_group,
            SUM(total_events) AS total_events,
            SUM(total_sessions) AS total_sessions,
            SUM(purchase_events) AS purchase_events,
            ROUND(
                CASE
                    WHEN SUM(total_sessions) > 0
                    THEN SUM(purchase_events)::DOUBLE / SUM(total_sessions)
                    ELSE 0
                END,
                6
            ) AS session_conversion_rate,
            ROUND(AVG(avg_engagement_score), 4) AS avg_engagement_score
        FROM mart_marketing_funnel
        GROUP BY ab_test_group
        ORDER BY session_conversion_rate DESC;
    """,
}


def create_connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(database=":memory:")

    for table_name, parquet_glob in TABLES.items():
        path = str(parquet_glob)

        if not list(parquet_glob.parent.parent.glob("**/*.parquet")):
            raise FileNotFoundError(f"Missing Parquet files for {table_name}: {path}")

        connection.execute(
            f"""
            CREATE OR REPLACE VIEW {table_name} AS
            SELECT * FROM read_parquet('{path}');
            """
        )

    return connection


def run_queries(connection: duckdb.DuckDBPyConnection) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print("Starting DuckDB Gold analytics")
    print(f"Gold directory: {GOLD_DIR}")
    print(f"Reports directory: {REPORTS_DIR}")

    for query_name, sql in ANALYTICS_QUERIES.items():
        output_path = REPORTS_DIR / f"{query_name}.csv"

        result = connection.execute(sql).fetchdf()
        result.to_csv(output_path, index=False)

        print(f"{query_name}: rows={len(result)} -> {output_path}")

    print("DuckDB Gold analytics completed successfully.")


def main() -> None:
    connection = create_connection()

    try:
        run_queries(connection)
    finally:
        connection.close()


if __name__ == "__main__":
    main()