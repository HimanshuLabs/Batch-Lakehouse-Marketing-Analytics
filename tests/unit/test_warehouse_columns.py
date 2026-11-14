"""Warehouse columns carry real source values (E1, p2-data-v2).

A sweep of the warehouse found columns that were empty or invented:
  - campaign_type: no source had it, so dim_campaign and both campaign marts
    showed 'Unknown' for every campaign. It was removed; since Part J the
    source delivers it (the campaign's objective) and it is carried again,
    beside campaign_status and ab_test_group.
  - inferred members: web events whose customer, product or campaign was
    quarantined in Silver got invented dimension members ('Inferred Campaign 2'),
    while orders, items and spend with the same keys were rejected. One rule
    holds for every fact since Part J: a NULL key is the unknown member (0), and
    a present key with no valid dimension row is the invalid-reference member
    (-1). The fact is kept, with its natural ID, and counted.
  - coalesce defaults turned missing loyalty_points / is_prime_user into 0 / false.
  - every order had channel_sk 0: its channel came from the lineage column
    'source' ('orders_from_purchase_events'); it now comes from the order's
    point-in-time campaign.
  - Gold fact_web_events dropped search_query, time_on_page_sec,
    scroll_depth_percent, schema_version and source_system.
  - fact_orders.discount_amount was always 0; it is now the sum of its items'
    (original_price - discounted_price) x quantity.
  - E2: gross_amount equalled net revenue, so gross - discount did not; it is now
    the sum of original_price x quantity over the order's loaded items, and
    reconciliation checks gross - discount = net for every complete order.

No Postgres runs in the unit tests, so the SQL is checked as text; the
behaviour is checked on a real build by tests/integration/test_warehouse.py.
"""
from __future__ import annotations

import re

from pyspark.sql import functions as F

from conftest import BRONZE_TEMPLATES, CODE_ROOT

WAREHOUSE_DIR = CODE_ROOT / "sql" / "warehouse"
TABLES_SQL = WAREHOUSE_DIR / "create_warehouse_tables.sql"
MARTS_SQL = WAREHOUSE_DIR / "create_marts.sql"
GOLD_JOB = "spark-batch/gold_transformations.py"


def sql_code(path) -> str:
    """The file without -- comments."""
    return "\n".join(line.split("--", 1)[0] for line in path.read_text(encoding="utf-8").splitlines())


def view_body(name: str) -> str:
    code = sql_code(MARTS_SQL)
    match = re.search(rf"CREATE OR REPLACE VIEW {re.escape(name)} AS(.*?);\s*\n", code, re.S)
    assert match, f"{name} is not defined"
    return match.group(1)


def table_ddl(name: str) -> str:
    match = re.search(rf"CREATE TABLE {re.escape(name)} \((.*?)\n\);", sql_code(TABLES_SQL), re.S)
    assert match, f"{name} is not defined"
    return match.group(1)


# Part J: the attributes that were removed as invented constants are real
# source data now. The generator produces each one; these are the columns that
# carry it, relation by relation.
REAL_ATTRIBUTES = {
    "warehouse.dim_product": ["brand", "product_status"],
    "warehouse.dim_campaign": ["campaign_type", "campaign_status", "ab_test_group"],
    "warehouse.fact_orders": ["order_status", "tax_amount", "shipping_amount", "total_amount"],
    "warehouse.fact_campaign_spend": ["conversions", "attributed_revenue"],
    "marts.mart_product_sales": ["brand", "product_status", "cancelled_units", "returned_units", "returned_amount"],
    "marts.mart_revenue_daily": ["tax_amount", "shipping_amount", "total_amount", "cancelled_order_count",
                                 "cancelled_amount", "returned_order_count", "returned_amount"],
    "marts.mart_campaign_performance": ["campaign_type", "campaign_status", "ab_test_group", "ad_platform_conversions",
                                        "platform_attributed_revenue", "platform_roas", "cancelled_order_count",
                                        "cancelled_amount", "returned_order_count", "returned_amount"],
    "marts.mart_customer_360": ["cancelled_order_count", "cancelled_amount", "returned_order_count", "returned_amount"],
}


def test_the_real_attributes_are_carried_by_the_warehouse_and_the_marts():
    for relation, columns in REAL_ATTRIBUTES.items():
        body = table_ddl(relation) if relation.startswith("warehouse.") else view_body(relation)
        for column in columns:
            assert re.search(rf"\b{column}\b", body), f"{relation}.{column}"
    code = sql_code(TABLES_SQL)
    # Loaded from the source value, never defaulted: a missing value fails the NOT NULL column.
    assert "lower(nullif(r->>'order_status', '')) AS order_status," in code
    assert "order_status TEXT NOT NULL," in code and "DEFAULT 'completed'" not in code
    assert "CONSTRAINT chk_fact_orders_status CHECK (order_status IN ('completed', 'cancelled', 'returned'))" in code
    for column in ("tax_amount NUMERIC(18, 2) NOT NULL,", "shipping_amount NUMERIC(18, 2) NOT NULL,",
                   "total_amount NUMERIC(18, 2) NOT NULL,", "conversions BIGINT NOT NULL,",
                   "attributed_revenue NUMERIC(18, 2) NOT NULL,"):
        assert column in code, column
    assert "warehouse.safe_numeric(r->>'total_amount') AS total_amount," in code
    assert "warehouse.safe_numeric(r->>'cart_value') AS net_revenue," in code, "one name, one meaning"
    assert "coalesce(nullif(r->>'status', ''), nullif(r->>'product_status', '')) AS product_status," in code
    assert "nullif(r->>'campaign_type', '') AS campaign_type," in code
    # J3: the ad feed is per campaign, day and region, so the spend fact has its region again, as real data.
    assert "region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk)," in table_ddl("warehouse.fact_campaign_spend")
    assert "ON dr.region_nk = warehouse.region_nk(NULL, b.state, b.country)" in code


def test_revenue_in_every_mart_counts_orders_that_were_not_cancelled_less_refunds():
    # K1: a sale is an order that was not cancelled; a returned order stays a
    # sale and its refund is subtracted.
    sold = "FILTER (WHERE fo.order_status <> 'cancelled')"
    customer = view_body("marts.mart_customer_360")
    for metric in ("COUNT(DISTINCT fo.order_id)", "SUM(fo.gross_amount)", "SUM(fo.discount_amount)",
                   "SUM(fo.net_revenue)", "AVG(fo.net_revenue)"):
        assert f"{metric} {sold}" in customer, f"mart_customer_360: {metric}"
    assert "AND fo.order_status <> 'cancelled'" in customer, "units purchased are those of orders that were not cancelled"
    assert f"COALESCE(SUM(fo.net_revenue) {sold}, 0) - COALESCE(SUM(fo.refund_amount), 0) AS lifetime_value," in customer
    campaign = view_body("marts.mart_campaign_performance")
    for metric in ("COUNT(DISTINCT fo.order_id)", "SUM(fo.net_revenue)", "AVG(fo.net_revenue)"):
        assert f"{metric} {sold}" in campaign, f"mart_campaign_performance: {metric}"
    assert f"COALESCE(SUM(fo.net_revenue) {sold}, 0) - COALESCE(SUM(fo.refund_amount), 0) AS warehouse_attributed_revenue," in campaign
    product = view_body("marts.mart_product_sales")
    for metric in ("COUNT(DISTINCT foi.order_id)", "SUM(foi.quantity)", "SUM(foi.line_revenue)", "AVG(foi.unit_price)"):
        assert f"{metric} {sold}" in product, f"mart_product_sales: {metric}"
    assert f"COALESCE(SUM(foi.line_revenue) {sold}, 0) - COALESCE(SUM(foi.refund_amount), 0) AS product_revenue," in product
    daily = view_body("marts.mart_revenue_daily")
    assert "(fo.order_status <> 'cancelled') AS sold," in daily
    assert "warehouse.date_sk(warehouse.business_date(fo.status_timestamp)) AS order_date_sk," in daily, \
        "a return is recognised on the date it happened"
    # No aggregate over all orders is left in the daily mart.
    unfiltered = re.findall(r"(?:SUM|COUNT)\((?:DISTINCT )?oe\.[a-z_]+\)(?! FILTER)", daily)
    assert unfiltered == [], unfiltered
    for status in ("cancelled", "returned"):
        for mart in (customer, campaign):
            assert f"FILTER (WHERE fo.order_status = '{status}')" in mart
    assert "FILTER (WHERE oe.order_status = 'cancelled')" in daily and "FILTER (WHERE oe.is_return)" in daily


def test_both_export_routes_and_dbt_carry_the_real_attributes():
    gold = (CODE_ROOT / "spark-batch" / "gold_transformations.py").read_text(encoding="utf-8")
    for column in ('"brand",', '"product_status",', '"campaign_type",', '"order_status",', '"tax_amount",',
                   '"shipping_amount",', '"total_amount",', '"conversions",', '"attributed_revenue",'):
        assert column in gold, column
    # Route B exports the SCD2 Gold marts and facts with SELECT *.
    marts = (CODE_ROOT / "spark-batch" / "scd2_gold_marts.py").read_text(encoding="utf-8")
    for column in ('"brand",', '"status",', '"campaign_type",', '"ad_platform_conversions"', '"platform_attributed_revenue"',
                   '"cancelled_orders_count"', '"returned_orders_count"', '"cancelled_units"', '"returned_units"'):
        assert column in marts, column
    assert '"order_status"' not in (CODE_ROOT / "spark-batch" / "point_in_time_fact_joins.py").read_text(encoding="utf-8"), \
        "the order status comes from the source, the job must not invent one"
    # Route A exports each mart with SELECT *, and the dbt models are SELECT * views.
    assert 'query = f"SELECT * FROM {mart.table_name} ORDER BY {mart.order_by};"' in \
        (CODE_ROOT / "scripts" / "export_power_bi_marts.py").read_text(encoding="utf-8")
    for model in ("warehouse/dim_product", "warehouse/dim_campaign", "warehouse/fact_orders", "marts/mart_revenue_daily",
                  "marts/mart_campaign_performance", "marts/mart_product_sales"):
        assert re.search(r"select\s+\*\s+from", (CODE_ROOT / "dbt" / "models" / f"{model}.sql").read_text(encoding="utf-8"))
    schema = (CODE_ROOT / "dbt" / "models" / "schema.yml").read_text(encoding="utf-8")
    for values in ("['completed', 'cancelled', 'returned']", "['active', 'discontinued', 'Unknown', 'Invalid reference']",
                   "['awareness', 'consideration', 'conversion', 'retention', 'Unknown', 'Invalid reference']"):
        assert values in schema, values


def test_the_constant_column_allow_list_holds_only_real_source_values():
    source = (CODE_ROOT / "tests" / "integration" / "test_warehouse.py").read_text(encoding="utf-8")
    allowed = re.search(r"CONSTANT_COLUMNS_ALLOWED = \{\n(.*?)\n\}", source, re.S).group(1)
    entries = re.findall(r'\("([^"]+)", "([^"]+)"\): "([^"]+)"', allowed)
    assert len(entries) == allowed.count("\n") + 1, "every entry is one line: (relation, column): reason"
    assert sorted((relation, column) for relation, column, _ in entries) == [
        ("*", "warehouse_loaded_at"),
        ("marts.mart_marketing_funnel", "year_number"),
        ("marts.mart_revenue_daily", "year_number"),
        ("warehouse.fact_conversions", "conversion_type"),
        ("warehouse.fact_web_events", "schema_version"),
        ("warehouse.fact_web_events", "source_system"),
    ]
    # Each one left is a value the source really delivers (constant only in the sample).
    assert not [reason for _, _, reason in entries if re.search(r"no source|not in the source|has no ", reason)]


def test_a_bad_reference_goes_to_the_invalid_member_and_no_fact_is_rejected_in_the_warehouse():
    code = sql_code(TABLES_SQL)
    assert re.search(r"inferred", code, re.IGNORECASE) is None, "no member is invented per key"
    # Each SCD2 dimension gets rows from two places only: its unknown and
    # invalid-reference members (one INSERT) and its staging loader.
    for dimension in ("dim_customer", "dim_product", "dim_campaign"):
        assert code.count(f"INSERT INTO warehouse.{dimension} (") == 2, dimension
        key = dimension.replace("dim_", "") + "_sk"
        assert re.search(rf"INSERT INTO warehouse\.{dimension} \(\s+{key},.*?VALUES \(\s+0, 'UNKNOWN',.*?\), \(\s+"
                         r"(?:--[^\n]*\n\s+)*-1, 'INVALID', 'Invalid reference'", (TABLES_SQL).read_text(encoding="utf-8"), re.S), dimension
    assert not (WAREHOUSE_DIR / "repair_web_event_surrogate_keys.sql").exists()

    # One rule for every fact: the covering version, else 0 for a known ID, else -1.
    assert "SELECT coalesce(resolved_sk, CASE WHEN natural_id_in_dimension THEN 0 ELSE -1 END);" in code
    resolved = re.findall(r"warehouse\.member_sk\((\w+)\.(\w+), b\.(\w+) IN \(SELECT known\.\3 FROM warehouse\.(\w+) known\)\)", code)
    assert sorted((dimension, key) for _, _, key, dimension in resolved) == [
        ("dim_campaign", "campaign_id"), ("dim_campaign", "campaign_id"), ("dim_campaign", "campaign_id"),
        ("dim_customer", "customer_id"), ("dim_customer", "customer_id"),
        ("dim_product", "product_id"), ("dim_product", "product_id")], "orders x2, items, spend, web events x3"
    # The reject table and its path are gone: every staging web event is a fact.
    everything = "".join(sql_code(path) for path in WAREHOUSE_DIR.glob("*.sql"))
    assert "fact_web_events_rejects" not in everything and "reject_reason" not in code
    reconciliation = sql_code(WAREHOUSE_DIR / "reconciliation_report.sql")
    assert "'Staging web event row count equals warehouse fact_web_events row count'" in reconciliation
    for fact, dimension in (("fact_orders", "customer"), ("fact_orders", "campaign"), ("fact_order_items", "product"),
                            ("fact_campaign_spend", "campaign"), ("fact_web_events", "customer"),
                            ("fact_web_events", "product"), ("fact_web_events", "campaign")):
        assert f"('{fact}', '{dimension}')" in reconciliation, (fact, dimension)
    assert "format('Invalid %s references in %s are on the invalid-reference member'," in reconciliation


def test_reconciliation_compares_scd2_version_counts_and_the_order_total():
    reconciliation = sql_code(WAREHOUSE_DIR / "reconciliation_report.sql")
    assert "format('Gold %s SCD2 version count equals warehouse dim_%s version count'," in reconciliation
    for dimension, staging in (("customer", "customers"), ("product", "products"), ("campaign", "campaigns")):
        assert f"('{dimension}', 'staging.stg_gold_dim_{staging}_scd2')" in reconciliation
    assert "'SELECT COUNT(*)::NUMERIC FROM warehouse.dim_%1$s WHERE %1$s_sk NOT IN (0, -1)'" in reconciliation
    assert "'Order total equals net revenue plus tax plus shipping for every order'" in reconciliation
    assert "WHERE total_amount <> net_revenue + tax_amount + shipping_amount;" in reconciliation


def test_missing_customer_attributes_stay_null():
    code = sql_code(TABLES_SQL)
    assert "warehouse.safe_int(r->>'loyalty_points') AS loyalty_points," in code
    assert "warehouse.safe_bool(r->>'is_prime_user') AS is_prime_user," in code
    ddl = table_ddl("warehouse.dim_customer")
    assert re.search(r"\bloyalty_points INTEGER,", ddl) and re.search(r"\bis_prime_user BOOLEAN,", ddl)


def test_no_channel_comes_from_the_lineage_source_column():
    for path in (TABLES_SQL, WAREHOUSE_DIR / "reconciliation_report.sql"):
        for line in sql_code(path).splitlines():
            if "r->>'source'" in line:
                # The only use left: the lineage column itself, as source_system.
                assert line.strip() == (
                    "coalesce(nullif(r->>'source', ''), nullif(r->>'source_system', '')) AS source_system,"
                ), f"{path.name}: {line.strip()}"
    code = sql_code(TABLES_SQL)
    assert "ON dch.channel_nk = warehouse.channel_nk(dcamp.channel)" in code, \
        "an order's channel must be its point-in-time campaign's channel"


def test_order_gross_and_discount_are_the_sums_of_their_items():
    code = re.sub(r"\s+", " ", sql_code(TABLES_SQL))
    assert ("warehouse.safe_numeric(r->>'original_price') "
            "* coalesce(warehouse.safe_int(r->>'quantity'), 1) AS gross_amount") in code
    assert ("(warehouse.safe_numeric(r->>'original_price') - warehouse.safe_numeric(r->>'discounted_price')) "
            "* coalesce(warehouse.safe_int(r->>'quantity'), 1) AS discount_amount") in code
    # Every order, with or without loaded items, takes both sums from its items.
    assert "SET gross_amount = order_amounts.gross_amount, discount_amount = order_amounts.discount_amount," in code
    assert ("COALESCE(SUM(i.gross_amount), 0) AS gross_amount, COALESCE(SUM(i.discount_amount), 0) AS discount_amount "
            "FROM warehouse.fact_orders o LEFT JOIN warehouse.fact_order_items i") in code


def test_reconciliation_checks_gross_minus_discount_equals_net():
    code = re.sub(r"\s+", " ", sql_code(WAREHOUSE_DIR / "reconciliation_report.sql"))
    assert "'Order gross minus discount equals net revenue for every complete order'" in code
    assert "WHERE o.line_total = o.net_revenue AND o.gross_amount - o.discount_amount <> o.net_revenue" in code


def test_gold_web_events_keep_the_engagement_and_lineage_fields(spark, load_job):
    gold = load_job(GOLD_JOB, BATCH_DATE="2026-06-18")
    silver_row = {**BRONZE_TEMPLATES["web_events"], "source_system": "web_events"}
    events = spark.createDataFrame([silver_row]).withColumn("event_timestamp", F.to_timestamp("event_timestamp")) \
        .withColumn("ingestion_date", F.to_date(F.lit("2026-06-18")))

    row = gold.build_fact_web_events(events).collect()[0].asDict()

    assert {key: row.get(key) for key in (
        "search_query", "time_on_page_sec", "scroll_depth_percent", "schema_version", "source_system"
    )} == {"search_query": "enim", "time_on_page_sec": 783, "scroll_depth_percent": 40,
           "schema_version": "v1", "source_system": "web_events"}


# --- Part J3 ------------------------------------------------------------------------

def consumer_contract_module():
    import importlib.util

    path = CODE_ROOT / "tests" / "integration" / "test_warehouse.py"
    spec = importlib.util.spec_from_file_location("p2_consumer_contract", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_consumer_contract_names_columns_that_exist_and_catches_every_breaking_change():
    module = consumer_contract_module()
    contract = module.CONSUMER_CONTRACT
    assert set(contract) == {"marts.mart_customer_360", "marts.mart_campaign_performance", "marts.mart_marketing_funnel",
                             "marts.mart_product_sales", "warehouse.fact_web_events"}, "the five relations P4 reads"
    assert module.CONSUMER_SCHEMAS == {"P5 contracts/project2_project3_sources.yaml": ["staging", "warehouse", "marts", "audit"]}
    # Without a database: every column a consumer reads is in the SQL that builds the relation.
    for relation, expected in contract.items():
        body = table_ddl(relation) if relation.startswith("warehouse.") else view_body(relation)
        for column in expected["columns"]:
            assert re.search(rf"\b{column}\b", body), f"{relation}.{column} is read by a consumer"
        assert set(expected["key"]) <= set(expected["columns"])

    def check(columns, rows=None, duplicates=None):
        return module.contract_violations(contract, columns, rows or {relation: 10 for relation in contract}, duplicates or {})

    current = {relation: dict(expected["columns"]) for relation, expected in contract.items()}
    assert check(current) == []
    # Adding a column is free.
    assert check({**current, "marts.mart_customer_360": {**current["marts.mart_customer_360"], "member_type": "text"}}) == []
    # Removing, renaming or retyping a column a consumer reads fails, and so does an empty relation or a broken key.
    campaign = current["marts.mart_campaign_performance"]
    removed = {key: value for key, value in campaign.items() if key != "ad_platform_conversions"}
    assert check({**current, "marts.mart_campaign_performance": removed}) == [
        "marts.mart_campaign_performance.ad_platform_conversions: removed or renamed"]
    renamed = {("spend" if key == "total_spend" else key): value for key, value in campaign.items()}
    assert check({**current, "marts.mart_campaign_performance": renamed}) == [
        "marts.mart_campaign_performance.total_spend: removed or renamed"]
    retyped = {**current["warehouse.fact_web_events"], "customer_sk": "integer"}
    assert check({**current, "warehouse.fact_web_events": retyped}) == [
        "warehouse.fact_web_events.customer_sk: type integer, the consumer reads bigint"]
    assert check({key: value for key, value in current.items() if key != "marts.mart_product_sales"}) == [
        "marts.mart_product_sales: the relation is gone"]
    assert check(current, rows={relation: 0 if relation == "marts.mart_marketing_funnel" else 5 for relation in contract}) == [
        "marts.mart_marketing_funnel: no rows (P4 fails on an empty source)"]
    assert check(current, duplicates={"marts.mart_customer_360": 2}) == [
        "marts.mart_customer_360: 2 duplicate or NULL key(s) ['customer_id']"]


def test_ad_spend_is_regional_and_checked_against_the_campaign_day_total():
    silver = (CODE_ROOT / "spark-batch" / "silver_transformations.py").read_text(encoding="utf-8")
    assert '"Invalid ad spend record: region (country, state) is missing"' in silver
    assert "campaign_day_spend is missing or less than the row's spend" in silver
    gold = (CODE_ROOT / "spark-batch" / "gold_transformations.py").read_text(encoding="utf-8")
    for column in ('"country",', '"state",', '"campaign_day_spend",'):
        assert column in gold, column
    code = sql_code(TABLES_SQL)
    assert "'staging.stg_gold_fact_campaign_spend_scd2'," in code[code.index("FOREACH source_table IN ARRAY ARRAY["):code.index("LOOP")], \
        "the ad feed's regions are loaded into dim_region"
    reconciliation = sql_code(WAREHOUSE_DIR / "reconciliation_report.sql")
    assert "'Regional ad spend rows sum to the campaign-day total'" in reconciliation
    assert "HAVING SUM(f.spend_amount) <> MAX(s.campaign_day_spend)::NUMERIC(18, 2)" in reconciliation
    assert "GROUP BY campaign_id, spend_date, region_sk HAVING COUNT(*) > 1" in reconciliation
    # campaign_day_spend is a repeated total: it stays out of the fact, where it would be summed.
    assert "campaign_day_spend" not in table_ddl("warehouse.fact_campaign_spend")
    pack = (WAREHOUSE_DIR / "dashboard_query_pack.sql").read_text(encoding="utf-8")
    assert "Ad spend and return on ad spend by channel" in pack and "Ad spend and return on ad spend by region" in pack


def test_every_mart_keeps_its_member_rows_flagged_and_validation_ties_out_to_the_facts():
    marts_code = sql_code(MARTS_SQL)
    for mart, key in (("marts.mart_customer_360", "cc.customer_id"), ("marts.mart_campaign_performance", "cc.campaign_id"),
                      ("marts.mart_product_sales", "cp.product_id")):
        body = view_body(mart)
        assert (f"CASE {key} WHEN 'UNKNOWN' THEN 'unknown_member' WHEN 'INVALID' THEN 'invalid_reference' "
                "ELSE 'known' END AS member_type") in re.sub(r"\s+", " ", body), mart
        assert not re.search(r"WHERE c[cp]\.\w+_id NOT IN \('UNKNOWN', 'INVALID'\)", body), f"{mart} drops its member rows"
    assert "ELSE 'known' END AS member_type" in re.sub(r"\s+", " ", view_body("marts.mart_marketing_funnel"))
    for check in ("customer_lifetime_value_matches_fact_orders", "customer_units_match_fact_order_items",
                  "campaign_revenue_matches_fact_orders", "product_units_match_fact_order_items",
                  "campaign_platform_results_match_fact_campaign_spend", "product_revenue_matches_fact_order_items",
                  "campaign_spend_matches_fact_campaign_spend", "daily_revenue_matches_fact_orders"):
        assert f"'{check}'" in marts_code, check
    # The validation compares each mart with the whole fact: no 'BI-visible' subset is left.
    validation = marts_code[marts_code.index("CREATE TABLE audit.mart_validation_report"):]
    assert "NOT IN ('UNKNOWN', 'INVALID')" not in validation and "BI-visible" not in validation
    gold_marts = (CODE_ROOT / "spark-batch" / "scd2_gold_marts.py").read_text(encoding="utf-8")
    assert gold_marts.count("with_member_rows(") == 4, "defined once, used by the campaign, customer and product marts"
    # The value segment's boundaries come from the data.
    assert "NTILE(3) OVER (" in view_body("marts.mart_customer_360") and ">= 1000 THEN 'High Value'" not in marts_code


def test_total_amount_has_one_meaning_in_every_layer():
    generator = (CODE_ROOT / "data-generator" / "generate_data.py").read_text(encoding="utf-8")
    silver = (CODE_ROOT / "spark-batch" / "silver_transformations.py").read_text(encoding="utf-8")
    pit = (CODE_ROOT / "spark-batch" / "point_in_time_fact_joins.py").read_text(encoding="utf-8")
    gold = (CODE_ROOT / "spark-batch" / "gold_transformations.py").read_text(encoding="utf-8")
    gold_marts = (CODE_ROOT / "spark-batch" / "scd2_gold_marts.py").read_text(encoding="utf-8")
    everything = generator + silver + pit + gold + gold_marts + "".join(
        path.read_text(encoding="utf-8") for path in WAREHOUSE_DIR.glob("*.sql"))
    assert not re.search(r"\border_total\b(?!_)", everything), "one name for the charged amount: total_amount"
    assert 'total_amount is not cart_value + tax_amount + shipping_amount"' in silver
    # Gold no longer makes a total_amount out of the net value, and its marts count revenue from cart_value.
    assert 'add_column_from_candidates(result, "total_amount"' not in pit
    assert '"total_amount"' not in gold_marts and 'F.col("cart_value")' in gold_marts
    code = sql_code(TABLES_SQL)
    assert "warehouse.safe_numeric(r->>'total_amount') AS total_amount," in code
    assert "warehouse.safe_numeric(r->>'cart_value') AS net_revenue," in code
    assert "r->>'total_amount')," not in code, "total_amount is no fallback for gross or net revenue"
    reconciliation = sql_code(WAREHOUSE_DIR / "reconciliation_report.sql")
    assert "to_jsonb(src)->>'total_amount'" not in reconciliation
    for pack in ("sql/scd2_gold_mart_validation_queries.sql", "sql/scd2_duckdb_analytics_queries.sql"):
        assert "SUM(total_amount)" not in (CODE_ROOT / pack).read_text(encoding="utf-8"), pack
