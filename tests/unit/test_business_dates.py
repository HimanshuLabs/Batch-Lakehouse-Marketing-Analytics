"""Business dates are Asia/Kolkata calendar days (N-12b, p2-data-v2).

Gold derives order_date and event_date in Asia/Kolkata, but the warehouse cast
its UTC timestamps straight to DATE, so an order between 00:00 and 05:30 IST
landed on the previous day in date keys, daily revenue and the funnel. Every
business date the warehouse derives from a timestamp now goes through
warehouse.business_date (UTC -> Asia/Kolkata), and "today" is
warehouse.business_today(). Stored timestamps stay UTC.

No Postgres runs in the unit tests, so these check the SQL text; the behaviour
is checked on a real warehouse by tests/integration/test_warehouse.py
(test_business_dates_are_ist_dates_like_gold), which compares every warehouse
order date key with Gold's Asia/Kolkata order_date.
"""
from __future__ import annotations

import re

from conftest import CODE_ROOT

TABLES_SQL = CODE_ROOT / "sql" / "warehouse" / "create_warehouse_tables.sql"
MARTS_SQL = CODE_ROOT / "sql" / "warehouse" / "create_marts.sql"


def sql_code(path) -> str:
    """The file without -- comments."""
    return "\n".join(line.split("--", 1)[0] for line in path.read_text(encoding="utf-8").splitlines())


def test_business_date_converts_utc_to_asia_kolkata():
    code = sql_code(TABLES_SQL)
    body = re.search(r"FUNCTION warehouse\.business_date\(utc_ts TIMESTAMP\).*?\$\$(.*?)\$\$", code, re.S)
    assert body, "warehouse.business_date(utc_ts TIMESTAMP) is not defined"
    assert re.sub(r"\s+", " ", body.group(1)).strip() == (
        "SELECT ((utc_ts AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata')::DATE;"
    )
    today = re.search(r"FUNCTION warehouse\.business_today\(\).*?\$\$(.*?)\$\$", code, re.S)
    assert today and "now() AT TIME ZONE 'Asia/Kolkata'" in today.group(1)


def test_no_timestamp_is_cast_straight_to_a_date():
    # A UTC timestamp cast to DATE gives the UTC day.
    for path in (TABLES_SQL, MARTS_SQL):
        code = sql_code(path)
        casts = re.findall(r"(?:_ts|_timestamp|\)\))::DATE", code) + re.findall(r"safe_timestamp\([^)]*\)::DATE", code)
        assert casts == [], f"{path.name}: {casts}"


def test_every_fact_date_key_is_a_business_date():
    code = sql_code(TABLES_SQL)
    date_keys = re.findall(r"warehouse\.date_sk\(([^\n]*?)\)\s*(?:,\s*0\)\s*)?AS (\w+_date_sk)", code)
    assert {name for _, name in date_keys} == {"order_date_sk", "event_date_sk", "spend_date_sk"}
    for argument, name in date_keys:
        if name == "spend_date_sk":
            # spend_date is a calendar date from the ad platform, not a timestamp.
            assert argument == "b.spend_date"
            continue
        assert argument.startswith("warehouse.business_date("), f"{name} is derived from {argument}"
    # dim_date is built with the same derivation, so every date key has its row.
    dim_date_source = re.search(r"SELECT coalesce\((.*?)\) AS d\b", code, re.S).group(1)
    timestamp_sources = re.findall(r"warehouse\.safe_timestamp\(r->>'(\w+)'\)", dim_date_source)
    assert timestamp_sources == ["order_timestamp", "event_timestamp", "event_time", "order_date"]
    assert dim_date_source.count("warehouse.business_date(") == len(timestamp_sources)


def test_days_since_last_order_counts_business_days():
    code = sql_code(MARTS_SQL)
    assert re.search(r"\bCURRENT_DATE\b", code, re.IGNORECASE) is None, "CURRENT_DATE is the session's (UTC) day"
    assert (
        "warehouse.business_today() - warehouse.business_date(o.last_order_timestamp)" in code
    ), "days_since_last_order must compare two Asia/Kolkata dates"
