"""data_quality_summary.csv comes only from verified checks.

It used to be a hand-made file that no code produced, with numbers that went
stale. The power_bi_export task now builds it from warehouse reconciliation,
the latest SCD2 validation run, the Silver quality report and the
point-in-time fact report, and refuses to write it when an input is missing.
Part J (p2-data-v2): no fact is rejected for a bad dimension reference any
more. It is kept on the invalid-reference member, and the summary shows how
many there are per fact and dimension, from the reconciliation (warehouse) and
from the point-in-time report (Gold).
"""
from __future__ import annotations

import pytest

EXPORT_JOB = "sql/export_power_bi_dataset.py"

WEB_EVENT_CHECK = "Staging web event row count equals warehouse fact_web_events row count"
# (dimension, fact): facts on the invalid-reference member, as the reconciliation counts them.
INVALID_REFERENCES = {
    ("customer", "fact_orders"): 11, ("campaign", "fact_orders"): 63, ("product", "fact_order_items"): 66,
    ("campaign", "fact_campaign_spend"): 64, ("customer", "fact_web_events"): 67,
    ("product", "fact_web_events"): 37, ("campaign", "fact_web_events"): 400,
}


def invalid_reference_checks(status: str = "PASS", counts: dict | None = None) -> list[dict]:
    return [{"check_name": f"Invalid {dimension} references in {fact} are on the invalid-reference member",
             "status": status, "count_difference": 0, "amount_difference": None, "target_count": count}
            for (dimension, fact), count in (counts or INVALID_REFERENCES).items()]


def with_web_event_check(reconciliation: list[dict], status: str = "PASS") -> list[dict]:
    """The checks the summary requires: every web event loaded, and the seven invalid-reference counts."""
    return reconciliation + [{"check_name": WEB_EVENT_CHECK, "status": status,
                              "count_difference": 0, "amount_difference": None}] + invalid_reference_checks(status)


def test_data_quality_summary_is_built_from_verified_checks(load_job):
    export = load_job(EXPORT_JOB, BATCH_DATE="2026-06-18")

    reconciliation = with_web_event_check([
        {"check_name": "Gold order count equals warehouse fact_orders count", "status": "PASS",
         "count_difference": 0, "amount_difference": None},
        {"check_name": "Gold revenue equals warehouse fact_orders net revenue", "status": "FAIL",
         "count_difference": None, "amount_difference": 12.5},
    ])
    scd2_validation = [
        {"check_name": "no_overlapping_effective_date_ranges", "status": "PASS", "failed_row_count": 0},
    ]
    silver_report = [
        {"table_name": "orders", "status": "PASSED", "quarantine_row_count": "4", "unaccounted_row_count": "0"},
        {"table_name": "campaigns", "status": "FAILED", "quarantine_row_count": "6", "unaccounted_row_count": "2"},
    ]
    pit_report = {"facts": {"fact_orders_scd2": {
        "conservation": {"status": "PASS", "rejected_rows": 147, "unaccounted_rows": 0},
        "fact_resolutions": {"campaign_sk": {"in_range": 2052, "unknown_member": 45, "invalid_reference": 63},
                             "customer_sk": {"in_range": 2149, "unknown_member": 0, "invalid_reference": 11}}}},
        "order_totals": {"status": "PASS", "checked_orders": 1900, "mismatched_orders": 0,
                         "excluded_incomplete_orders": 197, "orders_without_bronze_items": 0}}

    rows = export.build_data_quality_summary(
        reconciliation, scd2_validation, silver_report, pit_report)

    assert [list(row) for row in rows] == [export.SUMMARY_COLUMNS] * len(rows)
    assert [(r["metric_group"], r["metric_name"], r["metric_value"], r["status"]) for r in rows] == [
        ("Warehouse Reconciliation", "Gold order count equals warehouse fact_orders count", 0, "PASS"),
        ("Warehouse Reconciliation", "Gold revenue equals warehouse fact_orders net revenue", 12.5, "FAIL"),
        ("Warehouse Reconciliation", WEB_EVENT_CHECK, 0, "PASS"),
        ("SCD2 Validation", "no_overlapping_effective_date_ranges", 0, "PASS"),
        ("Silver Quarantine", "Quarantined rows: campaigns", 6, "FAIL"),
        ("Silver Quarantine", "Unaccounted rows: campaigns", 2, "FAIL"),
        ("Silver Quarantine", "Quarantined rows: orders", 4, "PASS"),
        ("Silver Quarantine", "Unaccounted rows: orders", 0, "PASS"),
        ("Point-in-Time Facts", "Rejected rows: fact_orders_scd2", 147, "PASS"),
        ("Point-in-Time Facts", "Unaccounted rows: fact_orders_scd2", 0, "PASS"),
        ("Point-in-Time Facts", "Orders with no campaign (unknown campaign member): orders", 45, "PASS"),
        ("Point-in-Time Facts", "Order totals checked: orders", 1900, "PASS"),
        ("Point-in-Time Facts", "Order totals not equal to sum of lines: orders", 0, "PASS"),
        ("Point-in-Time Facts", "Order totals excluded, incomplete: orders", 197, "PASS"),
        ("Point-in-Time Facts", "Order totals with no Bronze items: orders", 0, "PASS"),
        ("Invalid References", "Gold rows on the invalid-reference member, campaign_sk: fact_orders_scd2", 63, "PASS"),
        ("Invalid References", "Gold rows on the invalid-reference member, customer_sk: fact_orders_scd2", 11, "PASS"),
        ("Invalid References", "Warehouse rows on the invalid-reference member, campaign: fact_campaign_spend", 64, "PASS"),
        ("Invalid References", "Warehouse rows on the invalid-reference member, campaign: fact_orders", 63, "PASS"),
        ("Invalid References", "Warehouse rows on the invalid-reference member, campaign: fact_web_events", 400, "PASS"),
        ("Invalid References", "Warehouse rows on the invalid-reference member, customer: fact_orders", 11, "PASS"),
        ("Invalid References", "Warehouse rows on the invalid-reference member, customer: fact_web_events", 67, "PASS"),
        ("Invalid References", "Warehouse rows on the invalid-reference member, product: fact_order_items", 66, "PASS"),
        ("Invalid References", "Warehouse rows on the invalid-reference member, product: fact_web_events", 37, "PASS"),
    ]

    # A missing source is an error, never an empty or invented summary.
    for missing in ("reconciliation", "scd2_validation", "silver_report"):
        inputs = {"reconciliation": reconciliation, "scd2_validation": scd2_validation,
                  "silver_report": silver_report, "pit_report": pit_report}
        inputs[missing] = []
        with pytest.raises(ValueError):
            export.build_data_quality_summary(**inputs)
    with pytest.raises(ValueError):
        export.build_data_quality_summary(reconciliation, scd2_validation, silver_report, {"facts": {}})
    # A report written before the conservation check existed is stale, not zero.
    stale_report = {"facts": {"fact_orders_scd2": {"rejected_rows": 147}}}
    with pytest.raises(ValueError, match="rerun point_in_time_fact_joins"):
        export.build_data_quality_summary(reconciliation, scd2_validation, silver_report, stale_report)


def test_data_quality_summary_reports_the_order_totals_check(load_job):
    export = load_job(EXPORT_JOB, BATCH_DATE="2026-06-18")
    reconciliation = with_web_event_check(
        [{"check_name": "c", "status": "PASS", "count_difference": 0, "amount_difference": None}])
    scd2_validation = [{"check_name": "v", "status": "PASS", "failed_row_count": 0}]
    silver_report = [{"table_name": "orders", "status": "PASSED", "quarantine_row_count": "4",
                      "unaccounted_row_count": "0"}]
    facts = {"fact_orders_scd2": {"conservation": {"status": "PASS", "rejected_rows": 0, "unaccounted_rows": 0},
                                  "fact_resolutions": {"campaign_sk": {"unknown_member": 0, "invalid_reference": 0}}}}
    failed = {"status": "FAIL", "checked_orders": 10, "mismatched_orders": 2,
              "excluded_incomplete_orders": 3, "orders_without_bronze_items": 0}

    rows = export.build_data_quality_summary(
        reconciliation, scd2_validation, silver_report, {"facts": facts, "order_totals": failed})
    order_rows = {r["metric_name"]: (r["metric_value"], r["status"]) for r in rows if "Order totals" in r["metric_name"]}
    assert order_rows == {
        "Order totals checked: orders": (10, "FAIL"),
        "Order totals not equal to sum of lines: orders": (2, "FAIL"),
        "Order totals excluded, incomplete: orders": (3, "FAIL"),
        "Order totals with no Bronze items: orders": (0, "FAIL"),
    }

    # A report written before the check, or one where the check could not run, is an error.
    for order_totals in (None, {"status": "FAIL", "error": "missing input: bronze_order_items"}):
        report = {"facts": facts} if order_totals is None else {"facts": facts, "order_totals": order_totals}
        with pytest.raises(ValueError, match="order totals"):
            export.build_data_quality_summary(reconciliation, scd2_validation, silver_report, report)


def test_data_quality_summary_file_uses_unix_line_endings(load_job, tmp_path):
    # Like the other exports: "\n" only. csv's default "\r\n" would differ.
    export = load_job(EXPORT_JOB, BATCH_DATE="2026-06-18")
    rows = [
        {"metric_group": "SCD2 Validation", "metric_name": "check_a", "metric_value": 0, "status": "PASS"},
        {"metric_group": "SCD2 Validation", "metric_name": "check_b", "metric_value": 3, "status": "FAIL"},
    ]
    path = tmp_path / "data_quality_summary.csv"

    export.write_summary(rows, path)

    data = path.read_bytes()
    assert b"\r" not in data
    assert data.decode("utf-8").splitlines() == [
        ",".join(export.SUMMARY_COLUMNS),
        "SCD2 Validation,check_a,0,PASS",
        "SCD2 Validation,check_b,3,FAIL",
    ]


def test_data_quality_summary_counts_organic_orders_on_the_unknown_campaign(load_job):
    export = load_job(EXPORT_JOB, BATCH_DATE="2026-06-18")
    reconciliation = with_web_event_check(
        [{"check_name": "c", "status": "PASS", "count_difference": 0, "amount_difference": None}])
    scd2_validation = [{"check_name": "v", "status": "PASS", "failed_row_count": 0}]
    silver_report = [{"table_name": "orders", "status": "PASSED", "quarantine_row_count": "4",
                      "unaccounted_row_count": "0"}]
    conservation = {"status": "PASS", "rejected_rows": 0, "unaccounted_rows": 0}
    order_totals = {"status": "PASS", "checked_orders": 1, "mismatched_orders": 0,
                    "excluded_incomplete_orders": 0, "orders_without_bronze_items": 0}

    report = {"facts": {"fact_orders_scd2": {"conservation": conservation,
                                             "fact_resolutions": {"campaign_sk": {"unknown_member": 45, "invalid_reference": 0}}}},
              "order_totals": order_totals}
    rows = export.build_data_quality_summary(reconciliation, scd2_validation, silver_report, report)
    organic = [r for r in rows if r["metric_name"] == "Orders with no campaign (unknown campaign member): orders"]
    assert organic == [{"metric_group": "Point-in-Time Facts",
                        "metric_name": "Orders with no campaign (unknown campaign member): orders",
                        "metric_value": 45, "status": "PASS"}]

    # A report from before organic orders were kept has no unknown_member count: stale.
    stale = {"facts": {"fact_orders_scd2": {"conservation": conservation,
                                            "resolutions": {"campaign_sk": {"in_range": 1, "no_key": 45}}}},
             "order_totals": order_totals}
    with pytest.raises(ValueError, match="unknown_member"):
        export.build_data_quality_summary(reconciliation, scd2_validation, silver_report, stale)


def test_data_quality_summary_counts_invalid_references_per_fact_and_dimension(load_job):
    export = load_job(EXPORT_JOB, BATCH_DATE="2026-06-18")
    scd2_validation = [{"check_name": "v", "status": "PASS", "failed_row_count": 0}]
    silver_report = [{"table_name": "orders", "status": "PASSED", "quarantine_row_count": "4",
                      "unaccounted_row_count": "0"}]
    report = {"facts": {"fact_orders_scd2": {
        "conservation": {"status": "PASS", "rejected_rows": 0, "unaccounted_rows": 0},
        "fact_resolutions": {"campaign_sk": {"unknown_member": 0, "invalid_reference": 2}}}},
        "order_totals": {"status": "PASS", "checked_orders": 1, "mismatched_orders": 0,
                         "excluded_incomplete_orders": 0, "orders_without_bronze_items": 0}}
    web_event = [{"check_name": WEB_EVENT_CHECK, "status": "PASS", "count_difference": 0, "amount_difference": None}]

    # The counts are shown with the status of the check that they are exact.
    rows = export.build_data_quality_summary(
        web_event + invalid_reference_checks(status="FAIL"), scd2_validation, silver_report, report)
    shown = {r["metric_name"]: (r["metric_value"], r["status"]) for r in rows if r["metric_group"] == "Invalid References"}
    assert shown["Warehouse rows on the invalid-reference member, campaign: fact_web_events"] == (400, "FAIL")
    assert shown["Gold rows on the invalid-reference member, campaign_sk: fact_orders_scd2"] == (2, "PASS")
    assert len(shown) == 8
    assert not [r for r in rows if "Rejected rows: fact_web_events" in r["metric_name"]], "no web event is rejected"
    assert not [r for r in rows if r["metric_group"] == "Warehouse Reconciliation" and "Invalid" in r["metric_name"]], \
        "each count appears once"

    # A warehouse or a point-in-time report from before the invalid-reference member is refused.
    with pytest.raises(ValueError, match="predates the invalid-reference member; rebuild the warehouse"):
        export.build_data_quality_summary(web_event, scd2_validation, silver_report, report)
    with pytest.raises(ValueError, match="predates loading every web event"):
        export.build_data_quality_summary(invalid_reference_checks(), scd2_validation, silver_report, report)
    old_report = {**report, "facts": {"fact_orders_scd2": {**report["facts"]["fact_orders_scd2"],
                                                           "fact_resolutions": {"campaign_sk": {"unknown_member": 0}}}}}
    with pytest.raises(ValueError, match="no invalid_reference count for fact_orders_scd2 campaign_sk"):
        export.build_data_quality_summary(with_web_event_check([]), scd2_validation, silver_report, old_report)
