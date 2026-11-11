"""data_quality_summary.csv comes only from verified checks.

It used to be a hand-made file that no code produced, with numbers that went
stale. The power_bi_export task now builds it from warehouse reconciliation,
the latest SCD2 validation run, the Silver quality report and the
point-in-time fact report, and refuses to write it when an input is missing.
E1 (p2-data-v2): it also counts the web events the warehouse rejects (a source
customer, product or campaign key that is not in its dimension).
"""
from __future__ import annotations

import pytest

EXPORT_JOB = "sql/export_power_bi_dataset.py"

WEB_EVENT_CHECK = "Staging web event row count equals warehouse fact_web_events plus rejected web events"
WEB_EVENT_REJECTS = {"rejected_rows": 721, "customer": 67, "product": 54, "campaign": 459}


def with_web_event_check(reconciliation: list[dict], status: str = "PASS") -> list[dict]:
    return reconciliation + [{"check_name": WEB_EVENT_CHECK, "status": status,
                              "count_difference": 0, "amount_difference": None}]


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
        "fact_resolutions": {"campaign_sk": {"in_range": 2052, "unknown_member": 45}}}},
        "order_totals": {"status": "PASS", "checked_orders": 1900, "mismatched_orders": 0,
                         "excluded_incomplete_orders": 197, "orders_without_bronze_items": 0}}

    rows = export.build_data_quality_summary(
        reconciliation, scd2_validation, silver_report, pit_report, WEB_EVENT_REJECTS)

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
        ("Warehouse Facts", "Rejected rows: fact_web_events", 721, "PASS"),
        ("Warehouse Facts", "Rejected rows, customer key not in dimension: fact_web_events", 67, "PASS"),
        ("Warehouse Facts", "Rejected rows, product key not in dimension: fact_web_events", 54, "PASS"),
        ("Warehouse Facts", "Rejected rows, campaign key not in dimension: fact_web_events", 459, "PASS"),
    ]

    # A missing source is an error, never an empty or invented summary.
    for missing in ("reconciliation", "scd2_validation", "silver_report"):
        inputs = {"reconciliation": reconciliation, "scd2_validation": scd2_validation,
                  "silver_report": silver_report, "pit_report": pit_report,
                  "web_event_rejects": WEB_EVENT_REJECTS}
        inputs[missing] = []
        with pytest.raises(ValueError):
            export.build_data_quality_summary(**inputs)
    with pytest.raises(ValueError):
        export.build_data_quality_summary(reconciliation, scd2_validation, silver_report, {"facts": {}},
                                          WEB_EVENT_REJECTS)
    # A report written before the conservation check existed is stale, not zero.
    stale_report = {"facts": {"fact_orders_scd2": {"rejected_rows": 147}}}
    with pytest.raises(ValueError, match="rerun point_in_time_fact_joins"):
        export.build_data_quality_summary(reconciliation, scd2_validation, silver_report, stale_report,
                                          WEB_EVENT_REJECTS)


def test_data_quality_summary_reports_the_order_totals_check(load_job):
    export = load_job(EXPORT_JOB, BATCH_DATE="2026-06-18")
    reconciliation = with_web_event_check(
        [{"check_name": "c", "status": "PASS", "count_difference": 0, "amount_difference": None}])
    scd2_validation = [{"check_name": "v", "status": "PASS", "failed_row_count": 0}]
    silver_report = [{"table_name": "orders", "status": "PASSED", "quarantine_row_count": "4",
                      "unaccounted_row_count": "0"}]
    facts = {"fact_orders_scd2": {"conservation": {"status": "PASS", "rejected_rows": 0, "unaccounted_rows": 0},
                                  "fact_resolutions": {"campaign_sk": {"unknown_member": 0}}}}
    failed = {"status": "FAIL", "checked_orders": 10, "mismatched_orders": 2,
              "excluded_incomplete_orders": 3, "orders_without_bronze_items": 0}

    rows = export.build_data_quality_summary(
        reconciliation, scd2_validation, silver_report, {"facts": facts, "order_totals": failed}, WEB_EVENT_REJECTS)
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
            export.build_data_quality_summary(reconciliation, scd2_validation, silver_report, report,
                                              WEB_EVENT_REJECTS)


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
                                             "fact_resolutions": {"campaign_sk": {"unknown_member": 45}}}},
              "order_totals": order_totals}
    rows = export.build_data_quality_summary(reconciliation, scd2_validation, silver_report, report,
                                             WEB_EVENT_REJECTS)
    organic = [r for r in rows if r["metric_name"] == "Orders with no campaign (unknown campaign member): orders"]
    assert organic == [{"metric_group": "Point-in-Time Facts",
                        "metric_name": "Orders with no campaign (unknown campaign member): orders",
                        "metric_value": 45, "status": "PASS"}]

    # A report from before organic orders were kept has no unknown_member count: stale.
    stale = {"facts": {"fact_orders_scd2": {"conservation": conservation,
                                            "resolutions": {"campaign_sk": {"in_range": 1, "no_key": 45}}}},
             "order_totals": order_totals}
    with pytest.raises(ValueError, match="unknown_member"):
        export.build_data_quality_summary(reconciliation, scd2_validation, silver_report, stale, WEB_EVENT_REJECTS)


def test_data_quality_summary_counts_rejected_web_events(load_job):
    export = load_job(EXPORT_JOB, BATCH_DATE="2026-06-18")
    scd2_validation = [{"check_name": "v", "status": "PASS", "failed_row_count": 0}]
    silver_report = [{"table_name": "orders", "status": "PASSED", "quarantine_row_count": "4",
                      "unaccounted_row_count": "0"}]
    report = {"facts": {"fact_orders_scd2": {
        "conservation": {"status": "PASS", "rejected_rows": 0, "unaccounted_rows": 0},
        "fact_resolutions": {"campaign_sk": {"unknown_member": 0}}}},
        "order_totals": {"status": "PASS", "checked_orders": 1, "mismatched_orders": 0,
                         "excluded_incomplete_orders": 0, "orders_without_bronze_items": 0}}

    # The rows take the status of the check that every staging event is a fact or a reject.
    rows = export.build_data_quality_summary(
        with_web_event_check([], status="FAIL"), scd2_validation, silver_report, report,
        {"rejected_rows": 5, "customer": 3, "product": 0, "campaign": 2})
    assert [(r["metric_name"], r["metric_value"], r["status"]) for r in rows
            if r["metric_group"] == "Warehouse Facts"] == [
        ("Rejected rows: fact_web_events", 5, "FAIL"),
        ("Rejected rows, customer key not in dimension: fact_web_events", 3, "FAIL"),
        ("Rejected rows, product key not in dimension: fact_web_events", 0, "FAIL"),
        ("Rejected rows, campaign key not in dimension: fact_web_events", 2, "FAIL"),
    ]

    # A warehouse without the reject table, or a reconciliation run without the
    # check, is stale: an error, never a zero.
    with pytest.raises(ValueError, match="fact_web_events_rejects"):
        export.build_data_quality_summary(with_web_event_check([]), scd2_validation, silver_report, report, None)
    with pytest.raises(ValueError, match="rerun reconciliation_report.sql"):
        export.build_data_quality_summary(
            [{"check_name": "c", "status": "PASS", "count_difference": 0, "amount_difference": None}],
            scd2_validation, silver_report, report, WEB_EVENT_REJECTS)
