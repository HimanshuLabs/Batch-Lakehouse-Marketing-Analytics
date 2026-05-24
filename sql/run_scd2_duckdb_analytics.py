from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
QUERY_FILE = PROJECT_ROOT / "sql" / "scd2_duckdb_analytics_queries.sql"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def split_sql_queries(sql_text: str) -> list[str]:
    cleaned = re.sub(r"--.*", "", sql_text)
    return [
        query.strip()
        for query in cleaned.split(";")
        if query.strip()
    ]


def main() -> None:
    report = {
        "job_name": "scd2_duckdb_analytics",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "query_file": str(QUERY_FILE),
        "queries": [],
    }

    overall_status = "PASS"

    con = duckdb.connect(database=":memory:")

    try:
        sql_text = QUERY_FILE.read_text(encoding="utf-8")
        queries = split_sql_queries(sql_text)

        for index, query in enumerate(queries, start=1):
            print(f"Running query {index}/{len(queries)}")

            try:
                result = con.execute(query).fetchdf()
                row_count = len(result)

                report["queries"].append(
                    {
                        "query_number": index,
                        "status": "PASS",
                        "row_count": row_count,
                        "columns": list(result.columns),
                    }
                )

                print(result.head(10).to_string(index=False))
                print()

            except Exception as exc:
                overall_status = "FAIL"
                report["queries"].append(
                    {
                        "query_number": index,
                        "status": "FAIL",
                        "error": str(exc),
                    }
                )
                print(f"Query {index} failed: {exc}")
                print()

    finally:
        con.close()

        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["overall_status"] = overall_status

        report_path = LOG_DIR / "scd2_duckdb_analytics_report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print(f"Report written to: {report_path}")
        print(f"Overall status: {overall_status}")

    if overall_status != "PASS":
        raise SystemExit("SCD2 DuckDB analytics query pack failed.")


if __name__ == "__main__":
    main()
