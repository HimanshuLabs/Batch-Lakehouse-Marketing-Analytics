from pathlib import Path
import re
import sys


REQUIRED_FILES = {
    "sql/warehouse/create_schemas.sql": ["create schema"],
    "sql/warehouse/load_gold_to_staging.sql": ["staging"],
    "sql/warehouse/create_warehouse_tables.sql": ["warehouse"],
    "sql/warehouse/create_marts.sql": ["marts"],
    "sql/warehouse/create_indexes.sql": ["create index"],
    "sql/warehouse/reconciliation_report.sql": ["audit", "reconciliation"],
    "sql/warehouse/scd2_validation_queries.sql": ["scd2"],
    "sql/warehouse/performance_explain_analyze.sql": ["explain"],
    "sql/warehouse/dashboard_query_pack.sql": ["select"],
}

OPTIONAL_FILES = {
    "sql/warehouse/powerbi_export_queries.sql": [],
    "sql/warehouse/load_inferred_web_campaigns.sql": [],
    "sql/warehouse/load_inferred_web_customers.sql": [],
    "sql/warehouse/load_inferred_web_products.sql": [],
    "sql/warehouse/repair_web_event_surrogate_keys.sql": [],
}

FORBIDDEN_PATTERNS = [
    r"REPLACE_ME",
    r"FIXME",
    r"<your_",
    r"YOUR_[A-Z0-9_]*",
    r"CHANGE_ME",
    r"PASTE_",
    r"your-password",
    r"your_access_key",
    r"your-secret",
]


def meaningful_sql_lines(text: str) -> list[str]:
    lines: list[str] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("--"):
            continue

        lines.append(line)

    return lines


def validate_sql_file(
    path_text: str,
    keywords: list[str],
    required: bool,
    errors: list[str],
) -> None:
    path = Path(path_text)

    if not path.exists():
        if required:
            errors.append(f"Missing required SQL file: {path_text}")
        return

    if not path.is_file():
        errors.append(f"SQL path is not a file: {path_text}")
        return

    text = path.read_text(encoding="utf-8")
    lowered = text.lower()

    if not text.strip():
        errors.append(f"SQL file is empty: {path_text}")
        return

    if not meaningful_sql_lines(text):
        errors.append(f"SQL file has no executable-looking SQL lines: {path_text}")

    for pattern in FORBIDDEN_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            errors.append(f"Forbidden placeholder pattern {pattern!r} found in {path_text}")

    for keyword in keywords:
        if keyword.lower() not in lowered:
            errors.append(f"Expected keyword {keyword!r} not found in {path_text}")


def main() -> int:
    errors: list[str] = []

    for path_text, keywords in REQUIRED_FILES.items():
        validate_sql_file(path_text, keywords, required=True, errors=errors)

    for path_text, keywords in OPTIONAL_FILES.items():
        validate_sql_file(path_text, keywords, required=False, errors=errors)

    warehouse_sql_files = sorted(Path("sql/warehouse").glob("*.sql"))

    if not warehouse_sql_files:
        errors.append("No warehouse SQL files found under sql/warehouse/")

    for path in warehouse_sql_files:
        text = path.read_text(encoding="utf-8")
        if not text.strip():
            errors.append(f"Warehouse SQL file is empty: {path}")

    if errors:
        print("Warehouse SQL validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print(f"Warehouse SQL validation passed for {len(warehouse_sql_files)} files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
