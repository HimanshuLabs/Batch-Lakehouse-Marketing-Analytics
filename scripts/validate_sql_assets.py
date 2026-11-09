from pathlib import Path
import argparse
import os
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


# Schemas whose functions the project defines itself.
FUNCTION_SCHEMAS = ("warehouse", "audit")
_SCHEMA = "(" + "|".join(FUNCTION_SCHEMAS) + ")"

# A function call is "<schema>.<name>(". The same text also appears after
# keywords that take a relation followed by a column list, such as
# "INSERT INTO warehouse.dim_customer (" or "REFERENCES warehouse.dim_date(".
FUNCTION_CALL = re.compile(r"\b" + _SCHEMA + r"\.([a-z_][a-z0-9_]*)\s*\(", re.IGNORECASE)
FUNCTION_DEFINITION = re.compile(
    r"\bcreate\s+(?:or\s+replace\s+)?function\s+" + _SCHEMA + r"\.([a-z_][a-z0-9_]*)\s*\(",
    re.IGNORECASE,
)
RELATION_DEFINITION = re.compile(
    r"\bcreate\s+(?:or\s+replace\s+)?(?:materialized\s+)?(?:table|view)\s+"
    r"(?:if\s+not\s+exists\s+)?" + _SCHEMA + r"\.([a-z_][a-z0-9_]*)",
    re.IGNORECASE,
)
RELATION_KEYWORDS = {"into", "table", "references", "on", "join", "from", "update", "exists", "view"}


def strip_comments(text: str) -> str:
    """Blank out -- and /* */ comments, keeping every newline so line numbers stay right."""
    without_blocks = re.sub(
        r"/\*.*?\*/",
        lambda match: re.sub(r"[^\n]", " ", match.group(0)),
        text,
        flags=re.DOTALL,
    )
    return re.sub(r"--[^\n]*", lambda match: " " * len(match.group(0)), without_blocks)


def validate_function_references(sql_files: list[Path], errors: list[str]) -> None:
    """Every <schema>.<fn>( call (warehouse, audit) must have a CREATE FUNCTION <schema>.<fn> in sql/warehouse."""
    code = {path: strip_comments(path.read_text(encoding="utf-8")) for path in sql_files}

    defined = {
        (schema.lower(), name.lower())
        for text in code.values()
        for schema, name in FUNCTION_DEFINITION.findall(text)
    }
    relations = {
        (schema.lower(), name.lower())
        for text in code.values()
        for schema, name in RELATION_DEFINITION.findall(text)
    }

    for path, text in code.items():
        for match in FUNCTION_CALL.finditer(text):
            schema, name = match.group(1).lower(), match.group(2).lower()
            if (schema, name) in defined or (schema, name) in relations:
                continue

            preceding = re.findall(r"[a-z_]+", text[max(0, match.start() - 40):match.start()].lower())
            if preceding and preceding[-1] in RELATION_KEYWORDS:
                continue

            line = text.count("\n", 0, match.start()) + 1
            errors.append(
                f"{path}:{line}: {schema}.{name}() is called but no "
                f"CREATE FUNCTION {schema}.{name} exists in sql/warehouse/*.sql"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the warehouse SQL assets.")
    parser.add_argument(
        "--root",
        default=".",
        help="Repository root to validate (default: the current directory).",
    )
    args = parser.parse_args()
    os.chdir(args.root)

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

    validate_function_references(warehouse_sql_files, errors)

    if errors:
        print("Warehouse SQL validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print(f"Warehouse SQL validation passed for {len(warehouse_sql_files)} files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
