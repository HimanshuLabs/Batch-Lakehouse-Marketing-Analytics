from pathlib import Path
import csv
import json
import re
import sys


EXPORT_DIR = Path("exports/power_bi")
SCREENSHOT_DIR = Path("screenshots/power_bi")

EXPECTED_CSVS = {
    "revenue_daily.csv": "revenue daily",
    "campaign_performance.csv": "campaign performance",
    "product_sales.csv": "product sales",
    "customer_360.csv": "customer 360",
    "marketing_funnel.csv": "marketing funnel",
}

REQUIRED_DOCS = [
    Path("docs/power_bi_dashboard_spec.md"),
    Path("docs/warehouse/power_bi_dashboard.md"),
    Path("docs/warehouse/power_bi_data_dictionary.md"),
    Path("docs/warehouse/power_bi_refresh_flow.md"),
]

FORBIDDEN_SECRET_PATTERNS = [
    r"powerbi[_-]?token",
    r"power_bi[_-]?token",
    r"client_secret",
    r"refresh_token",
    r"access_token",
    r"tenant_secret",
]


def validate_csv(csv_path: Path, logical_name: str, errors: list[str]) -> None:
    if not csv_path.exists():
        errors.append(f"Missing Power BI dataset CSV for {logical_name}: {csv_path}")
        return

    if csv_path.stat().st_size == 0:
        errors.append(f"Power BI CSV is empty: {csv_path}")
        return

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        rows = list(reader)

    if not rows:
        errors.append(f"Power BI CSV has no rows: {csv_path}")
        return

    header = rows[0]

    if len(header) < 2:
        errors.append(f"Power BI CSV header has fewer than 2 columns: {csv_path}")

    if any(not column.strip() for column in header):
        errors.append(f"Power BI CSV has blank header column: {csv_path}")

    if len(rows) < 2:
        errors.append(f"Power BI CSV has header but no data rows: {csv_path}")


def validate_manifest(errors: list[str]) -> None:
    manifest_path = EXPORT_DIR / "export_manifest.json"

    if not manifest_path.exists():
        errors.append("Missing Power BI export manifest: exports/power_bi/export_manifest.json")
        return

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        errors.append(f"Invalid Power BI export manifest JSON: {exc}")
        return

    manifest_text = json.dumps(manifest).lower()

    for csv_name in EXPECTED_CSVS:
        csv_stem = Path(csv_name).stem.lower()
        if csv_name.lower() not in manifest_text and csv_stem not in manifest_text:
            errors.append(f"Manifest does not reference expected dataset: {csv_name}")


def validate_screenshots(errors: list[str]) -> None:
    if not SCREENSHOT_DIR.is_dir():
        errors.append("Missing Power BI screenshot directory: screenshots/power_bi")
        return

    screenshots = [
        path for path in SCREENSHOT_DIR.iterdir()
        if path.suffix.lower() in {".png", ".jpg", ".jpeg"}
    ]

    gitkeep = SCREENSHOT_DIR / ".gitkeep"

    if not screenshots and not gitkeep.exists():
        errors.append("Power BI screenshot directory has neither screenshots nor .gitkeep")

    for screenshot in screenshots:
        if screenshot.stat().st_size == 0:
            errors.append(f"Power BI screenshot is empty: {screenshot}")


def validate_docs(errors: list[str]) -> None:
    for doc_path in REQUIRED_DOCS:
        if not doc_path.exists():
            errors.append(f"Missing Power BI documentation file: {doc_path}")
            continue

        text = doc_path.read_text(encoding="utf-8")

        if not text.strip():
            errors.append(f"Power BI documentation file is empty: {doc_path}")
            continue

        lowered = text.lower()

        for expected_phrase in ("power bi", "dashboard"):
            if expected_phrase not in lowered:
                errors.append(f"Expected phrase {expected_phrase!r} missing from {doc_path}")


def validate_no_powerbi_secrets(errors: list[str]) -> None:
    scan_paths = []

    if EXPORT_DIR.exists():
        scan_paths.extend(path for path in EXPORT_DIR.glob("*") if path.is_file())

    scan_paths.extend(REQUIRED_DOCS)

    for path in scan_paths:
        if not path.exists() or not path.is_file():
            continue

        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        for pattern in FORBIDDEN_SECRET_PATTERNS:
            if re.search(pattern, text, flags=re.IGNORECASE):
                errors.append(f"Possible Power BI credential/token pattern {pattern!r} found in {path}")


def main() -> int:
    errors: list[str] = []

    if not EXPORT_DIR.is_dir():
        errors.append("Missing Power BI export directory: exports/power_bi")
    else:
        for csv_name, logical_name in EXPECTED_CSVS.items():
            validate_csv(EXPORT_DIR / csv_name, logical_name, errors)

        validate_manifest(errors)

    validate_screenshots(errors)
    validate_docs(errors)
    validate_no_powerbi_secrets(errors)

    if errors:
        print("Power BI artifact validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Power BI artifact validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
