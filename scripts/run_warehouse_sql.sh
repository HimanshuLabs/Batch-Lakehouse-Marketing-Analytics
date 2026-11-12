#!/usr/bin/env bash
# Run warehouse SQL with psql, stopping at the first error.
#
#   bash scripts/run_warehouse_sql.sh <file.sql>
#   bash scripts/run_warehouse_sql.sh <first.sql> <second.sql> ...
#
# Several files run in ONE session and ONE transaction: it begins before the
# first file and is committed where the last file commits (or after it, when
# that file has no COMMIT). Each file's own top-level "BEGIN;" and the earlier
# files' "COMMIT;" lines are left out, so nothing is visible to other sessions
# until every file has succeeded, and an error rolls all of it back. That
# includes the quality gate at the end of reconciliation_report.sql: a build
# with a failing check is never published. Its failing checks are in the log
# and in logs/warehouse/failed_build_<timestamp>.json.
#
# The warehouse is rebuilt this way (see sql/warehouse/begin_generation.sql):
#
#   bash scripts/run_warehouse_sql.sh sql/warehouse/begin_generation.sql \
#       sql/warehouse/create_warehouse_tables.sql sql/warehouse/scd2_validation_queries.sql \
#       sql/warehouse/create_marts.sql sql/warehouse/reconciliation_report.sql
#
# It runs under the pipeline run lock (scripts/pipeline_lock.py): inside a
# pipeline task the run already holds it (P2_LOCK_HELD); started by hand it
# takes it for this one command, and exits with 75 without running anything
# while another run owns it.
set -euo pipefail

if [[ $# -eq 0 ]]; then
  set -- sql/warehouse/create_schemas.sql
fi
SQL_FILES=("$@")
SQL_FILE="${SQL_FILES[0]}"

RUN_MODE="${RUN_MODE:-docker}"
P2_RUN_ID="${P2_RUN_ID:-warehouse-$(date +%s%N)-$$}"

if [[ -z "${P2_LOCK_HELD:-}" ]]; then
  # The lock file is logs/pipeline_run.lock of the project this runs in.
  export P2_RUN_ID PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"
  exec python3 "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/pipeline_lock.py" --last run_warehouse_sql -- \
    bash "${BASH_SOURCE[0]}" "$@"
fi

PGCONTAINER="${PGCONTAINER:-project2_postgres}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5434}"
PGDATABASE="${PGDATABASE:-marketing_analytics}"
PGUSER="${PGUSER:-project2}"

# The password of the read-only role p2_reader (sql/warehouse/create_schemas.sql):
# from the environment, or from .env in the project. Without one the role
# exists but cannot log in.
if [[ -z "${P2_READER_PASSWORD:-}" && -f .env ]]; then
  P2_READER_PASSWORD="$(grep -E '^P2_READER_PASSWORD=' .env | tail -1 | cut -d= -f2- || true)"
fi
READER_ARGS=()
if [[ -n "${P2_READER_PASSWORD:-}" ]]; then
  READER_ARGS=(-v "p2_reader_password=${P2_READER_PASSWORD}")
fi

for sql_path in "${SQL_FILES[@]}"; do
  if [[ ! -f "$sql_path" ]]; then
    echo "ERROR: SQL file not found: $sql_path"
    exit 1
  fi
done

# The SQL psql reads: the run id, then the file, or the files as one transaction.
sql_input() {
  printf "SELECT set_config('p2.run_id', :'pipeline_run_id', false);\n"
  if [[ ${#SQL_FILES[@]} -eq 1 ]]; then
    cat "$SQL_FILE"
    return
  fi
  local last=$(( ${#SQL_FILES[@]} - 1 )) index
  printf 'BEGIN;\n'
  for index in "${!SQL_FILES[@]}"; do
    printf '\n-- ---- %s\n' "${SQL_FILES[$index]}"
    if [[ "$index" -lt "$last" ]]; then
      grep -vxE '(BEGIN|COMMIT);[[:space:]]*' "${SQL_FILES[$index]}" || true
    else
      grep -vxE 'BEGIN;[[:space:]]*' "${SQL_FILES[$index]}" || true
    fi
  done
  if ! grep -qxE 'COMMIT;[[:space:]]*' "${SQL_FILES[$last]}"; then
    printf '\nCOMMIT;\n'
  fi
}

mkdir -p logs/warehouse

LOG_NAME="$(basename "$SQL_FILE" .sql)"
if [[ ${#SQL_FILES[@]} -gt 1 ]]; then
  LOG_NAME="warehouse_build"
fi
LOG_FILE="logs/warehouse/${LOG_NAME}_$(date +%Y%m%d_%H%M%S).log"

echo "Running warehouse SQL"
echo "SQL file : ${SQL_FILES[*]}"
if [[ ${#SQL_FILES[@]} -gt 1 ]]; then
  echo "           (one transaction)"
fi
echo "Mode     : $RUN_MODE"
echo "Host     : $PGHOST"
echo "Port     : $PGPORT"
echo "Database : $PGDATABASE"
echo "User     : $PGUSER"
echo "Log file : $LOG_FILE"

SQL_STATUS=0

if [[ "$RUN_MODE" == "docker" ]]; then
  echo "Container: $PGCONTAINER"
  echo

  if ! docker ps --format '{{.Names}}' | grep -qx "$PGCONTAINER"; then
    echo "ERROR: Docker container is not running: $PGCONTAINER"
    echo "Start it with: docker start $PGCONTAINER"
    exit 1
  fi

  docker exec -i "$PGCONTAINER" \
    psql -v ON_ERROR_STOP=1 -v pipeline_run_id="${P2_RUN_ID}" "${READER_ARGS[@]}" -U "$PGUSER" -d "$PGDATABASE" \
    < <(sql_input) \
    2>&1 | tee "$LOG_FILE" || SQL_STATUS=$?

elif [[ "$RUN_MODE" == "host" ]]; then
  echo "Host     : $PGHOST"
  echo "Port     : $PGPORT"
  echo

  if ! command -v psql >/dev/null 2>&1; then
    echo "ERROR: psql is not installed or not available in PATH."
    exit 1
  fi

  psql \
    --host "$PGHOST" \
    --port "$PGPORT" \
    --dbname "$PGDATABASE" \
    --username "$PGUSER" \
    --set ON_ERROR_STOP=1 \
    --set pipeline_run_id="${P2_RUN_ID}" \
    "${READER_ARGS[@]}" \
    --file <(sql_input) \
    2>&1 | tee "$LOG_FILE" || SQL_STATUS=$?

else
  echo "ERROR: RUN_MODE must be either 'docker' or 'host'."
  exit 1
fi

if [[ "$SQL_STATUS" -ne 0 ]]; then
  # The quality gate reports its failing checks as one line of JSON before it
  # rolls the build back; keep it where it can be read without the tables.
  REPORT_JSON="$(grep -o 'P2_FAILED_BUILD_REPORT {.*' "$LOG_FILE" | tail -1 | sed 's/^P2_FAILED_BUILD_REPORT //' || true)"
  if [[ -n "$REPORT_JSON" ]]; then
    REPORT_FILE="logs/warehouse/failed_build_$(date -u +%Y%m%dT%H%M%S%3NZ).json"
    if ! printf '%s\n' "$REPORT_JSON" | python3 -m json.tool > "$REPORT_FILE" 2>/dev/null; then
      printf '%s\n' "$REPORT_JSON" > "$REPORT_FILE"
    fi
    echo
    echo "QUALITY GATE FAILED: the build was rolled back and nothing was published."
    echo "Failing checks: $REPORT_FILE (and the FAILED CHECK lines above)."
  fi
  echo "Warehouse SQL FAILED (exit $SQL_STATUS). Log file: $LOG_FILE"
  exit "$SQL_STATUS"
fi

echo
echo "Warehouse SQL completed successfully."
