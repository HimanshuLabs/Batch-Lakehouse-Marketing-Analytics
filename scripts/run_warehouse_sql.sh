#!/usr/bin/env bash
set -euo pipefail

SQL_FILE="${1:-sql/warehouse/create_schemas.sql}"

RUN_MODE="${RUN_MODE:-docker}"

PGCONTAINER="${PGCONTAINER:-project2_postgres}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5434}"
PGDATABASE="${PGDATABASE:-marketing_analytics}"
PGUSER="${PGUSER:-project2}"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "ERROR: SQL file not found: $SQL_FILE"
  exit 1
fi

mkdir -p logs/warehouse

LOG_FILE="logs/warehouse/$(basename "$SQL_FILE" .sql)_$(date +%Y%m%d_%H%M%S).log"

echo "Running warehouse SQL"
echo "SQL file : $SQL_FILE"
echo "Mode     : $RUN_MODE"
echo "Host     : $PGHOST"
echo "Port     : $PGPORT"
echo "Database : $PGDATABASE"
echo "User     : $PGUSER"
echo "Log file : $LOG_FILE"

if [[ "$RUN_MODE" == "docker" ]]; then
  echo "Container: $PGCONTAINER"
  echo

  if ! docker ps --format '{{.Names}}' | grep -qx "$PGCONTAINER"; then
    echo "ERROR: Docker container is not running: $PGCONTAINER"
    echo "Start it with: docker start $PGCONTAINER"
    exit 1
  fi

  docker exec -i "$PGCONTAINER" \
    psql -v ON_ERROR_STOP=1 -U "$PGUSER" -d "$PGDATABASE" \
    < "$SQL_FILE" \
    2>&1 | tee "$LOG_FILE"

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
    --file "$SQL_FILE" \
    2>&1 | tee "$LOG_FILE"

else
  echo "ERROR: RUN_MODE must be either 'docker' or 'host'."
  exit 1
fi

echo
echo "Warehouse SQL completed successfully."
