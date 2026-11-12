#!/usr/bin/env bash
# End-to-end integration run of this checkout, as CI runs it:
#   1. every DAG task in order for BATCH_DATE 2026-05-18, then 2026-06-18
#   2. the assertions in tests/integration/test_warehouse.py
#   3. a rerun of 2026-06-18 that must leave the lake, the warehouse and the
#      marts identical (tests/integration/fingerprint.py, tests/sql/*.sql)
#   4. tests/integration/rebuild_warehouse.sh (rebuilds and insert-order checks)
#   During 3 and 4 a reader (tests/integration/reader_loop.py, as p2_reader)
#   keeps querying every mart and every dbt view: through those four rebuilds,
#   and a fifth that fails its checks and is rolled back, it must get no error
#   and no empty result, and only whole generations.
#
# Usage: bash tests/integration/run_integration.sh <work dir>
#
# Needs, as the DAG's DEFAULT_ENV expects: ./venv (requirements.txt), ./venv-dbt
# (requirements-dev.txt) unless the dbt tasks are skipped, .env and
# dbt/profiles.yml, and a Postgres container (PGCONTAINER, default
# project2_postgres) holding marketing_analytics for user project2.
#
# Env:
#   PGCONTAINER         Postgres container (default project2_postgres)
#   P2_IT_SKIP          DAG tasks to skip, space separated (default: none)
#   P2_IT_TASK_ENV      extra KEY=VALUE pairs for every task, space separated
#   P2_IT_PYTEST_ARGS   extra pytest arguments for the assertions
set -euo pipefail

WORK=${1:?usage: run_integration.sh <work dir>}
REPO=$(cd "$(dirname "$0")/../.." && pwd)
PY="$REPO/venv/bin/python"
export PGCONTAINER=${PGCONTAINER:-project2_postgres}
read -r -a SKIP <<< "${P2_IT_SKIP:-}"
read -r -a TASK_ENV <<< "${P2_IT_TASK_ENV:-}"
read -r -a PYTEST_ARGS <<< "${P2_IT_PYTEST_ARGS:-}"

mkdir -p "$WORK"
cd "$REPO"

run_batch() {
    local batch=$1 tag=$2
    local args=(--batch-date "$batch" --log-dir "$WORK/$tag")
    [ "${#SKIP[@]}" -gt 0 ] && args+=(--skip "${SKIP[@]}")
    [ "${#TASK_ENV[@]}" -gt 0 ] && args+=(--env "${TASK_ENV[@]}")
    echo "################ DAG run $tag (BATCH_DATE=$batch)"
    "$PY" -B tests/integration/run_dag.py "${args[@]}"
}

fingerprint() {
    local out=$1
    "$PY" -B tests/integration/fingerprint.py "$REPO" > "$out-lake.tsv"
    docker exec -i "$PGCONTAINER" psql -U project2 -d marketing_analytics -v ON_ERROR_STOP=1 -At -F $'\t' \
        < tests/sql/keyfree.sql > "$out-warehouse.tsv"
    docker exec -i "$PGCONTAINER" psql -U project2 -d marketing_analytics -v ON_ERROR_STOP=1 -At -F $'\t' \
        < tests/sql/marts_fingerprint.sql > "$out-marts.tsv"
}

run_batch 2026-05-18 run-2026-05-18
run_batch 2026-06-18 run-2026-06-18

echo "################ assertions"
P2_IT_PROJECT_ROOT="$REPO" P2_IT_PGCONTAINER="$PGCONTAINER" \
    "$PY" -B -m pytest tests/integration -m integration -p no:cacheprovider "${PYTEST_ARGS[@]}"

echo "################ a reader keeps reading during the rebuilds below"
rm -f "$WORK/reader.stop"
"$PY" -B tests/integration/reader_loop.py --container "$PGCONTAINER" \
    --stop-file "$WORK/reader.stop" --out "$WORK/reader.json" &
READER_PID=$!
# Whatever happens below, the reader is told to stop.
trap 'touch "$WORK/reader.stop"' EXIT

echo "################ determinism: rerun 2026-06-18"
fingerprint "$WORK/before"
run_batch 2026-06-18 rerun-2026-06-18
fingerprint "$WORK/after"
for part in lake warehouse marts; do
    if cmp -s "$WORK/before-$part.tsv" "$WORK/after-$part.tsv"; then
        echo "PASS  rerun leaves the $part identical ($(wc -l < "$WORK/before-$part.tsv") tables)"
    else
        echo "FAIL  rerun changed the $part:"; diff "$WORK/before-$part.tsv" "$WORK/after-$part.tsv" | head -20
        exit 1
    fi
done

echo "################ rebuild and insert-order checks"
bash tests/integration/rebuild_warehouse.sh "$WORK/rebuild"

echo "################ what the reader saw"
touch "$WORK/reader.stop"
wait "$READER_PID"

echo
echo "Integration run passed."
