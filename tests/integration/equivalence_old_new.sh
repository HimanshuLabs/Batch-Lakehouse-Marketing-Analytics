#!/usr/bin/env bash
# Build the warehouse with the base commit's SQL and with this checkout's SQL,
# from the same staging tables, and require identical data, report rows and
# catalog (tests/sql/equivalence_fingerprint.sql):
#   old, old-again      the base commit's SQL and runner, twice (the control:
#                       a fingerprint line that differs between them is unstable
#                       by itself, and nothing is compared with it)
#   new                 this checkout's SQL and runner
#   new-scale-settings  the same, with the session settings of a scale run
#
# Usage: bash tests/integration/equivalence_old_new.sh <work dir> <base dir>
#   <base dir> holds the build files of the base commit and nothing else:
#     git archive HEAD sql/warehouse scripts/run_warehouse_sql.sh | tar -x -C <base dir>
#
# It only ever runs in the throwaway container of tests/integration/run_local.sh
# (PGCONTAINER=p2it_postgres_<pid>); run_integration.sh calls it there when
# P2_IT_EQUIVALENCE_BASE names the base directory.
set -euo pipefail

WORK=${1:?usage: equivalence_old_new.sh <work dir> <base dir>}
BASE=${2:?usage: equivalence_old_new.sh <work dir> <base dir>}
REPO=$(cd "$(dirname "$0")/../.." && pwd)
PGCONTAINER=${PGCONTAINER:-}

if [[ ! "$PGCONTAINER" =~ ^p2it_postgres_[0-9]+$ ]]; then
    echo "ERROR: equivalence_old_new.sh runs only in the sandbox of run_local.sh (PGCONTAINER=p2it_postgres_<pid>)."
    exit 2
fi
BASE=$(cd "$BASE" && pwd)
if [ "$BASE" = "$REPO" ]; then
    echo "ERROR: the base directory is this checkout; old and new would be the same files."
    exit 2
fi

# What a build reads from its root: the five SQL files and the runner.
SOURCES=(
    sql/warehouse/begin_generation.sql
    sql/warehouse/create_warehouse_tables.sql
    sql/warehouse/scd2_validation_queries.sql
    sql/warehouse/create_marts.sql
    sql/warehouse/reconciliation_report.sql
    scripts/run_warehouse_sql.sh
)
for root in "$BASE" "$REPO"; do
    for source in "${SOURCES[@]}"; do
        if [ ! -f "$root/$source" ]; then
            echo "ERROR: $root/$source is missing."
            exit 2
        fi
    done
done

# The whole script is one run under the pipeline run lock, as rebuild_warehouse.sh is.
if [ -z "${P2_LOCK_HELD:-}" ]; then
    export PROJECT_ROOT="${PROJECT_ROOT:-$REPO}" P2_RUN_ID="equivalence-$(date +%s%N)-$$"
    exec python3 "$REPO/scripts/pipeline_lock.py" --last equivalence_old_new -- bash "$0" "$@"
fi

mkdir -p "$WORK"

# The runner takes the reader password from the environment, or from .env in
# the directory it runs in. The base directory has no .env, so it is resolved
# once here, the way the runner does it, and every build gets the same value.
# It is never printed.
if [[ -z "${P2_READER_PASSWORD:-}" && -f "$REPO/.env" ]]; then
    P2_READER_PASSWORD="$(grep -E '^P2_READER_PASSWORD=' "$REPO/.env" | tail -1 | cut -d= -f2- || true)"
fi
export P2_READER_PASSWORD="${P2_READER_PASSWORD:-}"
if [ -n "$P2_READER_PASSWORD" ]; then
    echo "reader password: set (the same for every build)"
else
    echo "reader password: not set (the same for every build)"
fi

# Evidence of what each root holds.
for pair in "old:$BASE" "new:$REPO"; do
    (cd "${pair#*:}" && md5sum "${SOURCES[@]}") > "$WORK/sources-${pair%%:*}.md5"
    echo "== ${pair%%:*} sources (${pair#*:})"
    cat "$WORK/sources-${pair%%:*}.md5"
done

psql_db() { docker exec -i "$PGCONTAINER" psql -U project2 -d marketing_analytics -v ON_ERROR_STOP=1 -At -F $'\t' "$@"; }

# One build from the files of <root>, and its fingerprint. The runner resolves
# its SQL arguments, .env and logs/ against the directory it runs in, so it
# runs in <root>, with absolute paths to <root>'s files and PROJECT_ROOT=<root>.
build() {
    local tag=$1 root=$2
    export P2_RUN_ID="equivalence-$tag-$(date +%s%N)-$$"
    echo "== build $tag from $root"
    if ! (cd "$root" && PROJECT_ROOT="$root" PGCONTAINER="$PGCONTAINER" RUN_MODE=docker \
            bash "$root/scripts/run_warehouse_sql.sh" \
            "$root/sql/warehouse/begin_generation.sql" "$root/sql/warehouse/create_warehouse_tables.sql" \
            "$root/sql/warehouse/scd2_validation_queries.sql" "$root/sql/warehouse/create_marts.sql" \
            "$root/sql/warehouse/reconciliation_report.sql") > "$WORK/$tag-build.log" 2>&1; then
        echo "ERROR in build $tag:"; tail -20 "$WORK/$tag-build.log"; exit 1
    fi
    psql_db < "$REPO/tests/sql/equivalence_fingerprint.sql" > "$WORK/$tag.tsv"
}

# Which files a build ran: the new runner writes one "p2 step" line per SQL
# file to the log, the base commit's runner writes none.
steps() { grep -c '^---- p2 step: ' "$WORK/$1-build.log" || true; }

build old "$BASE"
build old-again "$BASE"
build new "$REPO"
P2_BUILD_WORK_MEM=256MB P2_BUILD_HASH_MEM_MULTIPLIER=4 P2_BUILD_MAINTENANCE_WORK_MEM=1GB P2_BUILD_TEMP_BUFFERS=1GB \
    build new-scale-settings "$REPO"

failed=0
echo
for tag in old old-again; do
    if [ "$(steps "$tag")" = "0" ]; then
        echo "PASS  build $tag ran the base commit's runner (no 'p2 step' line)"
    else
        echo "FAIL  build $tag has $(steps "$tag") 'p2 step' line(s): it ran the new files"; failed=1
    fi
done
for tag in new new-scale-settings; do
    if [ "$(steps "$tag")" = "5" ]; then
        echo "PASS  build $tag ran the new runner (5 'p2 step' lines, one per SQL file)"
    else
        echo "FAIL  build $tag has $(steps "$tag") 'p2 step' line(s), expected 5"; failed=1
    fi
done
if [ "$failed" != "0" ]; then
    echo "Equivalence checks FAILED: the builds did not run the files they were meant to (details in $WORK)."
    exit 1
fi

# The control: the old SQL against itself.
if ! cmp -s "$WORK/old.tsv" "$WORK/old-again.tsv"; then
    echo "FAIL  old != old-again: these fingerprint lines differ between two builds of the same SQL,"
    echo "      so they cannot be compared with the new SQL:"
    diff "$WORK/old.tsv" "$WORK/old-again.tsv" || true
    echo "Equivalence checks FAILED: the fingerprint is not stable (details in $WORK)."
    exit 1
fi
echo "PASS  old == old-again ($(wc -l < "$WORK/old.tsv") lines: data, report rows, catalog)"

for other in new new-scale-settings; do
    if cmp -s "$WORK/old.tsv" "$WORK/$other.tsv"; then
        echo "PASS  old == $other"
    else
        # diff exits 1 here; under set -e and pipefail it must not end the script.
        echo "FAIL  old != $other"; { diff "$WORK/old.tsv" "$WORK/$other.tsv" || true; } | head -40; failed=1
    fi
done

# Check 4j of the mart validation must plan as an anti-join, not as a subplan
# run for every row. The plan is of the build's own text, cut from the new
# create_marts.sql: the copy of mart_customer_360 that it makes before 4j-4l
# (with its ANALYZE), and check 4j's first subquery, which reads that copy.
MARTS_SQL="$REPO/sql/warehouse/create_marts.sql"
copy_4j=$(grep -x 'CREATE TEMP TABLE validation_customer_360 ON COMMIT DROP AS SELECT \* FROM marts\.mart_customer_360;' "$MARTS_SQL" || true)
query_4j=$(awk '/^-- 4j\. / {f = 1}
                f && /^        \(SELECT COUNT\(\*\) FROM marts\.mart_customer_daily_snapshot s$/ {q = 1}
                q {print}
                q && /= to_jsonb\(s\) - .snapshot_date. - .days_since_last_order.\)\)$/ {exit}' "$MARTS_SQL" \
           | sed '1s/^        (//; $s/))$/)/')
if [ "$(printf '%s\n' "$copy_4j" | grep -c .)" != 1 ] \
   || [ "$(printf '%s\n' "$query_4j" | grep -c 'AND NOT EXISTS (SELECT 1 FROM pg_temp.validation_customer_360 m$')" != 1 ]; then
    echo "FAIL  check 4j: its copy or its first subquery was not found in $MARTS_SQL"; failed=1
elif ! printf '%s\n' 'BEGIN;' "$copy_4j" 'ANALYZE pg_temp.validation_customer_360;' "EXPLAIN $query_4j;" 'ROLLBACK;' \
        | psql_db > "$WORK/4j-plan.txt" 2>&1; then
    echo "FAIL  check 4j could not be planned (see $WORK/4j-plan.txt)"; failed=1
elif grep -q "Anti Join" "$WORK/4j-plan.txt"; then
    echo "PASS  check 4j plans as an anti-join"
else
    echo "FAIL  check 4j does not plan as an anti-join (plan: $WORK/4j-plan.txt)"; failed=1
fi

echo
if [ "$failed" = "0" ]; then echo "Equivalence checks passed."; else echo "Equivalence checks FAILED (details in $WORK)."; fi
exit "$failed"
