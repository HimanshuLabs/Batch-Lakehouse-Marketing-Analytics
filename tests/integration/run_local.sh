#!/usr/bin/env bash
# Run the integration test on a developer machine without touching the real
# data/, logs/ or the project2_postgres / marketing_analytics warehouse:
#   - a copy of this working tree under /tmp (tracked and untracked, non-ignored
#     files), sharing the project venv through a symlink (nothing installed)
#   - a throwaway Postgres container on its own port
#   - the dbt tasks are skipped: venv-dbt does not work on Python 3.14 locally;
#     CI runs them in a Python 3.13 venv
# Everything is removed afterwards unless --keep is given.
#
# Usage: bash tests/integration/run_local.sh [--keep] [--write-expected]
#   --write-expected  skip the golden-count test, then write the counts of this
#                     run to tests/integration/expected_counts.json in this checkout
# Env:
#   P2_IT_PORT      host port for the throwaway Postgres (default 5440)
#   P2_IT_PG_IMAGE  Postgres image (default postgres:16.14, the image CI pins)
set -euo pipefail

KEEP=0
WRITE_EXPECTED=0
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=1 ;;
        --write-expected) WRITE_EXPECTED=1 ;;
        *) echo "usage: run_local.sh [--keep] [--write-expected]"; exit 2 ;;
    esac
done

REPO=$(cd "$(dirname "$0")/../.." && pwd)
PORT=${P2_IT_PORT:-5440}
IMAGE=${P2_IT_PG_IMAGE:-postgres:16.14}
CONTAINER="p2it_postgres_$$"
WORK=$(mktemp -d /tmp/p2-integration-XXXXXX)
COPY="$WORK/repo"

# The only container this script may ever start or remove is its own sandbox:
# named p2it_postgres_<pid>, removed afterwards by the ID that docker run
# returned. It never stops, restarts or removes any other container.
STARTED_ID=""
[[ "$CONTAINER" =~ ^p2it_postgres_[0-9]+$ ]] || { echo "ERROR: unexpected sandbox name $CONTAINER"; exit 2; }
if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "ERROR: container $CONTAINER already exists"; exit 2
fi
if docker ps --format '{{.Ports}}' | grep -q ":$PORT->"; then
    echo "ERROR: port $PORT is already published by a container; set P2_IT_PORT"; exit 2
fi

cleanup() {
    if [ -n "$STARTED_ID" ]; then
        docker rm -f "$STARTED_ID" > /dev/null 2>&1 || true
    fi
    if [ "$KEEP" = "1" ]; then
        echo "kept: $WORK (container $CONTAINER removed)"
    else
        case "$WORK" in /tmp/p2-integration-*) rm -rf "$WORK" ;; esac
        echo "removed $WORK and container $CONTAINER"
    fi
}
trap cleanup EXIT

echo "== copy of the working tree: $COPY"
mkdir -p "$COPY"
(cd "$REPO" && git ls-files -z -co --exclude-standard) \
    | python3 -c "import os,sys; sys.stdout.write(''.join(p + '\0' for p in sys.stdin.read().split('\0') if p and os.path.exists(p)))" \
    | (cd "$REPO" && tar --null -T - -cf -) | tar -xf - -C "$COPY"
ln -s "$REPO/venv" "$COPY/venv"
sed "s/^PGPORT=.*/PGPORT=$PORT/" "$REPO/.env.example" > "$COPY/.env"

echo "== throwaway Postgres $CONTAINER ($IMAGE) on 127.0.0.1:$PORT"
STARTED_ID=$(docker run -d --name "$CONTAINER" \
    -e POSTGRES_USER=project2 -e POSTGRES_PASSWORD=project2 -e POSTGRES_DB=marketing_analytics \
    -p "127.0.0.1:$PORT:5432" "$IMAGE")
ready=0
for _ in $(seq 1 180); do
    if docker exec "$CONTAINER" pg_isready -U project2 -d marketing_analytics > /dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [ "$ready" != "1" ]; then
    echo "ERROR: Postgres in $CONTAINER did not become ready within 180 s; last log lines:"
    docker logs --tail 20 "$CONTAINER" 2>&1 || true
    exit 1
fi

pytest_args=""
[ "$WRITE_EXPECTED" = "1" ] && pytest_args="--deselect=tests/integration/test_warehouse.py::test_golden_counts"

(
    cd "$COPY"
    PGCONTAINER="$CONTAINER" \
    P2_IT_SKIP="dbt_run dbt_test dbt_docs" \
    P2_IT_TASK_ENV="PGCONTAINER=$CONTAINER P2_POSTGRES_PORT=$PORT" \
    P2_IT_PYTEST_ARGS="$pytest_args" \
        bash tests/integration/run_integration.sh "$WORK/out"
)

if [ "$WRITE_EXPECTED" = "1" ]; then
    (cd "$COPY" && P2_IT_PROJECT_ROOT="$COPY" P2_IT_PGCONTAINER="$CONTAINER" \
        venv/bin/python -B tests/integration/test_warehouse.py --write-expected)
    cp "$COPY/tests/integration/expected_counts.json" "$REPO/tests/integration/expected_counts.json"
    echo "wrote $REPO/tests/integration/expected_counts.json"
fi
