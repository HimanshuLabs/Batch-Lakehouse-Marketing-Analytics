#!/usr/bin/env bash
# Rebuild the warehouse three times from the same staging tables and prove the
# result does not depend on the build or on insert order:
#   A, B  the warehouse SQL as committed, twice
#   C     the same, with the dim_region / dim_channel source lists reversed
# Keys (dimension and fact), the key-free fingerprint (tests/sql/keyfree.sql)
# and every mart must be identical across A, B and C, every region/channel key
# must equal warehouse.deterministic_sk(prefix, natural key), and every fact
# row key must equal warehouse.deterministic_fact_sk(prefix, natural key).
#
# Usage: bash tests/integration/rebuild_warehouse.sh <work dir>
# Env:   PGCONTAINER (default project2_postgres): the Postgres container to use.
#
# It rebuilds the warehouse in that container's marketing_analytics database,
# so it refuses to run against project2_postgres outside CI (CI=true) unless
# P2_IT_ALLOW_REBUILD=1 is set.
set -euo pipefail

WORK=${1:?usage: rebuild_warehouse.sh <work dir>}
REPO=$(cd "$(dirname "$0")/../.." && pwd)
PGCONTAINER=${PGCONTAINER:-project2_postgres}

if [ "$PGCONTAINER" = "project2_postgres" ] && [ "${CI:-}" != "true" ] && [ "${P2_IT_ALLOW_REBUILD:-}" != "1" ]; then
    echo "ERROR: refusing to rebuild the warehouse in project2_postgres outside CI."
    echo "Point PGCONTAINER at a throwaway container (tests/integration/run_local.sh does this)."
    exit 2
fi

mkdir -p "$WORK"
psql_db() { docker exec -i -e "PGOPTIONS=-c p2.run_id=${P2_RUN_ID}" "$PGCONTAINER" psql -U project2 -d marketing_analytics -v ON_ERROR_STOP=1 -At -F $'\t' "$@"; }

# Build C uses a copy of create_warehouse_tables.sql with the first two
# FOREACH source lists (dim_region, dim_channel) reversed.
python3 - "$REPO/sql/warehouse/create_warehouse_tables.sql" "$WORK/create_warehouse_tables_reversed.sql" <<'PY'
import re, sys
text = open(sys.argv[1], encoding="utf-8").read()
pattern = re.compile(r"(FOREACH source_table IN ARRAY ARRAY\[\s*)(.*?)\s*\]", re.S)
blocks = list(pattern.finditer(text))[:2]
assert len(blocks) == 2, "expected the dim_region and dim_channel source lists"
for match in reversed(blocks):
    items = [i.strip() for i in match.group(2).split(",") if i.strip()]
    text = text[:match.start()] + match.group(1) + ",\n        ".join(reversed(items)) + "\n    ]" + text[match.end():]
open(sys.argv[2], "w", encoding="utf-8").write(text)
PY

build() {
    local tag=$1 warehouse_sql=$2
    export P2_RUN_ID="integration-$tag-$(date +%s%N)-$$"
    echo "== build $tag"
    for f in "$warehouse_sql" \
             "$REPO/sql/warehouse/scd2_validation_queries.sql" "$REPO/sql/warehouse/create_marts.sql" \
             "$REPO/sql/warehouse/reconciliation_report.sql"; do
        if ! psql_db -q < "$f" > "$WORK/$tag-$(basename "$f" .sql).log" 2>&1; then
            echo "ERROR in $(basename "$f") (build $tag):"; tail -20 "$WORK/$tag-$(basename "$f" .sql).log"; exit 1
        fi
    done
    psql_db > "$WORK/$tag-keys.tsv" <<'SQL'
SELECT 'region', region_sk, region_nk, country, state, city FROM warehouse.dim_region ORDER BY region_nk;
SELECT 'channel', channel_sk, channel_nk, channel_name, channel_group FROM warehouse.dim_channel ORDER BY channel_nk;
SELECT 'customer', customer_sk, customer_id, effective_from FROM warehouse.dim_customer ORDER BY customer_sk;
SELECT 'product', product_sk, product_id, effective_from FROM warehouse.dim_product ORDER BY product_sk;
SELECT 'campaign', campaign_sk, campaign_id, effective_from FROM warehouse.dim_campaign ORDER BY campaign_sk;
SELECT 'orders', order_id, order_sk, customer_sk, campaign_sk, order_date_sk, region_sk, channel_sk FROM warehouse.fact_orders ORDER BY order_id;
SELECT 'items', order_item_nk, order_item_sk, order_sk, customer_sk, product_sk, order_date_sk, region_sk, channel_sk FROM warehouse.fact_order_items ORDER BY order_item_nk;
SELECT 'spend', campaign_spend_nk, campaign_spend_sk, campaign_sk, spend_date_sk, region_sk, channel_sk FROM warehouse.fact_campaign_spend ORDER BY campaign_spend_nk;
SELECT 'web', event_id, event_sk, customer_sk, product_sk, campaign_sk, event_date_sk, region_sk, channel_sk FROM warehouse.fact_web_events ORDER BY event_id;
SELECT 'conversions', conversion_nk, conversion_sk, event_sk, customer_sk, product_sk, campaign_sk, conversion_date_sk, region_sk, channel_sk FROM warehouse.fact_conversions ORDER BY conversion_nk;
SQL
    psql_db < "$REPO/tests/sql/keyfree.sql" > "$WORK/$tag-keyfree.tsv"
    psql_db < "$REPO/tests/sql/marts_fingerprint.sql" > "$WORK/$tag-marts.tsv"
}

build A "$REPO/sql/warehouse/create_warehouse_tables.sql"
build B "$REPO/sql/warehouse/create_warehouse_tables.sql"
build C "$WORK/create_warehouse_tables_reversed.sql"

failed=0
echo
for part in keys keyfree marts; do
    for other in B C; do
        if cmp -s "$WORK/A-$part.tsv" "$WORK/$other-$part.tsv"; then
            echo "PASS  $part: A == $other ($(wc -l < "$WORK/A-$part.tsv") lines)"
        else
            echo "FAIL  $part: A != $other"; diff "$WORK/A-$part.tsv" "$WORK/$other-$part.tsv" | head -10; failed=1
        fi
    done
done

non_hash_keys=$(psql_db -c "
SELECT (SELECT count(*) FROM warehouse.dim_region
        WHERE region_sk <> CASE WHEN region_nk = 'unknown|unknown|unknown' THEN 0
                                ELSE warehouse.deterministic_sk('region', region_nk) END)
     + (SELECT count(*) FROM warehouse.dim_channel
        WHERE channel_sk <> CASE WHEN channel_nk = 'unknown' THEN 0
                                 ELSE warehouse.deterministic_sk('channel', channel_nk) END)")
if [ "$non_hash_keys" = "0" ]; then
    echo "PASS  every region/channel key is deterministic_sk(prefix, natural key)"
else
    echo "FAIL  $non_hash_keys region/channel key(s) are not deterministic_sk(prefix, natural key)"; failed=1
fi

# Fact row keys, and the parent keys items and conversions carry.
non_hash_fact_keys=$(psql_db -c "
SELECT (SELECT count(*) FROM warehouse.fact_orders
        WHERE order_sk IS DISTINCT FROM warehouse.deterministic_fact_sk('order', order_id))
     + (SELECT count(*) FROM warehouse.fact_order_items
        WHERE order_item_sk IS DISTINCT FROM warehouse.deterministic_fact_sk('order_item', order_item_nk)
           OR order_sk IS DISTINCT FROM warehouse.deterministic_fact_sk('order', order_id))
     + (SELECT count(*) FROM warehouse.fact_campaign_spend
        WHERE campaign_spend_sk IS DISTINCT FROM warehouse.deterministic_fact_sk('campaign_spend', campaign_spend_nk))
     + (SELECT count(*) FROM warehouse.fact_web_events
        WHERE event_sk IS DISTINCT FROM warehouse.deterministic_fact_sk('web_event', event_id))
     + (SELECT count(*) FROM warehouse.fact_conversions
        WHERE conversion_sk IS DISTINCT FROM warehouse.deterministic_fact_sk('conversion', conversion_nk)
           OR event_sk IS DISTINCT FROM warehouse.deterministic_fact_sk('web_event', event_id))")
if [ "$non_hash_fact_keys" = "0" ]; then
    echo "PASS  every fact row key (and items' order_sk, conversions' event_sk) is deterministic_fact_sk(prefix, natural key)"
else
    echo "FAIL  $non_hash_fact_keys fact row(s) have a key that is not deterministic_fact_sk(prefix, natural key)"; failed=1
fi

echo
if [ "$failed" = "0" ]; then echo "Rebuild checks passed."; else echo "Rebuild checks FAILED (details in $WORK)."; fi
exit "$failed"
