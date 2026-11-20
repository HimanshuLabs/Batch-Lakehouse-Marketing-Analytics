-- Old-against-new fingerprint of one warehouse generation: data, report rows
-- and catalog. Two builds of the same staging tables must give identical
-- output. Read-only. Used by tests/integration/equivalence_old_new.sh.
--
-- Data: every table and view in warehouse and marts, and the three audit
-- reports: its row count and an order-independent hash (the md5 of every row
-- as jsonb, summed as two 64-bit halves). Left out: the load time, the check
-- time and the run id, which differ on every build.

SET timezone = 'UTC';

\echo ## data
SELECT format(
    'SELECT %L, count(*), coalesce(sum((''x'' || substr(h, 1, 16))::bit(64)::bigint::numeric), 0), '
    'coalesce(sum((''x'' || substr(h, 17, 16))::bit(64)::bigint::numeric), 0) '
    'FROM (SELECT md5((to_jsonb(t) - %L::text[])::text) AS h FROM %I.%I t) hashed',
    n.nspname || '.' || c.relname,
    '{warehouse_loaded_at,checked_at,validation_run_id,reconciliation_run_id}', n.nspname, c.relname)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE (n.nspname IN ('warehouse', 'marts') AND c.relkind IN ('r', 'v'))
   OR (n.nspname = 'audit' AND c.relname IN ('reconciliation_report', 'scd2_validation_results', 'mart_validation_report'))
ORDER BY 1
\gexec

\echo ## report rows
SELECT 'reconciliation', (to_jsonb(t) - '{checked_at,reconciliation_run_id}'::text[])::text
FROM audit.reconciliation_report t ORDER BY reconciliation_id;
SELECT 'scd2', (to_jsonb(t) - '{checked_at,validation_run_id}'::text[])::text
FROM audit.scd2_validation_results t ORDER BY validation_result_id;
SELECT 'mart', (to_jsonb(t) - '{checked_at,validation_run_id}'::text[])::text
FROM audit.mart_validation_report t ORDER BY validation_id;

\echo ## columns
SELECT table_schema, table_name, ordinal_position, column_name, data_type, coalesce(numeric_precision::text, ''),
       coalesce(numeric_scale::text, ''), is_nullable, coalesce(column_default, ''), is_identity
FROM information_schema.columns WHERE table_schema IN ('warehouse', 'marts', 'audit') ORDER BY 1, 2, 3;

\echo ## constraints
SELECT n.nspname, c.relname, k.conname, k.contype, pg_get_constraintdef(k.oid)
FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('warehouse', 'marts', 'audit') ORDER BY 1, 2, 3;

\echo ## indexes
SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes
WHERE schemaname IN ('warehouse', 'marts', 'audit') ORDER BY 1, 2, 3;

\echo ## views
SELECT n.nspname, c.relname, md5(pg_get_viewdef(c.oid))
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('warehouse', 'marts', 'audit') AND c.relkind = 'v' ORDER BY 1, 2;

\echo ## functions
SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), l.lanname, p.provolatile, p.proparallel,
       p.proisstrict, coalesce(p.proconfig::text, ''), md5(p.prosrc)
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace JOIN pg_language l ON l.oid = p.prolang
WHERE n.nspname IN ('warehouse', 'marts', 'audit') ORDER BY 1, 2, 3;

\echo ## comments
SELECT n.nspname, c.relname, d.objsubid, md5(d.description)
FROM pg_description d JOIN pg_class c ON c.oid = d.objoid AND d.classoid = 'pg_class'::regclass
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('warehouse', 'marts', 'audit') ORDER BY 1, 2, 3;
SELECT nspname, md5(coalesce(obj_description(oid, 'pg_namespace'), '')) FROM pg_namespace
WHERE nspname IN ('staging', 'warehouse', 'marts', 'audit') ORDER BY 1;

\echo ## grants
SELECT n.nspname, c.relname, c.relkind, coalesce(c.relacl::text, '')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('warehouse', 'marts', 'audit') AND c.relkind IN ('r', 'v', 'S') ORDER BY 1, 2;
SELECT nspname, coalesce(nspacl::text, '') FROM pg_namespace
WHERE nspname IN ('warehouse', 'marts', 'audit') ORDER BY 1;
SELECT n.nspname, d.defaclobjtype, d.defaclacl::text
FROM pg_default_acl d JOIN pg_namespace n ON n.oid = d.defaclnamespace
WHERE n.nspname IN ('warehouse', 'marts', 'audit') ORDER BY 1, 2;
