/* =============================================================================
Start a new warehouse generation (first file of the atomic warehouse build)
===============================================================================

A rebuild must never show a reader an error, an empty mart or a half-built
warehouse. So the build does not drop or change anything a reader can see.
In ONE transaction it:

  1. renames the live schemas aside (warehouse -> warehouse_retired_<n>, and
     the same for marts, audit and staging),
  2. creates empty warehouse, marts and audit schemas and takes the freshly
     loaded staging_next as staging,
  3. runs the build files (create_warehouse_tables.sql,
     scd2_validation_queries.sql, create_marts.sql, reconciliation_report.sql),
  4. checks the quality gate at the end of reconciliation_report.sql: a failing
     check rolls everything back,
  5. commits.

Until the commit every other session still resolves the names warehouse, marts
and audit to the old schemas, which nothing has touched: no lock, no wait. From
the commit on, the same names are the new generation, complete.

Views outside the generation (the dbt views, a consumer's own views) are bound
to the tables they were created on, so they keep answering from the retired
generation until they are replaced; dbt run replaces each of its views in its
own transaction. A retired generation is dropped by a later build, once no view
outside it reads it. If dbt or the export fails, the old dbt views therefore
still exist and still work.

Run it only through the build, which supplies the transaction:

    bash scripts/run_warehouse_sql.sh sql/warehouse/begin_generation.sql \
        sql/warehouse/create_warehouse_tables.sql sql/warehouse/scd2_validation_queries.sql \
        sql/warehouse/create_marts.sql sql/warehouse/reconciliation_report.sql
============================================================================= */

-- Outside a transaction block this fails, and nothing below runs.
SAVEPOINT generation_needs_the_build_transaction;
RELEASE SAVEPOINT generation_needs_the_build_transaction;

-- ---------------------------------------------------------------------------
-- 1. Drop the generations retired by earlier builds that nothing reads any
--    more. One that a view outside it still reads (a dbt view that was not
--    replaced because dbt failed) is kept.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    generation RECORD;
    outside_views BIGINT;
BEGIN
    FOR generation IN
        SELECT substring(nspname FROM '_retired_(\d+)$') AS tag,
               array_agg(oid) AS schema_oids,
               string_agg(quote_ident(nspname), ', ' ORDER BY nspname) AS schema_list
        FROM pg_namespace
        WHERE nspname ~ '^(staging|warehouse|marts|audit)_retired_\d+$'
        GROUP BY 1
        ORDER BY min(oid)   -- oldest first: a later one may be read by an earlier one's views
    LOOP
        SELECT count(DISTINCT reader.oid)
        INTO outside_views
        FROM pg_depend d
        JOIN pg_rewrite rule ON d.classid = 'pg_rewrite'::regclass AND rule.oid = d.objid
        JOIN pg_class reader ON reader.oid = rule.ev_class
        WHERE reader.relnamespace <> ALL (generation.schema_oids)
          AND (
                (d.refclassid = 'pg_class'::regclass AND EXISTS (
                    SELECT 1 FROM pg_class c
                    WHERE c.oid = d.refobjid AND c.relnamespace = ANY (generation.schema_oids)))
             OR (d.refclassid = 'pg_proc'::regclass AND EXISTS (
                    SELECT 1 FROM pg_proc f
                    WHERE f.oid = d.refobjid AND f.pronamespace = ANY (generation.schema_oids)))
          );

        IF outside_views > 0 THEN
            RAISE NOTICE 'Kept retired generation % (%): % view(s) outside it still read it.',
                generation.tag, generation.schema_list, outside_views;
            CONTINUE;
        END IF;

        BEGIN
            -- Nothing reads it, so the locks are free; never wait long for one.
            SET LOCAL lock_timeout = '5s';
            EXECUTE 'DROP SCHEMA ' || generation.schema_list || ' CASCADE';
            RAISE NOTICE 'Dropped retired generation % (%).', generation.tag, generation.schema_list;
        EXCEPTION WHEN lock_not_available OR deadlock_detected THEN
            RAISE NOTICE 'Kept retired generation % (%): it is still in use.', generation.tag, generation.schema_list;
        END;
        SET LOCAL lock_timeout = 0;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 2. Retire the live generation and create the new, empty one.
--    Renaming a schema locks no table and no view.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    tag TEXT := '_retired_' || txid_current();
    -- The staging loader writes to staging_next. Without one (a hand-made
    -- staging schema) the existing staging is built from and stays in place.
    promote_staging BOOLEAN := to_regnamespace('staging_next') IS NOT NULL;
    reader_exists BOOLEAN := EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'p2_reader');
    schema_name TEXT;
    schema_comment TEXT;
BEGIN
    IF NOT promote_staging AND to_regnamespace('staging') IS NULL THEN
        RAISE EXCEPTION 'No staging_next and no staging schema: load staging first (scripts/load_gold_to_postgres_staging.py).';
    END IF;

    FOREACH schema_name IN ARRAY ARRAY['staging', 'warehouse', 'marts', 'audit'] LOOP
        CONTINUE WHEN schema_name = 'staging' AND NOT promote_staging;

        schema_comment := NULL;
        IF to_regnamespace(schema_name) IS NOT NULL THEN
            schema_comment := obj_description(to_regnamespace(schema_name), 'pg_namespace');
            EXECUTE format('ALTER SCHEMA %I RENAME TO %I', schema_name, schema_name || tag);
            IF reader_exists THEN
                -- Views that already read the retired tables keep working;
                -- the retired names themselves are not for consumers.
                EXECUTE format('REVOKE ALL ON SCHEMA %I FROM p2_reader', schema_name || tag);
            END IF;
        END IF;

        IF schema_name = 'staging' THEN
            ALTER SCHEMA staging_next RENAME TO staging;
        ELSE
            EXECUTE format('CREATE SCHEMA %I', schema_name);
            IF reader_exists THEN
                -- The same grants as sql/warehouse/create_schemas.sql: they
                -- belong to a schema, and this is a new one.
                EXECUTE format('GRANT USAGE ON SCHEMA %I TO p2_reader', schema_name);
                EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO p2_reader', schema_name);
            END IF;
        END IF;
        IF schema_comment IS NOT NULL THEN
            EXECUTE format('COMMENT ON SCHEMA %I IS %L', schema_name, schema_comment);
        END IF;
    END LOOP;

    -- The staging loader's history is not rebuilt, so it moves to the new
    -- audit schema (the view first: readers lock a view before its table).
    IF to_regclass(format('%I.v_gold_to_staging_latest_counts', 'audit' || tag)) IS NOT NULL THEN
        EXECUTE format('ALTER VIEW %I.v_gold_to_staging_latest_counts SET SCHEMA audit', 'audit' || tag);
    END IF;
    IF to_regclass(format('%I.gold_to_staging_load_audit', 'audit' || tag)) IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %I.gold_to_staging_load_audit SET SCHEMA audit', 'audit' || tag);
    END IF;

    RAISE NOTICE 'New warehouse generation started; the live one is retired as *%.', tag;
END $$;
