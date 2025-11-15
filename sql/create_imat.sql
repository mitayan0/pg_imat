CREATE OR REPLACE FUNCTION create_imat(
    p_object_name TEXT,                  -- MV name (without schema) and aggregate table name
    p_schema_name TEXT DEFAULT 'public'  -- schema for the aggregate table
)
RETURNS void AS
$$
DECLARE
    v_mv_definition   TEXT;
    v_schema_name     TEXT := p_schema_name;
    v_regclass_oid    OID;

    -- Parsed clauses
    v_select_list   TEXT;
    v_from_clause   TEXT;
    v_group_by_list TEXT;

    -- Base tables for trigger creation
    v_base_tables   TEXT[] := ARRAY[]::TEXT[];
    v_table_match   TEXT[];

    -- Dynamic SQL for table/backfill
    col_defs               TEXT := '';
    v_pk_cols_list         TEXT := '';
    v_backfill_insert_cols TEXT := '';
    backfill_query         TEXT;
    create_table_sql       TEXT;

    -- Trigger function SQL
    function_name          TEXT := 'trg_fn_' || p_object_name;
    func_sql               TEXT;

    col_info_record RECORD;
BEGIN
    -- 1. Store in imat config
    INSERT INTO imat.imat_config (mv_name, mv_schema_name, agg_schema_name)
    VALUES (p_object_name, 'imat', v_schema_name)
    ON CONFLICT (mv_name) DO NOTHING;

    -- 2. Get MV definition and OID
    BEGIN
        SELECT pg_get_viewdef('imat.' || p_object_name, true)
        INTO v_mv_definition;

        EXECUTE format('REFRESH MATERIALIZED VIEW imat.%I WITH NO DATA;', p_object_name);

        SELECT ('imat.' || p_object_name)::regclass
        INTO v_regclass_oid;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Error getting MV definition or OID for imat.%: %',
                        p_object_name, SQLERRM;
    END;

    -- 3. Extract SELECT / FROM / GROUP BY from MV SQL
    SELECT (regexp_matches(v_mv_definition, 'SELECT\s+(.*?)\s+FROM', 'is'))[1]
    INTO v_select_list;

    SELECT (regexp_matches(v_mv_definition, 'FROM\s+(.*?)\s+GROUP BY', 'is'))[1]
    INTO v_from_clause;

    SELECT (regexp_matches(
        v_mv_definition,
        'GROUP BY\s+([^;]+?)(?:\s+ORDER BY|\s+HAVING|\s+LIMIT|;|$)',
        'i'
    ))[1]
    INTO v_group_by_list;

    v_select_list   := trim(both E' \n\t' FROM v_select_list);
    v_from_clause   := trim(both E' \n\t' FROM v_from_clause);
    v_group_by_list := trim(both E' \n\t' FROM v_group_by_list);

    -- 4. Collect base tables from FROM/JOIN for trigger creation
    FOR v_table_match IN
        SELECT regexp_matches(
                   v_from_clause,
                   '(?:^\s*|JOIN\s+)((?:("?\w+"?)\.)?("?\w+"?))',
                   'gi'
               )
    LOOP
        -- regexp_matches returns TEXT[], index 1 is the full match
        v_base_tables := array_append(v_base_tables, v_table_match[1]);
    END LOOP;

    v_base_tables := array(SELECT DISTINCT unnest(v_base_tables));

    -- 5. Build aggregate table columns based on MV's physical columns
    FOR col_info_record IN
        SELECT attname AS column_name,
               format_type(atttypid, atttypmod) AS data_type
        FROM pg_catalog.pg_attribute
        WHERE attrelid = v_regclass_oid
          AND attnum > 0
          AND NOT attisdropped
        ORDER BY attnum
    LOOP
        col_defs := col_defs
                    || format('%I %s, ', col_info_record.column_name, col_info_record.data_type);
        v_backfill_insert_cols := v_backfill_insert_cols
                                  || format('%I, ', col_info_record.column_name);
    END LOOP;

    v_backfill_insert_cols := trim(both ', ' FROM v_backfill_insert_cols);
    col_defs               := trim(both ', ' FROM col_defs);

    -- 6. Derive primary key columns from GROUP BY list (by matching column names)
    v_pk_cols_list := '';

    FOR col_info_record IN
        SELECT attname AS column_name
        FROM pg_catalog.pg_attribute
        WHERE attrelid = v_regclass_oid
          AND attnum > 0
          AND NOT attisdropped
    LOOP
        IF position(col_info_record.column_name IN v_group_by_list) > 0 THEN
            v_pk_cols_list := v_pk_cols_list || format('%I, ', col_info_record.column_name);
        END IF;
    END LOOP;

    -- Fallback: if none detected, use all MV columns as PK (not ideal, but safe)
    IF v_pk_cols_list = '' THEN
        FOR col_info_record IN
            SELECT attname AS column_name
            FROM pg_catalog.pg_attribute
            WHERE attrelid = v_regclass_oid
              AND attnum > 0
              AND NOT pa.attisdropped
        LOOP
            v_pk_cols_list := v_pk_cols_list || format('%I, ', col_info_record.column_name);
        END LOOP;
    END IF;

    v_pk_cols_list := trim(both ', ' FROM v_pk_cols_list);

    -- 7. Backfill query: use MV's SELECT list directly
    backfill_query := format(
        'INSERT INTO %I.%I (%s) SELECT %s FROM %s GROUP BY %s;',
        v_schema_name, p_object_name,
        v_backfill_insert_cols,  -- targets (agg table columns)
        v_select_list,           -- MV's original SELECT expressions
        v_from_clause,           -- MV's FROM clause
        v_group_by_list          -- MV's GROUP BY clause
    );

    -- 8. Create aggregate table
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE;', v_schema_name, p_object_name);

    create_table_sql := format(
        'CREATE TABLE %I.%I (%s, PRIMARY KEY (%s));',
        v_schema_name, p_object_name, col_defs, v_pk_cols_list
    );
    EXECUTE create_table_sql;

    -- 9. Backfill initial data
    RAISE NOTICE 'Backfilling initial data for %.%', v_schema_name, p_object_name;
    EXECUTE backfill_query;
    RAISE NOTICE 'Backfill complete.';

    -- 10. Drop existing trigger function (if any)
    EXECUTE format(
        'DROP FUNCTION IF EXISTS %I.%I() CASCADE;',
        v_schema_name, function_name
    );

    -- 11. Create trigger function: recompute whole aggregate table on any change
    func_sql := format($func$
CREATE OR REPLACE FUNCTION %1$I.%2$I()
RETURNS TRIGGER AS $function$
BEGIN
    -- Rebuild the aggregate table completely.
    DELETE FROM %1$I.%3$I;

    INSERT INTO %1$I.%3$I (%4$s)
    SELECT %5$s
    FROM %6$s
    GROUP BY %7$s;

    RETURN NULL;
END;
$function$ LANGUAGE plpgsql;
$func$,
        v_schema_name,           -- %1
        function_name,           -- %2
        p_object_name,           -- %3
        v_backfill_insert_cols,  -- %4
        v_select_list,           -- %5
        v_from_clause,           -- %6
        v_group_by_list          -- %7
    );

    EXECUTE func_sql;

    -- 12. Attach triggers to all base tables
    FOR col_info_record IN
        SELECT unnest(v_base_tables) AS table_name_full
    LOOP
        DECLARE
            v_table_name_full   TEXT := col_info_record.table_name_full;
            v_table_name_simple TEXT;
            trigger_name        TEXT;
        BEGIN
            v_table_name_simple :=
                (regexp_match(v_table_name_full, '"?(\w+)"?\s*$'))[1];
            trigger_name := format('trg_%s_%s', p_object_name, v_table_name_simple);

            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s;',
                           trigger_name, v_table_name_full);

            EXECUTE format(
                'CREATE TRIGGER %I
                 AFTER INSERT OR UPDATE OR DELETE ON %s
                 FOR EACH ROW
                 EXECUTE FUNCTION %I.%I();',
                trigger_name, v_table_name_full, v_schema_name, function_name
            );
        END;
    END LOOP;

END;
$$ LANGUAGE plpgsql;