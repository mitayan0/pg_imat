CREATE OR REPLACE FUNCTION create_imat(
    p_object_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS void AS
$$
DECLARE
    v_mv_definition   TEXT;
    v_schema_name     TEXT := p_schema_name;
    v_regclass_oid    OID;

    v_select_list   TEXT;
    v_from_clause   TEXT;
    v_group_by_list TEXT;

    v_base_tables   TEXT[] := ARRAY[]::TEXT[];
    v_table_match   TEXT[];

    col_defs               TEXT := '';
    v_pk_cols_list         TEXT;
    v_backfill_insert_cols TEXT := '';
    backfill_query         TEXT;
    create_table_sql       TEXT;

    function_name          TEXT := 'trg_fn_' || p_object_name;
    v_new_table_name       TEXT := p_object_name || '_new';
    v_old_table_name       TEXT := p_object_name || '_old';
    func_sql               TEXT;

    col_info_record RECORD;

    v_select_items TEXT[];
    v_mv_cols      TEXT[];
    v_pk_positions INT[] := ARRAY[]::INT[];
    v_expr         TEXT;
    v_pos          INT;
    
    -- Variables for smart splitting
    v_split_input TEXT;
    v_split_result TEXT[];
    v_current_item TEXT;
    v_char CHAR(1);
    v_paren_depth INT;
    v_in_single_quote BOOLEAN;
    v_in_double_quote BOOLEAN;
    v_prev_char CHAR(1);
    v_i INT;
BEGIN
    RAISE NOTICE 'Starting IMAT creation for % (using Aggregate Detection)', p_object_name;

    -- 1-5. [Same as before - initialization and parsing]
    INSERT INTO imat.imat_config (mv_name, mv_schema_name, agg_schema_name)
    VALUES (p_object_name, 'imat', v_schema_name)
    ON CONFLICT (mv_name) DO NOTHING;

    BEGIN
        SELECT pg_get_viewdef('imat.' || p_object_name, true)
        INTO v_mv_definition;
        EXECUTE format('REFRESH MATERIALIZED VIEW imat.%I WITH NO DATA;', p_object_name);
        SELECT ('imat.' || p_object_name)::regclass INTO v_regclass_oid;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Error getting MV definition: %', SQLERRM;
    END;

    SELECT (regexp_matches(v_mv_definition, 'SELECT\s+(.*?)\s+FROM', 'is'))[1]
    INTO v_select_list;
    SELECT (regexp_matches(v_mv_definition, 'FROM\s+(.*?)\s+GROUP BY', 'is'))[1]
    INTO v_from_clause;
    SELECT (regexp_matches(v_mv_definition, 'GROUP BY\s+([^;]+?)(?:\s+ORDER BY|\s+HAVING|\s+LIMIT|;|$)', 'i'))[1]
    INTO v_group_by_list;

    v_select_list   := trim(both E' \n\t' FROM v_select_list);
    v_from_clause   := trim(both E' \n\t' FROM v_from_clause);
    v_group_by_list := trim(both E' \n\t' FROM v_group_by_list);

    -- Get base tables
    FOR v_table_match IN
        SELECT regexp_matches(v_from_clause, '(?:^\s*|JOIN\s+)((?:("?\w+"?)\.)?("?\w+"?))', 'gi')
    LOOP
        v_base_tables := array_append(v_base_tables, v_table_match[1]);
    END LOOP;
    v_base_tables := array(SELECT DISTINCT unnest(v_base_tables));

    -- Build column definitions
    v_mv_cols := ARRAY[]::TEXT[];
    FOR col_info_record IN
        SELECT a.attname AS column_name,
               format_type(a.atttypid, a.atttypmod) AS data_type
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = v_regclass_oid
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
    LOOP
        v_mv_cols := array_append(v_mv_cols, col_info_record.column_name);
        col_defs := col_defs || format('%I %s, ', col_info_record.column_name, col_info_record.data_type);
        v_backfill_insert_cols := v_backfill_insert_cols || format('%I, ', col_info_record.column_name);
    END LOOP;

    v_backfill_insert_cols := trim(both ', ' FROM v_backfill_insert_cols);
    col_defs               := trim(both ', ' FROM col_defs);

    -- ================================================================
    -- 6. PRAGMATIC FIX: Detect GROUP BY columns by identifying
    --    which SELECT items are NOT aggregate functions
    -- ================================================================
    v_pk_cols_list := '';
    
    -- Smart split SELECT list
    v_split_input := v_select_list;
    v_split_result := ARRAY[]::TEXT[];
    v_current_item := '';
    v_paren_depth := 0;
    v_in_single_quote := FALSE;
    v_in_double_quote := FALSE;
    v_prev_char := '';

    IF v_split_input IS NOT NULL AND v_split_input != '' THEN
        FOR v_i IN 1..length(v_split_input) LOOP
            v_char := substring(v_split_input FROM v_i FOR 1);

            IF v_char = '''' AND v_prev_char != '\' AND NOT v_in_double_quote THEN
                v_in_single_quote := NOT v_in_single_quote;
            ELSIF v_char = '"' AND v_prev_char != '\' AND NOT v_in_single_quote THEN
                v_in_double_quote := NOT v_in_double_quote;
            END IF;

            IF NOT v_in_single_quote AND NOT v_in_double_quote THEN
                IF v_char = '(' THEN
                    v_paren_depth := v_paren_depth + 1;
                ELSIF v_char = ')' THEN
                    v_paren_depth := v_paren_depth - 1;
                END IF;
            END IF;

            IF v_char = ',' AND v_paren_depth = 0 AND NOT v_in_single_quote AND NOT v_in_double_quote THEN
                v_split_result := array_append(v_split_result, trim(v_current_item));
                v_current_item := '';
            ELSE
                v_current_item := v_current_item || v_char;
            END IF;

            v_prev_char := v_char;
        END LOOP;

        IF v_current_item != '' THEN
            v_split_result := array_append(v_split_result, trim(v_current_item));
        END IF;
    END IF;

    v_select_items := v_split_result;

    -- ================================================================
    -- Identify non-aggregate columns (these are GROUP BY columns)
    -- ================================================================
    FOR v_i IN 1 .. array_length(v_select_items, 1) LOOP
        v_expr := upper(trim(v_select_items[v_i]));
        
        -- Check if this is an aggregate function
        -- List of all PostgreSQL aggregate functions
        IF NOT (
            v_expr ~ '^\s*(COUNT|SUM|AVG|MIN|MAX)\s*\(' OR
            v_expr ~ '^\s*(ARRAY_AGG|STRING_AGG|JSONB_AGG|JSON_AGG)\s*\(' OR
            v_expr ~ '^\s*(BOOL_AND|BOOL_OR|BIT_AND|BIT_OR)\s*\(' OR
            v_expr ~ '^\s*(EVERY|STDDEV|STDDEV_POP|STDDEV_SAMP)\s*\(' OR
            v_expr ~ '^\s*(VARIANCE|VAR_POP|VAR_SAMP)\s*\(' OR
            v_expr ~ '^\s*(COVAR_POP|COVAR_SAMP|CORR)\s*\(' OR
            v_expr ~ '^\s*(REGR_[A-Z]+|PERCENTILE_[A-Z]+|MODE)\s*\('
        ) THEN
            -- Not an aggregate - this is a GROUP BY column
            v_pk_positions := array_append(v_pk_positions, v_i);
            RAISE NOTICE 'Column % identified as GROUP BY (non-aggregate): %', 
                         v_i, v_mv_cols[v_i];
        ELSE
            RAISE NOTICE 'Column % identified as aggregate (not GROUP BY): %', 
                         v_i, v_mv_cols[v_i];
        END IF;
    END LOOP;

    -- Build PK column list
    IF array_length(v_pk_positions, 1) IS NOT NULL THEN
        FOREACH v_pos IN ARRAY v_pk_positions LOOP
            IF v_pos >= 1 AND v_pos <= array_length(v_mv_cols, 1) THEN
                v_pk_cols_list := v_pk_cols_list || format('%I, ', v_mv_cols[v_pos]);
            END IF;
        END LOOP;
    END IF;

    v_pk_cols_list := trim(both ', ' FROM v_pk_cols_list);

    IF v_pk_cols_list = '' THEN
        RAISE WARNING 'Could not determine PK columns. Creating table without PK.';
        v_pk_cols_list := NULL;
    ELSE
        RAISE NOTICE 'Detected primary key columns: %', v_pk_cols_list;
    END IF;

    -- 7-12. [Rest remains the same - table creation, backfill, triggers]
    
    backfill_query := format(
        'INSERT INTO %I.%I (%s) SELECT %s FROM %s GROUP BY %s;',
        v_schema_name, p_object_name, v_backfill_insert_cols,
        v_select_list, v_from_clause, v_group_by_list
    );

    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE;', v_schema_name, p_object_name);

    IF v_pk_cols_list IS NOT NULL THEN
        create_table_sql := format(
            'CREATE TABLE %I.%I (%s, PRIMARY KEY (%s));',
            v_schema_name, p_object_name, col_defs, v_pk_cols_list
        );
    ELSE
        create_table_sql := format('CREATE TABLE %I.%I (%s);', v_schema_name, p_object_name, col_defs);
    END IF;

    EXECUTE create_table_sql;

    RAISE NOTICE 'Backfilling initial data...';
    EXECUTE backfill_query;

    EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE;', v_schema_name, function_name);

    func_sql := format($func$
CREATE OR REPLACE FUNCTION %1$I.%2$I()
RETURNS TRIGGER AS $function$
BEGIN
    DROP TABLE IF EXISTS %1$I.%7$I;
    CREATE TABLE %1$I.%7$I AS SELECT %4$s FROM %5$s GROUP BY %6$s;
    LOCK TABLE %1$I.%3$I IN ACCESS EXCLUSIVE MODE;
    DROP TABLE IF EXISTS %1$I.%8$I;
    ALTER TABLE %1$I.%3$I RENAME TO %8$I;
    ALTER TABLE %1$I.%7$I RENAME TO %3$I;
    DROP TABLE %1$I.%8$I;
    RETURN NULL;
END;
$function$ LANGUAGE plpgsql;
$func$,
        v_schema_name, function_name, p_object_name,
        v_select_list, v_from_clause, v_group_by_list,
        v_new_table_name, v_old_table_name
    );

    EXECUTE func_sql;

    FOR col_info_record IN SELECT unnest(v_base_tables) AS table_name_full
    LOOP
        DECLARE
            v_table_name_full   TEXT := col_info_record.table_name_full;
            v_table_name_simple TEXT;
            trigger_name        TEXT;
        BEGIN
            v_table_name_simple := (regexp_match(v_table_name_full, '"?(\w+)"?\s*$'))[1];
            trigger_name := format('trg_%s_%s', p_object_name, v_table_name_simple);
            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I;', trigger_name, v_table_name_full);
            EXECUTE format(
                'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %I
                 FOR EACH STATEMENT EXECUTE FUNCTION %I.%I();',
                trigger_name, v_table_name_full, v_schema_name, function_name
            );
        END;
    END LOOP;

    RAISE NOTICE 'IMAT creation complete for %.%', v_schema_name, p_object_name;
END;
$$ LANGUAGE plpgsql;