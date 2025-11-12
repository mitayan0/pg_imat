CREATE OR REPLACE FUNCTION create_imat(
    p_object_name TEXT,     -- Common name (e.g., 't1_agg'), MV name and target table name
    p_schema_name TEXT DEFAULT 'public' -- Schema for the *aggregate table*
)
RETURNS void AS
$$
DECLARE
    v_mv_definition TEXT;
    v_schema_name TEXT := p_schema_name;
    v_regclass_oid OID;

    -- SQL clause parsing
    v_select_list TEXT;
    v_from_clause TEXT;
    v_group_by_list TEXT;

    -- Parsed elements
    v_base_tables TEXT[] := ARRAY[]::TEXT[];
    v_group_col_aliases TEXT[] := ARRAY[]::TEXT[];
    v_group_cols_raw TEXT[] := ARRAY[]::TEXT[];
    
    agg_columns_info JSONB := '{}';
    all_columns_info JSONB := '{}';

    -- Column/Alias parsing
    v_match TEXT[];
    v_col_expr TEXT;
    v_col_expr_raw TEXT;
    v_alias TEXT;
    v_col_name TEXT;
    v_col_data_type TEXT;
    agg_func_type TEXT;
    agg_target_column TEXT;
    v_alias_map RECORD; 

    -- Dynamic SQL for Table/Backfill
    col_defs TEXT := ''; 
    v_pk_cols_list TEXT := '';
    create_table_sql TEXT;
    
    -- Backfill specific variables
    v_backfill_insert_cols TEXT := '';
    v_backfill_select_list TEXT := '';
    v_group_by_expressions TEXT := ''; 
    backfill_query TEXT;

    -- Dynamic SQL for Trigger Function
    v_i INT;
    function_name TEXT := 'trg_fn_' || p_object_name;
    func_sql TEXT;
    trigger_sql TEXT;
    
    v_declare_key_vars TEXT := '';
    v_declare_agg_vars TEXT := '';
    v_assign_old_keys TEXT := ''; 
    v_assign_new_keys TEXT := '';
    v_key_changed_check TEXT := '';
    v_recalc_select_list TEXT := ''; 
    v_recalc_query TEXT;        
    
    -- Variables for Dynamic Execution
    v_placeholder_constraint TEXT := '1=1'; 
    v_using_key_vars TEXT := '';           
    
    v_all_into_vars TEXT := ''; 
    v_insert_key_cols TEXT := '';
    v_insert_agg_cols TEXT := '';
    v_insert_key_vars TEXT := '';
    v_insert_agg_vars TEXT := '';
    v_conflict_target TEXT := '';
    v_update_set_list TEXT := '';
    v_delete_where_clause TEXT := '';
    v_nullify_agg_vars TEXT := ''; 
    
    -- Internal __sum/__count support
    v_has_avg BOOLEAN := false;
    v_target_col TEXT := '*'; 

    col_info_record RECORD;

BEGIN

    INSERT INTO imat.imat_config (mv_name, mv_schema_name, agg_schema_name)
    VALUES(p_object_name, 'imat', v_schema_name)
    ON CONFLICT (mv_name) DO NOTHING;

    -- Get materialized view definition and OID
    BEGIN
        SELECT pg_get_viewdef('imat.' || p_object_name, true) INTO v_mv_definition;
        EXECUTE format('REFRESH MATERIALIZED VIEW imat.%I WITH NO DATA;', p_object_name); 
        SELECT ('imat.' || p_object_name)::regclass INTO v_regclass_oid;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Error getting MV definition or OID for imat.%: %', p_object_name, SQLERRM;
    END;
    
    -- Extract clauses
    SELECT (regexp_matches(v_mv_definition, 'SELECT\s+(.*?)\s+FROM', 'is'))[1] INTO v_select_list;
    SELECT (regexp_matches(v_mv_definition, 'FROM\s+(.*?)\s+GROUP BY', 'is'))[1] INTO v_from_clause;
    SELECT (regexp_matches(v_mv_definition, 'GROUP BY\s+([^;]+?)(?:\s+ORDER BY|\s+HAVING|\s+LIMIT|;|$)', 'i'))[1] INTO v_group_by_list;

    v_select_list := trim(both E'\n\t ' FROM v_select_list);
    
    -- Parse all base tables
    FOR v_match IN SELECT regexp_matches(v_from_clause, '(?:^\s*|JOIN\s+)((?:("?\w+"?)\.)?("?\w+"?))', 'gi')
    LOOP
        v_base_tables := array_append(v_base_tables, v_match[1]);
    END LOOP;
    v_base_tables := array(SELECT DISTINCT unnest(v_base_tables)); 
    
    -- Parse SELECT list (for aliases and expressions)
    FOR v_match IN SELECT regexp_matches(v_select_list, '\s*((?:".*?"|[\w.]+\([^)]*\)|[\w.]+|[^,)]+))\s*(?:AS\s+("?\w+"?))?\s*(?:,\s*|$)', 'gi')
    LOOP
        v_col_expr := trim(v_match[1]);
        
        -- Determine alias: use explicit alias, or implicit column name if unaliased
        v_alias := COALESCE(trim(v_match[2]), trim(regexp_replace(v_col_expr, '.*\W(\w+)$', '\1')));
        v_alias := trim(both '"' from v_alias);

        -- Store the exact expression used in the SELECT list against the alias.
        all_columns_info := jsonb_set(all_columns_info, ARRAY[v_alias], jsonb_build_object('expr', v_col_expr), true);

        SELECT lower((regexp_matches(v_col_expr, '(\w+)\(([^)]*)\)', 'i'))[1]),
               (regexp_matches(v_col_expr, '(\w+)\(([^)]*)\)', 'i'))[2]
        INTO agg_func_type, agg_target_column;

        IF agg_func_type IS NOT NULL THEN
            agg_columns_info := jsonb_set(agg_columns_info, ARRAY[v_alias], jsonb_build_object('type', agg_func_type, 'target', trim(both '"' from agg_target_column)), true);
            IF agg_func_type = 'avg' THEN 
                v_has_avg := true; 
                v_target_col := trim(both '"' from agg_target_column);
            END IF;
        END IF;
    END LOOP;

    -- Parse GROUP BY columns and build key-related variables
    v_group_cols_raw := string_to_array(v_group_by_list, ',');
    v_i := 1;
    v_assign_new_keys := '';
    v_assign_old_keys := '';

    FOREACH v_col_expr_raw IN ARRAY v_group_cols_raw 
    LOOP
        v_col_expr := trim(both E' \t\n' FROM v_col_expr_raw); 
        
        -- Find the alias associated with the GROUP BY item
        v_alias := NULL;
        FOR v_alias_map IN SELECT * FROM jsonb_each(all_columns_info)
        LOOP
            -- V10 FIX: Check if the GROUP BY expression (e.g., 'p.user_id') matches the 
            -- SELECT expression (e.g., 'p.user_id') OR the SELECT alias (e.g., 'user_id').
            IF v_alias_map.value->>'expr' = v_col_expr OR v_alias_map.key = v_col_expr THEN
                v_alias := v_alias_map.key;
                EXIT;
            END IF;
        END LOOP;
        
        IF v_alias IS NULL THEN
            RAISE EXCEPTION 'Could not map GROUP BY item "%" to a SELECT list column/alias. Please ensure grouping columns are in the SELECT list.', v_col_expr_raw;
        END IF;
        
        -- Get data type (for key declaration)
        SELECT format_type(atttypid, atttypmod)
        INTO v_col_data_type
        FROM pg_catalog.pg_attribute WHERE attrelid = v_regclass_oid AND attname = v_alias;
        
        -- Build Key Assignment (for trigger)
        -- Assignment uses the column name/alias from the MV (v_alias)
        v_assign_new_keys := v_assign_new_keys || format('v_key_%s := NEW.%I; ', v_i, v_alias);
        v_assign_old_keys := v_assign_old_keys || format('v_key_%s := OLD.%I; ', v_i, v_alias);
        
        -- Build key-related variables for other clauses
        v_placeholder_constraint := v_placeholder_constraint || format(' AND %s = $%s', v_col_expr, v_i); 
        v_using_key_vars := v_using_key_vars || format('v_key_%s, ', v_i);
        v_pk_cols_list := v_pk_cols_list || format('%I, ', v_alias);
        v_conflict_target := v_conflict_target || format('%I, ', v_alias);
        v_insert_key_cols := v_insert_key_cols || format('%I, ', v_alias);
        v_insert_key_vars := v_insert_key_vars || format('v_key_%s, ', v_i);
        v_declare_key_vars := v_declare_key_vars || format('v_key_%s %s; ', v_i, v_col_data_type);
        v_delete_where_clause := v_delete_where_clause || format(' AND %I = v_key_%s', v_alias, v_i);
        
        -- The key change check uses the full expression against NEW/OLD context
        v_key_changed_check := v_key_changed_check || format('OLD.%s IS DISTINCT FROM NEW.%s OR ', v_col_expr, v_col_expr);
        
        -- Collect the full key expression for the GROUP BY clause of the backfill/recalc queries
        v_group_by_expressions := v_group_by_expressions || format('%s, ', v_col_expr);
        
        v_group_col_aliases := array_append(v_group_col_aliases, v_alias);
        v_i := v_i + 1;
    END LOOP;
    
    -- Trim trailing syntax
    v_pk_cols_list := trim(both ', ' FROM v_pk_cols_list);
    v_conflict_target := trim(both ', ' FROM v_conflict_target);
    v_insert_key_cols := trim(both ', ' FROM v_insert_key_cols);
    v_insert_key_vars := trim(both ', ' FROM v_insert_key_vars);
    v_delete_where_clause := trim(leading ' AND ' FROM v_delete_where_clause);
    v_key_changed_check := trim(trailing ' OR ' FROM v_key_changed_check);
    v_assign_new_keys := trim(trailing '; ' FROM v_assign_new_keys);
    v_assign_old_keys := trim(trailing '; ' FROM v_assign_old_keys);
    v_group_by_expressions := trim(both ', ' FROM v_group_by_expressions);

    -- Build column definitions, INSERT/UPDATE lists, and recalc query SELECT list
    FOR col_info_record IN
        SELECT attname AS column_name,
               format_type(atttypid, atttypmod) AS data_type
        FROM pg_catalog.pg_attribute pa
        WHERE pa.attrelid = v_regclass_oid AND pa.attnum > 0 AND NOT pa.attisdropped
        ORDER BY pa.attnum
    LOOP
        v_col_name := col_info_record.column_name;
        v_col_data_type := col_info_record.data_type;

        col_defs := col_defs || format('%I %s, ', v_col_name, v_col_data_type);
        
        -- Backfill columns list (all columns in MV)
        v_backfill_insert_cols := v_backfill_insert_cols || format('%I, ', v_col_name);
        v_backfill_select_list := v_backfill_select_list || format('%s, ', all_columns_info->v_col_name->>'expr');

        IF v_col_name NOT IN (SELECT unnest(v_group_col_aliases)) THEN
            -- Aggregate/Non-key column setup
            v_declare_agg_vars := v_declare_agg_vars || format('v_agg_%s %s; ', v_col_name, v_col_data_type);
            v_nullify_agg_vars := v_nullify_agg_vars || format('v_agg_%s := NULL; ', v_col_name); 
            v_insert_agg_cols := v_insert_agg_cols || format('%I, ', v_col_name);
            v_insert_agg_vars := v_insert_agg_vars || format('v_agg_%s, ', v_col_name);
            v_update_set_list := v_update_set_list || format('%I = EXCLUDED.%I, ', v_col_name, v_col_name);
            v_all_into_vars := v_all_into_vars || format('v_agg_%s, ', v_col_name);

            -- Recalc SELECT list (for trigger)
            v_recalc_select_list := v_recalc_select_list || format('%s, ', all_columns_info->v_col_name->>'expr');
        ELSE
            -- Key column setup: Only add its key var to INTO list for the trigger's EXECUTE
            v_all_into_vars := v_all_into_vars || format('v_key_%s, ', array_position(v_group_col_aliases, v_col_name));
            
            -- Recalc SELECT list (for trigger)
            v_recalc_select_list := v_recalc_select_list || format('%s, ', all_columns_info->v_col_name->>'expr');
        END IF;
    END LOOP;

    -- Add internal __sum and __count if AVG() is used
    IF v_has_avg THEN
        col_defs := col_defs || format('%I float, %I bigint, ', '__sum', '__count');
        v_backfill_insert_cols := v_backfill_insert_cols || format('%I, %I, ', '__sum', '__count');
        
        v_declare_agg_vars := v_declare_agg_vars || E'\n\tv___sum float; \n\tv___count bigint;';
        v_nullify_agg_vars := v_nullify_agg_vars || E'\n\tv___sum := NULL; \n\tv___count := 0; '; 
        v_insert_agg_cols := v_insert_agg_cols || format('%I, %I, ', '__sum', '__count');
        v_insert_agg_vars := v_insert_agg_vars || format('v___sum, v___count, ');
        v_update_set_list := v_update_set_list || format('%I = EXCLUDED.%I, %I = EXCLUDED.%I, ', '__sum', '__sum', '__count', '__count');
        
        -- Add to SELECT list for backfill and trigger's EXECUTE
        v_backfill_select_list := v_backfill_select_list || format('SUM(%s), COUNT(%s), ', v_target_col, v_target_col);
        v_recalc_select_list := v_recalc_select_list || format('SUM(%s), COUNT(%s), ', v_target_col, v_target_col);
        v_all_into_vars := v_all_into_vars || 'v___sum, v___count, ';
    END IF;
    
    -- Final trim of all SQL snippets
    v_backfill_insert_cols := trim(both ', ' FROM v_backfill_insert_cols);
    v_backfill_select_list := trim(both ', ' FROM v_backfill_select_list);
    v_nullify_agg_vars := trim(trailing E'; ' FROM v_nullify_agg_vars); 
    v_insert_agg_cols := trim(both ', ' FROM v_insert_agg_cols);
    v_insert_agg_vars := trim(both ', ' FROM v_insert_agg_vars);
    v_update_set_list := trim(both ', ' FROM v_update_set_list);
    v_recalc_select_list := trim(both ', ' FROM v_recalc_select_list);
    v_using_key_vars := trim(both ', ' FROM v_using_key_vars);
    v_all_into_vars := trim(both ', ' FROM v_all_into_vars);
    col_defs := trim(both ', ' FROM col_defs);
    
    -- Build the final recalculation query (for trigger)
    v_recalc_query := format(
        'SELECT %1$s FROM %2$s WHERE %3$s GROUP BY %4$s', 
        v_recalc_select_list, v_from_clause, v_placeholder_constraint, v_group_by_expressions 
    );
    
    -- Build the backfill query (for initial data load)
    backfill_query := format(
        'INSERT INTO %I.%I (%s) SELECT %s FROM %s GROUP BY %s;',
        v_schema_name, p_object_name, v_backfill_insert_cols, v_backfill_select_list, v_from_clause, v_group_by_expressions 
    );

    -- 1. Create Aggregate Table
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE;', v_schema_name, p_object_name);
    create_table_sql := format('CREATE TABLE %I.%I (%s, PRIMARY KEY (%s));', v_schema_name, p_object_name, col_defs, v_pk_cols_list);
    EXECUTE create_table_sql;
    
    -- 2. Backfill Data 🚀
    RAISE NOTICE 'Backfilling initial data for %.%', v_schema_name, p_object_name;
    EXECUTE backfill_query;
    RAISE NOTICE 'Backfill complete.';

    -- 3. Drop existing trigger function
    EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE;', v_schema_name, function_name);

    -- 4. Create Master Trigger Function
    func_sql := format($func$
CREATE FUNCTION %1$I.%2$I()
RETURNS TRIGGER AS $function$
DECLARE
    %3$s 
    %4$s 
    v_row_exists INT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        %5$s; 
    ELSE
        %6$s; 
    END IF;

    -- Recalculate the group for these keys
    BEGIN
        EXECUTE %7$L INTO %18$s USING %19$s; 
        GET DIAGNOSTICS v_row_exists = ROW_COUNT;
    EXCEPTION WHEN division_by_zero THEN
        v_row_exists := 1; 
        %17$s; 
        v_row_exists := 1; 
    END;

    IF v_row_exists > 0 THEN
        INSERT INTO %1$I.%8$I (%9$s, %10$s)
        VALUES (%11$s, %12$s)
        ON CONFLICT (%13$s) DO UPDATE
        SET %14$s;
    ELSE
        DELETE FROM %1$I.%8$I WHERE %15$s;
    END IF;

    IF TG_OP = 'UPDATE' AND (%16$s) THEN
        %5$s; 
        
        BEGIN
            EXECUTE %7$L INTO %18$s USING %19$s; 
            GET DIAGNOSTICS v_row_exists = ROW_COUNT;
        EXCEPTION WHEN division_by_zero THEN
            v_row_exists := 1; 
            %17$s; 
            v_row_exists := 1;
        END;

        IF v_row_exists > 0 THEN
            INSERT INTO %1$I.%8$I (%9$s, %10$s)
            VALUES (%11$s, %12$s)
            ON CONFLICT (%13$s) DO UPDATE
            SET %14$s;
        ELSE
            DELETE FROM %1$I.%8$I WHERE %15$s;
        END IF;
    END IF;
    
    RETURN NULL; 
END;
$function$ LANGUAGE plpgsql;
$func$,
        v_schema_name, function_name, v_declare_key_vars, v_declare_agg_vars, 
        v_assign_old_keys, v_assign_new_keys, v_recalc_query, p_object_name, 
        v_insert_key_cols, v_insert_agg_cols, v_insert_key_vars, v_insert_agg_vars, 
        v_conflict_target, v_update_set_list, v_delete_where_clause, v_key_changed_check, 
        v_nullify_agg_vars, v_all_into_vars, v_using_key_vars
    );

    EXECUTE func_sql;

    -- 5. Create Triggers on Base Tables
    FOR col_info_record IN SELECT unnest(v_base_tables) AS table_name_full
    LOOP
        DECLARE
            v_table_name_full TEXT := col_info_record.table_name_full;
            v_table_name_simple TEXT;
            trigger_name TEXT;
        BEGIN
            v_table_name_simple := (regexp_match(v_table_name_full, '"?(\w+)"?\s*$'))[1];
            trigger_name := format('trg_%s_%s', p_object_name, v_table_name_simple);

            EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s;', trigger_name, v_table_name_full);
            EXECUTE format('
                CREATE TRIGGER %I
                AFTER INSERT OR UPDATE OR DELETE ON %s
                FOR EACH ROW
                EXECUTE FUNCTION %I.%I();',
                trigger_name, v_table_name_full, v_schema_name, function_name
            );
        END;
    END LOOP;

END;
$$ LANGUAGE plpgsql;

