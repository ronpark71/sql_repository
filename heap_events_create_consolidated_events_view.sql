CREATE OR REPLACE PROCEDURE schema.vw_heap_consolidated_events_view( view_schema character varying)
LANGUAGE plpgsql
AS $$
DECLARE
    row RECORD;
    create_view_sql VARCHAR(65535) := ' ';
    union_view_sql VARCHAR(65535) := ' ';
BEGIN

    DROP TABLE IF EXISTS temp_queries;

    -- Step 1: Create the temp table to store SELECT statements.
    CREATE TEMP TABLE temp_queries (
        table_name VARCHAR(500),
        query_part VARCHAR(65535),
        rn INTEGER
    );

    -- Step 2: Insert the dynamic queries into the temp table.
    INSERT INTO temp_queries (table_name, query_part, rn)

WITH cte_column_info AS (
    SELECT
        c.table_name AS table_name,

--you can rename columns here
        CASE
            WHEN c.column_name = 'col1' THEN '"col1"'
            WHEN c.column_name = 'col2' THEN '"col2"'
            ELSE c.column_name
        END AS column_name,
        c.data_type AS data_type
    FROM SVV_COLUMNS c
    join svv_tables t on t.TABLE_name = c.table_name and t.table_schema = c.table_schema
    WHERE c.table_schema = '<enter heap events schema name here>'
    and t.table_type = 'BASE TABLE'
    AND c.table_name NOT IN (
        '_dropped_tables', '_event_metadata', '_sync_history', '_sync_info'  ---need to filter out any tables that you dont want to consolidate here
    )
    AND c.column_name <> 'event_table_name'
    AND c.table_name in (     select distinct c.table_name AS table_name
    from SVV_COLUMNS c
    join svv_tables t on t.TABLE_name = c.table_name and t.table_schema = c.table_schema
    WHERE c.table_schema = '<enter heap events schema name here>'
    and t.table_type = 'BASE TABLE'
    and c.column_name = 'event_id'  )
    ORDER BY c.table_name, c.column_name
),

cte_all_columns AS (
    SELECT DISTINCT
        column_name
    FROM cte_column_info
),

cte_all_tables AS (
    SELECT DISTINCT table_name FROM cte_column_info
),

cte_all_tables_columns AS (
    SELECT DISTINCT table_name, column_name FROM cte_all_tables CROSS JOIN cte_all_columns
),

cte_formatted AS (
    SELECT
        a.table_name::varchar(500),
        a.column_name,

--begin of section may be needed to consolidate column names across events
        CASE WHEN c.column_name IS NOT NULL and a.column_name = '"col1"'     
                THEN a.column_name || ' AS col1_'
             WHEN c.column_name IS NOT NULL and c.column_name = '"col2"'
                THEN a.column_name || ' AS col2_'
            WHEN a.column_name = '"col1"'
                THEN 'NULL as col1_'
            WHEN a.column_name = '"col2"'
                THEN 'NULL as col2_'
--end of section

            WHEN c.column_name IS NOT NULL
                THEN a.column_name
            ELSE 'NULL as ' || a.column_name
        END ::varchar(100) AS column_formatted
    FROM cte_all_tables_columns a
    LEFT JOIN cte_column_info c
        ON c.table_name = a.table_name
        AND c.column_name = a.column_name
),

cte_final_sql AS (
    SELECT
        table_name,
        LISTAGG(column_formatted, ', ') WITHIN GROUP (ORDER BY column_name) AS select_text
    FROM cte_formatted
    GROUP BY table_name
)

SELECT table_name,
    'SELECT ''' || table_name || ''' as event_table_name, ' || select_text ||
    ' FROM <enter heap database here>.<enter heap events schema name here>.' || table_name  AS query_part,
    ROW_NUMBER() OVER (ORDER BY table_name) as rn
FROM cte_final_sql
ORDER BY ROW_NUMBER() OVER (ORDER BY table_name)
;

         -- Create or replace a view for each row.
    EXECUTE 'DROP VIEW IF EXISTS ' || view_schema || '.<enter name of the new view here> CASCADE';   --maybe should make new view name a parameter

    -- Step 3: Loop through the rows in temp_queries and create or replace views.  Creates one view per event table
    FOR row IN SELECT * FROM temp_queries LOOP
        -- Create or replace a view for each row.
        EXECUTE 'DROP VIEW IF EXISTS ' || view_schema || '.v_stg_' || row.table_name || ' CASCADE';

        create_view_sql := 'CREATE OR REPLACE VIEW ' || view_schema || '.v_stg_' || row.table_name || ' AS ' || row.query_part || ' WITH NO SCHEMA BINDING';
        EXECUTE create_view_sql;
    END LOOP;

    -- Step 4: Create the final view that unions all individual views.
    FOR row IN SELECT * FROM temp_queries LOOP

        union_view_sql := union_view_sql || 'SELECT * FROM ' || view_schema || '.v_stg_' || row.table_name || ' UNION ALL ';

    END LOOP;

 -- Print the final UNION query for debugging
    RAISE NOTICE 'Final UNION Query: %', union_view_sql;

    -- Remove the last 'UNION ALL' and execute the final view creation.
    IF LENGTH(union_view_sql) > 0 THEN
        union_view_sql := LEFT(union_view_sql, LENGTH(union_view_sql) - 10);
        EXECUTE 'CREATE OR REPLACE VIEW ' || view_schema || '.<enter name of the new view here> AS ' || union_view_sql || ' WITH NO SCHEMA BINDING';
    END IF;

    -- Clean up: Drop the temp table after use.
    DROP TABLE IF EXISTS temp_queries;

    -- Perform Grants
    --EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA ' || view_schema || ' TO GROUP XXXXXXX';

END;
$$;
