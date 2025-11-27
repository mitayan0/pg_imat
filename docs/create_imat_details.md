# `create_imat` Function Documentation

## Overview
The `create_imat` function is a PostgreSQL PL/pgSQL function designed to automate the creation and maintenance of "Immediate Materialized Views" (IMAT). Unlike standard PostgreSQL Materialized Views which require manual refreshing (or scheduled refreshing), this function converts a defined Materialized View into a regular table that is **automatically updated** whenever its underlying base tables change.

It achieves this by:
1.  Parsing the definition of an existing Materialized View.
2.  Creating a physical table with the same structure.
3.  Identifying the Primary Key (dimensions) vs. Aggregates (metrics).
4.  Setting up triggers on all base tables to perform a full refresh of the target table upon any data modification.

## Function Signature

```sql
FUNCTION create_imat(
    p_object_name TEXT,              -- Name of the existing Materialized View
    p_schema_name TEXT DEFAULT 'public' -- Target schema for the new table
) RETURNS void
```

## Prerequisites
*   The function assumes the existence of an `imat` schema.
*   It expects a configuration table `imat.imat_config` to exist.
*   The source Materialized View should exist in the `imat` schema (referenced as `imat.<p_object_name>`).

## Detailed Workflow

The function executes the following steps:

### 1. Initialization and Configuration
*   Logs the start of the process.
*   Registers the view in `imat.imat_config`.
*   Retrieves the SQL definition of the source Materialized View using `pg_get_viewdef`.
*   Refreshes the source Materialized View to ensure metadata is up-to-date.

### 2. SQL Parsing
The function uses Regular Expressions to deconstruct the view's query into its core components:
*   **SELECT List**: The columns being selected.
*   **FROM Clause**: The source tables and joins.
*   **GROUP BY Clause**: The grouping logic.

It also extracts all **Base Tables** involved in the query by parsing the `FROM` clause. This is crucial for attaching triggers later.

### 3. Column Analysis and "Smart Splitting"
To correctly define the target table, the function needs to understand the columns.
*   It queries `pg_attribute` to get exact column names and data types.
*   **Smart Splitting**: It implements a custom character-by-character parser to split the `SELECT` list. This parser handles complex SQL features like:
    *   Nested parentheses `(...)`
    *   Single quotes `'...'`
    *   Double quotes `"..."`
    *   This ensures that commas inside function calls or strings don't incorrectly split the column list.

### 4. Primary Key Detection (Aggregate vs. Dimension)
A key feature of this function is its ability to automatically determine the Primary Key of the resulting table. It does this using a heuristic:
*   It iterates through every item in the `SELECT` list.
*   It checks if the item matches a known pattern of PostgreSQL **Aggregate Functions** (e.g., `COUNT`, `SUM`, `AVG`, `JSON_AGG`, etc.).
*   **Logic**:
    *   If an item is **NOT** an aggregate function, it is considered a **Dimension** (part of the `GROUP BY`).
    *   These Dimension columns are collected to form the **Primary Key** of the new table.

### 5. Table Creation and Backfill
*   It drops the target table (if it exists) in the destination schema (`p_schema_name`).
*   It creates a new table with the detected columns and Primary Key.
*   It executes an `INSERT INTO ... SELECT ...` query to populate the table with initial data from the source query.

### 6. The Trigger System (Auto-Refresh Mechanism)
This is the core of the "Immediate" functionality.

#### The Trigger Function (`trg_fn_<object_name>`)
A dedicated trigger function is generated. When executed, it performs an **Atomic Swap**:
1.  Creates a temporary "New" table (`_new`).
2.  Populates it by re-running the full view query.
3.  Acquires an `ACCESS EXCLUSIVE` lock on the main table.
4.  Renames the current table to "Old" (`_old`).
5.  Renames the "New" table to the actual table name.
6.  Drops the "Old" table.

*Note: This approach ensures that the table is always available for reads, except for the very brief moment of the rename/swap.*

#### Attaching Triggers
Finally, the function iterates through the list of **Base Tables** identified in Step 2.
*   It drops any existing triggers for this IMAT.
*   It creates a new trigger on each base table:
    *   **Events**: `AFTER INSERT OR UPDATE OR DELETE`
    *   **Level**: `FOR EACH STATEMENT`
    *   **Action**: Execute the Trigger Function created above.

## Usage Example

```sql
-- 1. Create a standard Materialized View in the 'imat' schema
CREATE MATERIALIZED VIEW imat.my_summary_mv AS
SELECT 
    user_id, 
    COUNT(*) as total_orders, 
    SUM(amount) as total_spent
FROM orders
GROUP BY user_id;

-- 2. Convert it to an auto-updating IMAT table in 'public'
SELECT create_imat('my_summary_mv', 'public');

-- Result: 
-- A table 'public.my_summary_mv' is created.
-- Triggers are added to the 'orders' table.
-- Any change to 'orders' will immediately rebuild 'public.my_summary_mv'.
```

## Limitations & Considerations
*   **Performance**: The trigger performs a **full rebuild** of the table on every modification to the base tables. This is suitable for read-heavy, write-light workloads or small datasets. For high-write volume tables, this might cause performance bottlenecks.
*   **Parsing**: While the "Smart Split" is robust, extremely complex SQL queries might still confuse the regex parsers.
*   **Dependencies**: The function relies on the `imat` schema convention.
