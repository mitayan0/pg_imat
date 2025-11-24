# pg_imat - PostgreSQL Incremental Materialized Tables

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-blue.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**pg_imat** is a high-performance PostgreSQL extension designed to transform standard materialized views into incrementally maintained, real-time aggregate tables. 

> **Note:** This project is actively used in the **PROCG** backend database to power high-speed aggregate reporting from predefined materialized views.

---

## 🚀 Overview

Traditional PostgreSQL materialized views are powerful but suffer from a major drawback: `REFRESH MATERIALIZED VIEW` is an expensive operation that often locks the view or requires a complete rebuild.

**pg_imat** solves this by converting your materialized views into **self-maintaining tables**. It uses a smart combination of triggers and table swapping to ensure your aggregate data is always up-to-date with the base tables, without the performance penalty of full refreshes.

### Why use pg_imat?

- **Real-Time Data**: Aggregates are updated instantly as data changes in the underlying tables.
- **Zero Maintenance**: No need to schedule cron jobs for `REFRESH MATERIALIZED VIEW`.
- **High Performance**: Optimized for read-heavy workloads that need fresh data.
- **Seamless Integration**: Works with your existing materialized view definitions.

---

## ✨ Key Features

- **Automatic Trigger Generation**: Automatically creates triggers on base tables to detect changes (INSERT, UPDATE, DELETE).
- **Smart Column Detection**: Intelligently distinguishes between `GROUP BY` columns and aggregate functions.
- **Atomic Updates**: Uses table swapping to ensure data consistency during updates.
- **Complex Query Support**: Handles complex views with `JOIN`s and multiple aggregation levels.
- **Zero Downtime**: The conversion process and subsequent updates are designed to minimize locking.

---

## 🔧 Installation

### Prerequisites

- PostgreSQL 12 or higher.
- Existing materialized views defined with `GROUP BY` clauses.

### Setup

1.  **Create the Schema**:
    Run the following SQL to create the necessary schema and functions.

    ```sql
    CREATE SCHEMA IF NOT EXISTS imat;
    -- Execute the contents of sql/create_imat.sql here
    ```

2.  **Install the Extension Function**:
    Load the `create_imat` function into your database. You can do this by executing the `sql/create_imat.sql` file using `psql` or your preferred database client.

    ```bash
    psql -d your_database -f sql/create_imat.sql
    ```

---

## 📖 Usage

Using `pg_imat` is straightforward. Once you have a materialized view you want to convert, simply call the `create_imat` function.

### 1. Define your Materialized View

```sql
CREATE MATERIALIZED VIEW my_sales_summary AS
SELECT 
    region,
    product_id,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count
FROM sales
GROUP BY region, product_id;
```

### 2. Convert to Incremental Table

Call the `create_imat` function with the name of your materialized view.

```sql
-- This will create a new table 'my_sales_summary' in the 'imat' schema
-- and set up all necessary triggers.
SELECT create_imat('my_sales_summary');
```

### 3. Enjoy Real-Time Data

Now, whenever you insert, update, or delete rows in the `sales` table, `imat.my_sales_summary` will be automatically updated.

```sql
-- Query the auto-maintained table
SELECT * FROM imat.my_sales_summary;
```

---

## ⚙️ How It Works

1.  **Analysis**: `pg_imat` parses the definition of your materialized view to understand the source tables, grouping columns, and aggregates.
2.  **Table Creation**: It creates a physical table in the `imat` schema with the same structure as your view.
3.  **Backfill**: The table is populated with the current data from the view.
4.  **Trigger Setup**: Triggers are added to all base tables. When a change occurs, the trigger fires a function that re-calculates the affected aggregates and updates the `imat` table.

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License.
