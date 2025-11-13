# pg_imat — Incremental Materialized Aggregation Tables for PostgreSQL

pg_imat is a standalone PostgreSQL function that automates the creation and maintenance of Incremental Materialized Aggregation Tables (IMATs).
It provides a way to define aggregate tables that stay in sync with one or more base tables through automatically generated triggers and logic — without needing to manually refresh a materialized view.

In essence, pg_imat lets you transform any SELECT ... GROUP BY query into a live, self-updating aggregate table, keeping your summary data accurate in real time.

---

## 🚀 Features

- ✅ **Automatic aggregation** — no need to refresh manually.  
- 🔄 **Real-time sync** — changes in base tables propagate automatically.  
- 🧩 **Join & multi-table support** *(planned)* — aggregate across multiple sources.  
- 🗂️ **Config tracking** — every IMAT registered in `imat.imat_config`.  
- 📊 **Flexible grouping** — supports time truncation (`DATE_TRUNC`) and arbitrary GROUP BYs.  
- 🕒 **Compatible with partitioned tables** — works seamlessly on range or hash partitions.  

---

## 🧠 Concept

An **IMAT** behaves like a *materialized view that maintains itself*.

---

## Example

Below is a minimal example to test pg_imat — showing how to set up schemas, create a base table, define an aggregate, and register it as an IMAT.

```sql
-- 1️⃣ Create schemas
CREATE SCHEMA IF NOT EXISTS imat;
CREATE SCHEMA IF NOT EXISTS public;

-- 2️⃣ Create configuration table
CREATE TABLE IF NOT EXISTS imat.imat_config (
    mv_name TEXT PRIMARY KEY,
    mv_schema_name TEXT,
    agg_schema_name TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- 3️⃣ Create a base table
CREATE TABLE public.trips_ts_v1 (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL,
    distance FLOAT NOT NULL,
    total_amount FLOAT NOT NULL,
    cab_type_id INT NOT NULL
);

-- 4️⃣ Insert some test data
INSERT INTO public.trips_ts_v1 (started_at, ended_at, distance, total_amount, cab_type_id)
VALUES
('2025-11-13 08:00', '2025-11-13 08:15', 2.3, 10.5, 1),
('2025-11-13 08:10', '2025-11-13 08:25', 1.8, 9.0, 1),
('2025-11-13 09:00', '2025-11-13 09:10', 3.1, 12.0, 2);

-- 5️⃣ Create the aggregate table (simulating a materialized view)
CREATE MATERIALIZED VIEW imat.trips_ts_v1_hourly_agg AS
SELECT
    cab_type_id,
    DATE_TRUNC('hour', started_at) AS hour_start,
    COUNT(*) AS total_trips,
    SUM(distance) AS total_distance,
    AVG(distance) AS avg_distance,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_revenue
FROM public.trips_ts_v1
GROUP BY cab_type_id, DATE_TRUNC('hour', started_at);

-- 6️⃣ Create the IMAT function

SELECT create_imat('trips_ts_v1_hourly_agg', 'public');


-- 7️⃣ Check configuration
SELECT * FROM imat.imat_config;

-- 8️⃣ Verify aggregate output
SELECT * FROM imat.trips_ts_v1_hourly_agg;

```
