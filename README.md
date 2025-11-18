# pg_imat - PostgreSQL Incremental Materialized Tables

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-blue.svg)](https://www.postgresql.org/)

## Overview

pg_imat (PostgreSQL Incremental Materialized Tables) is an advanced database extension that transforms traditional PostgreSQL materialized views into high-performance, automatically maintained tables. It provides near real-time data synchronization with significantly better performance than standard materialized view refreshes.

## 🚀 Key Features

- **Automatic Maintenance**: Converts materialized views into self-maintaining tables with automatic triggers
- **Real-time Updates**: Changes to base tables automatically propagate to aggregated tables
- **Smart Column Detection**: Automatically identifies GROUP BY columns vs aggregate columns
- **Performance Optimized**: Uses efficient table swapping instead of incremental updates
- **Zero Downtime Migration**: Seamlessly converts existing materialized views
- **Flexible Schema Support**: Works with complex queries including JOINs and multiple aggregations

## 📋 Prerequisites

- PostgreSQL 12 or higher
- Existing materialized views with GROUP BY clauses
- Appropriate database permissions for creating triggers and functions

## 🔧 Installation

### Step 1: Create Required Schema

```sql
CREATE SCHEMA IF NOT EXISTS imat;

```

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
