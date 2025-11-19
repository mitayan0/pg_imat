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
