# pg_imat — Incremental Materialized Aggregation Tables for PostgreSQL

**pg_imat** (IMAT) is a lightweight SQL framework that automates aggregation in PostgreSQL.  
It allows you to define **Incremental Materialized Aggregation Tables (IMATs)** —
aggregate tables that automatically stay in sync with one or more base tables.

Unlike standard materialized views that require manual refresh, IMATs are **trigger-driven**.  
Whenever you `INSERT`, `UPDATE`, or `DELETE` data in a source table, the aggregate table updates instantly.

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
