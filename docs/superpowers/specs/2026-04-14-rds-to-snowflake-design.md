# RDS to Snowflake Loader — Design Spec

**Date:** 2026-04-14
**Status:** Approved

---

## Overview

A standalone Python script (`load_snowflake.py`) that reads all 8 raw Basket Craft tables from AWS RDS PostgreSQL and loads them into the `basket_craft.raw` schema in Snowflake. This is the next hop in the pipeline after `load_raw_tables.py` (MySQL → RDS).

---

## Architecture

```
AWS RDS PostgreSQL  (RDS_* env vars)
  └─ SQLAlchemy engine + pd.read_sql_table()
       └─ DataFrame (one table at a time, in-memory)
            └─ lowercase all column names
                 └─ write_pandas(overwrite=True, auto_create_table=True)
                      └─ Snowflake basket_craft.raw  (SNOWFLAKE_* env vars)
```

Three responsibilities:

- `get_rds_engine()` — SQLAlchemy engine reading `RDS_*` from `.env`
- `get_snowflake_conn()` — `snowflake-connector-python` connection reading `SNOWFLAKE_*` from `.env`
- `load_table(table, rds_engine, sf_conn)` — read → lowercase columns → `write_pandas`

---

## Tables Loaded

All 8 raw Basket Craft tables (same set as `load_raw_tables.py`):

```python
TABLES = [
    "employees",
    "order_item_refunds",
    "order_items",
    "orders",
    "products",
    "users",
    "website_pageviews",
    "website_sessions",
]
```

---

## Load Strategy

**Full truncate-and-reload on every run.** `write_pandas` is called with:

- `overwrite=True` — truncates the target table before writing (idempotent re-runs)
- `auto_create_table=True` — creates the table on first run if it does not exist

This handles both first run (table does not exist) and subsequent runs (truncate then load) without a separate explicit truncate step.

---

## Identifier Casing

All table and column names are kept **lowercase and never quoted**. Column names are normalized via `df.columns = df.columns.str.lower()` before each `write_pandas` call. This is required for dbt compatibility — Snowflake uppercases unquoted identifiers by default, so quoting mixed-case names would break all downstream dbt model references.

---

## Chunking

None. All tables are read fully into memory. The Basket Craft dataset is small enough that this is safe and simpler than batching.

---

## Error Handling

No per-table try/except. If any table fails, the script crashes loudly with the full error. Silent partial loads are more dangerous than a loud crash at this scale. Each successful table prints `[table] X rows loaded` to stdout.

---

## Credentials

All credentials are read from `.env` via `python-dotenv`. No credentials are hardcoded.

| Variable | Purpose |
|---|---|
| `RDS_HOST`, `RDS_PORT`, `RDS_USER`, `RDS_PASSWORD`, `RDS_DATABASE` | Source RDS connection |
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` | Snowflake auth |
| `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA` | Snowflake session context |

---

## Dependencies

Add to `requirements.txt`:

- `snowflake-connector-python[pandas]` — includes `write_pandas` and Parquet serialization support

---

## Verification

After running, verify row counts in Snowsight match RDS:

```sql
USE DATABASE basket_craft;
USE SCHEMA raw;

SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM order_items;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM users;
-- repeat for all 8 tables
```

---

## Files Changed

| File | Change |
|---|---|
| `load_snowflake.py` | New — the loader script |
| `requirements.txt` | Add `snowflake-connector-python[pandas]` |
| `.env.example` | Add `SNOWFLAKE_*` variable stubs |
| `CLAUDE.md` | Document new script, credentials, and target |
