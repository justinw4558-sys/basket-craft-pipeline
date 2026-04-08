# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run the ELT pipeline (MySQL → local PostgreSQL)
python pipeline.py

# Load all raw MySQL tables into AWS RDS PostgreSQL (one-time / refresh)
python load_raw_tables.py

# Run tests
pytest tests/test_pipeline.py -v

# Run a single test
pytest tests/test_pipeline.py::test_extract_returns_dataframe_with_expected_columns -v

# Start local PostgreSQL (required for pipeline.py and integration tests)
docker compose up -d

# Stop local PostgreSQL
docker compose down
```

## Architecture

Two independent scripts:

**`pipeline.py`** — Monthly ELT pipeline targeting local Docker PostgreSQL.
- `extract(mysql_conn)` — JOINs `order_items` + `products` from MySQL, returns a DataFrame
- `load_staging(df, pg_conn)` — Truncates and reloads `stg_order_items` (raw staging table)
- `transform(pg_conn)` — Runs SQL inside PostgreSQL to build `mart_monthly_sales` (aggregated by month + product)
- `get_mysql_conn()` / `get_pg_conn()` — Connection factories reading from `.env`
- Full refresh on every run — both tables are truncated before writing

**`load_raw_tables.py`** — One-time bulk loader targeting AWS RDS PostgreSQL.
- Copies all 8 MySQL tables as-is using SQLAlchemy `to_sql` with `if_exists="replace"`
- Uses `RDS_*` env vars (separate from the local `POSTGRES_*` vars used by `pipeline.py`)

## Destination Tables

Local PostgreSQL (`POSTGRES_*`):
- `stg_order_items` — raw extract (staging layer)
- `mart_monthly_sales` — aggregated by `(month CHAR(7), product_name)` with `total_revenue`, `order_count`, `avg_order_value`

AWS RDS (`RDS_*`):
- All 8 source tables loaded raw: `employees`, `order_item_refunds`, `order_items`, `orders`, `products`, `users`, `website_pageviews`, `website_sessions`

## Environment

Copy `.env.example` to `.env` and fill in credentials. The `.env` file is gitignored.

Two sets of database credentials in `.env`:
- `MYSQL_*` — source MySQL at `db.isba.co` (read-only analyst account)
- `POSTGRES_*` — local Docker PostgreSQL (pipeline.py target)
- `RDS_*` — AWS RDS PostgreSQL in `us-east-2` (load_raw_tables.py target)

## Tests

Integration tests connect to the real local Docker PostgreSQL — Docker must be running. Each test explicitly drops and recreates its tables so tests can run in any order.

- 2 unit tests for `extract` (mocked, no DB needed)
- 4 integration tests for `load_staging` and `transform` (require Docker PostgreSQL)
