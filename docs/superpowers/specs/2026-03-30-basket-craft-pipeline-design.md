# Basket Craft ELT Pipeline — Design Spec

**Date:** 2026-03-30
**Status:** Approved

---

## Overview

A single-script ELT pipeline that extracts sales data from the Basket Craft MySQL database, loads raw rows into a PostgreSQL staging table, then transforms them into a monthly sales summary using SQL executed inside PostgreSQL.

**Out of scope:** Any dashboard, visualization, or reporting tool. The pipeline produces data; consumption is a separate concern.

---

## Source

**Database:** MySQL at `db.isba.co:3306`, database `basket_craft`

**Tables used:**

| Table | Key columns |
|---|---|
| `order_items` | `order_item_id`, `order_id`, `product_id`, `created_at`, `price_usd` |
| `products` | `product_id`, `product_name` |

**Extract query:** A single JOIN of `order_items` and `products` returning all columns needed for the staging table. No filtering — full extract on every run.

**Products (categories):**
1. The Original Gift Basket
2. The Valentine's Gift Basket
3. The Birthday Gift Basket
4. The Holiday Gift Basket

Data volume: ~32K orders, ~40K order items, spanning 2023–2026.

---

## Destination

**Database:** PostgreSQL running in Docker on `localhost:5432`

### Table: `stg_order_items`

Raw extracted rows. Truncated and fully replaced on each pipeline run.

| Column | Type | Source |
|---|---|---|
| `order_item_id` | INTEGER | `order_items.order_item_id` |
| `order_id` | INTEGER | `order_items.order_id` |
| `product_id` | INTEGER | `order_items.product_id` |
| `product_name` | VARCHAR(50) | `products.product_name` |
| `created_at` | TIMESTAMP | `order_items.created_at` |
| `price_usd` | NUMERIC(6,2) | `order_items.price_usd` |

### Table: `mart_monthly_sales`

Aggregated monthly summary by product. Truncated and rebuilt from `stg_order_items` on each pipeline run.

| Column | Type | Description |
|---|---|---|
| `month` | CHAR(7) | `YYYY-MM` format, derived from `created_at` |
| `product_name` | VARCHAR(50) | Product name (serves as category) |
| `total_revenue` | NUMERIC(10,2) | `SUM(price_usd)` |
| `order_count` | INTEGER | `COUNT(DISTINCT order_id)` |
| `avg_order_value` | NUMERIC(10,2) | `total_revenue / order_count` |

Primary key: `(month, product_name)`

---

## Pipeline

### File: `pipeline.py`

Single executable script with three functions called in sequence from `__main__`.

```
extract()         → pandas DataFrame (raw joined rows from MySQL)
load_staging(df)  → truncates stg_order_items, inserts df rows
transform()       → executes SQL inside PostgreSQL: stg_ → mart_
```

### Phase 1 — Extract

- Connect to MySQL using credentials from `.env`
- Execute JOIN query: `order_items` × `products`
- Return result as a pandas DataFrame

### Phase 2 — Load

- Connect to PostgreSQL using credentials from `.env`
- Ensure `stg_order_items` table exists (CREATE TABLE IF NOT EXISTS)
- Truncate `stg_order_items`
- Bulk insert DataFrame rows

### Phase 3 — Transform (SQL in PostgreSQL)

- Ensure `mart_monthly_sales` table exists
- Truncate `mart_monthly_sales`
- Execute aggregation SQL:

```sql
INSERT INTO mart_monthly_sales (month, product_name, total_revenue, order_count, avg_order_value)
SELECT
    TO_CHAR(created_at, 'YYYY-MM')   AS month,
    product_name,
    SUM(price_usd)                   AS total_revenue,
    COUNT(DISTINCT order_id)         AS order_count,
    ROUND(SUM(price_usd) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS avg_order_value
FROM stg_order_items
GROUP BY TO_CHAR(created_at, 'YYYY-MM'), product_name
ORDER BY TO_CHAR(created_at, 'YYYY-MM'), product_name;
```

### Refresh strategy

Full refresh on every run: both tables are truncated before writing. No incremental logic. Appropriate for the dataset size (~40K rows).

---

## Configuration

All credentials stored in `.env` (already in `.gitignore`):

```
MYSQL_HOST=db.isba.co
MYSQL_PORT=3306
MYSQL_USER=analyst
MYSQL_PASSWORD=...
MYSQL_DATABASE=basket_craft

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=...
POSTGRES_PASSWORD=...
POSTGRES_DATABASE=...
```

`pipeline.py` reads `.env` via `python-dotenv`.

---

## Dependencies

| Package | Purpose |
|---|---|
| `pymysql` | MySQL connection |
| `pandas` | DataFrame for extract result |
| `psycopg2-binary` | PostgreSQL connection |
| `python-dotenv` | Load `.env` credentials |

No ORM, no heavy framework. Standard library handles everything else.

---

## Error handling

- Connection failures surface as exceptions (no silent swallowing)
- No retries — if the pipeline fails, re-run manually
- Both staging and mart truncations happen inside the same run; a mid-run failure leaves staging populated but mart empty (acceptable for a full-refresh pattern)

---

## Running the pipeline

```bash
python pipeline.py
```

Expected console output:
```
[extract] Fetched 40025 rows from MySQL
[load]    Loaded 40025 rows into stg_order_items
[transform] Built mart_monthly_sales (X rows)
Done.
```
