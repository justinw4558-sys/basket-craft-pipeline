# Basket Craft Pipeline

Data pipeline for the Basket Craft e-commerce dataset.

## Databases

| Database | Type | Host | Purpose |
|---|---|---|---|
| `basket_craft` | MySQL | `db.isba.co:3306` | Source (read-only) |
| `basket_craft` | PostgreSQL | `localhost:5432` (Docker) | ELT pipeline output |
| `basket_craft` | PostgreSQL | AWS RDS `us-east-2` | Raw tables mirror |

## Scripts

### `pipeline.py` — Monthly ELT

Extracts sales data from MySQL, loads raw rows into local PostgreSQL, then transforms them into a monthly sales summary using SQL.

```bash
docker compose up -d   # start local PostgreSQL
python pipeline.py
```

Output tables (local PostgreSQL):
- `stg_order_items` — raw staging layer
- `mart_monthly_sales` — revenue, order count, and average order value by product and month

### `load_raw_tables.py` — AWS RDS Raw Load

Copies all 8 source tables from MySQL into AWS RDS PostgreSQL as-is (full refresh).

```bash
python load_raw_tables.py
```

Tables loaded into RDS:

| Table | Rows |
|---|---|
| employees | 20 |
| order_item_refunds | 1,731 |
| order_items | 40,025 |
| orders | 32,313 |
| products | 4 |
| users | 31,696 |
| website_pageviews | 1,188,124 |
| website_sessions | 472,871 |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in credentials
docker compose up -d   # for local PostgreSQL
```

## Tests

```bash
pytest tests/test_pipeline.py -v
```

Requires local Docker PostgreSQL running. Integration tests drop and recreate tables so they can run in any order.
