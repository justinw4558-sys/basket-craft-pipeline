# RDS to Snowflake Loader — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `load_snowflake.py`, a standalone script that reads all 8 raw Basket Craft tables from AWS RDS PostgreSQL and writes them into Snowflake `basket_craft.raw`, truncating and reloading on every run.

**Architecture:** Mirror the shape of `load_raw_tables.py` — three functions per script: an RDS engine factory, a Snowflake connection factory, and a per-table load function. Each table is read fully into a pandas DataFrame, columns are lowercased, and the table is written to Snowflake using `write_pandas` with `overwrite=True` and `auto_create_table=True`.

**Tech Stack:** `snowflake-connector-python[pandas]` (for `write_pandas`), `sqlalchemy` + `psycopg2-binary` (for RDS reads via `pd.read_sql_table`), `python-dotenv` (credentials from `.env`)

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `load_snowflake.py` | Create | RDS → Snowflake loader script |
| `requirements.txt` | Modify | Add `sqlalchemy` and `snowflake-connector-python[pandas]` |
| `tests/test_load_snowflake.py` | Create | Unit test for column lowercasing |
| `.env.example` | Modify | Add `SNOWFLAKE_*` variable stubs |
| `CLAUDE.md` | Modify | Document new script, credentials, target |

---

## Task 1: Update requirements.txt

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add the two missing dependencies**

Open `requirements.txt` and add these two lines at the end:

```
sqlalchemy
snowflake-connector-python[pandas]
```

`sqlalchemy` is already used by `load_raw_tables.py` but was missing from `requirements.txt`. Both are needed here.

- [ ] **Step 2: Commit**

```bash
git add requirements.txt
git commit -m "chore: add sqlalchemy and snowflake-connector-python to requirements"
```

---

## Task 2: Write the failing unit test (TDD)

**Files:**
- Create: `tests/test_load_snowflake.py`

- [ ] **Step 1: Create the test file**

Create `tests/test_load_snowflake.py` with this content:

```python
import pandas as pd
from unittest.mock import MagicMock, patch


def test_load_table_lowercases_columns():
    """load_table() must lowercase all column names before writing to Snowflake."""
    df_mixed = pd.DataFrame(columns=["OrderID", "ProductName", "Price_USD"])

    mock_engine = MagicMock()
    mock_sf_conn = MagicMock()

    with patch("load_snowflake.pd.read_sql_table", return_value=df_mixed):
        with patch("load_snowflake.write_pandas", return_value=(True, 1, 0, None)) as mock_write:
            from load_snowflake import load_table
            load_table("orders", mock_engine, mock_sf_conn)

    called_df = mock_write.call_args[0][1]
    assert list(called_df.columns) == ["orderid", "productname", "price_usd"]
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
pytest tests/test_load_snowflake.py::test_load_table_lowercases_columns -v
```

Expected: `ModuleNotFoundError: No module named 'load_snowflake'`

- [ ] **Step 3: Commit the failing test**

```bash
git add tests/test_load_snowflake.py
git commit -m "test: add failing test for load_snowflake column lowercasing"
```

---

## Task 3: Implement load_snowflake.py

**Files:**
- Create: `load_snowflake.py`

- [ ] **Step 1: Create the script**

Create `load_snowflake.py` with this content:

```python
import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

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


def get_rds_engine():
    host = os.environ["RDS_HOST"]
    port = os.getenv("RDS_PORT", 5432)
    user = os.environ["RDS_USER"]
    password = os.environ["RDS_PASSWORD"]
    database = os.environ["RDS_DATABASE"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def load_table(table, rds_engine, sf_conn):
    print(f"[{table}] Reading from RDS...", flush=True)
    df = pd.read_sql_table(table, rds_engine)
    df.columns = df.columns.str.lower()
    print(f"[{table}] {len(df)} rows — loading into Snowflake...", flush=True)
    success, nchunks, nrows, _ = write_pandas(
        sf_conn,
        df,
        table_name=table,
        auto_create_table=True,
        overwrite=True,
    )
    print(f"[{table}] Done ({nrows} rows loaded).", flush=True)


if __name__ == "__main__":
    rds_engine = get_rds_engine()
    sf_conn = get_snowflake_conn()
    try:
        for table in TABLES:
            load_table(table, rds_engine, sf_conn)
        print("\nAll tables loaded.")
    finally:
        sf_conn.close()
```

- [ ] **Step 2: Run the unit test to verify it passes**

```bash
pytest tests/test_load_snowflake.py::test_load_table_lowercases_columns -v
```

Expected:
```
PASSED tests/test_load_snowflake.py::test_load_table_lowercases_columns
```

- [ ] **Step 3: Run the full test suite to confirm no regressions**

```bash
pytest tests/test_pipeline.py -v
```

Expected: all 6 existing tests pass (the 2 unit tests pass without Docker; the 4 integration tests require Docker to be running).

- [ ] **Step 4: Commit**

```bash
git add load_snowflake.py
git commit -m "feat: add load_snowflake.py — RDS to Snowflake loader"
```

---

## Task 4: Install new dependencies

**Files:** none (runtime environment only)

- [ ] **Step 1: Install from requirements.txt into the project virtual environment**

```bash
pip install -r requirements.txt
```

Expected: pip installs `sqlalchemy` and `snowflake-connector-python[pandas]` (and their transitive dependencies). No errors.

- [ ] **Step 2: Verify snowflake connector is importable**

```bash
python -c "import snowflake.connector; from snowflake.connector.pandas_tools import write_pandas; print('OK')"
```

Expected: `OK`

---

## Task 5: Update .env.example

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: Add Snowflake variable stubs**

Open `.env.example` and append these lines:

```
RDS_HOST=your_rds_host_here
RDS_PORT=5432
RDS_USER=your_rds_user_here
RDS_PASSWORD=your_rds_password_here
RDS_DATABASE=basket_craft

SNOWFLAKE_ACCOUNT=your_account_identifier_here
SNOWFLAKE_USER=your_snowflake_username_here
SNOWFLAKE_PASSWORD=your_snowflake_password_here
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=basket_craft_wh
SNOWFLAKE_DATABASE=basket_craft
SNOWFLAKE_SCHEMA=raw
```

> Note: `.env.example` currently has no `RDS_*` or `SNOWFLAKE_*` entries. Add both blocks so new contributors know all required variables.

- [ ] **Step 2: Commit**

```bash
git add .env.example
git commit -m "chore: add RDS and Snowflake env stubs to .env.example"
```

---

## Task 6: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add load_snowflake.py to the Commands section**

In `CLAUDE.md`, under the `## Commands` section, add:

```bash
# Load all raw tables from AWS RDS into Snowflake basket_craft.raw (one-time / refresh)
python load_snowflake.py
```

- [ ] **Step 2: Add load_snowflake.py to the Architecture section**

Under `## Architecture`, after the `load_raw_tables.py` entry, add:

```
**`load_snowflake.py`** — One-time bulk loader targeting Snowflake `basket_craft.raw`.
- Reads all 8 raw tables from AWS RDS PostgreSQL using SQLAlchemy + `pd.read_sql_table`
- Writes each table to Snowflake using `write_pandas` with `overwrite=True` (truncate-and-reload)
- All column names are lowercased before writing (required for dbt compatibility)
- Uses `SNOWFLAKE_*` env vars for credentials and `RDS_*` env vars for the source
```

- [ ] **Step 3: Add Snowflake to the Destination Tables section**

Under `## Destination Tables`, add:

```
Snowflake (`SNOWFLAKE_*`):
- Schema: `basket_craft.raw`
- All 8 source tables loaded raw with lowercase column names: `employees`, `order_item_refunds`, `order_items`, `orders`, `products`, `users`, `website_pageviews`, `website_sessions`
```

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md to document load_snowflake.py"
```

---

## Verification (manual, post-run)

After running `python load_snowflake.py`, verify row counts in Snowsight:

```sql
USE DATABASE basket_craft;
USE SCHEMA raw;

SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM order_items;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM employees;
SELECT COUNT(*) FROM order_item_refunds;
SELECT COUNT(*) FROM website_pageviews;
SELECT COUNT(*) FROM website_sessions;
```

Each count must match the row count in the corresponding RDS table.
