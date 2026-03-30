# Basket Craft ELT Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-script ELT pipeline that extracts Basket Craft sales data from MySQL, loads it into a PostgreSQL staging table, then transforms it into a monthly sales summary using SQL.

**Architecture:** `pipeline.py` exposes three functions (`extract`, `load_staging`, `transform`) that accept connections as parameters for testability. A `__main__` block creates connections from `.env` and calls them in sequence. All transformation logic lives as SQL executed inside PostgreSQL — no pandas aggregation.

**Tech Stack:** Python 3, pymysql, pandas, psycopg2-binary, python-dotenv, pytest, Docker (PostgreSQL)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `requirements.txt` | Create | Python dependencies |
| `docker-compose.yml` | Create | Local PostgreSQL container |
| `.env` | Modify | Add PostgreSQL credentials |
| `pipeline.py` | Create | ELT pipeline (extract, load_staging, transform, __main__) |
| `tests/test_pipeline.py` | Create | Unit + integration tests |

---

## Task 1: Project scaffolding

**Files:**
- Create: `requirements.txt`
- Create: `docker-compose.yml`
- Modify: `.env`

- [ ] **Step 1: Create `requirements.txt`**

```
pymysql==1.1.1
pandas==2.2.2
psycopg2-binary==2.9.9
python-dotenv==1.0.1
pytest==8.2.0
```

- [ ] **Step 2: Create `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: pipeline
      POSTGRES_PASSWORD: pipeline
      POSTGRES_DB: basket_craft
    ports:
      - "5432:5432"
```

- [ ] **Step 3: Add PostgreSQL credentials to `.env`**

Append to the existing `.env` (which already has MySQL credentials):

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=pipeline
POSTGRES_PASSWORD=pipeline
POSTGRES_DATABASE=basket_craft
```

- [ ] **Step 4: Install dependencies**

```bash
pip install -r requirements.txt
```

Expected: all packages install without errors.

- [ ] **Step 5: Start PostgreSQL container**

```bash
docker compose up -d
```

Expected output:
```
✔ Container basket-craft-pipeline-postgres-1  Started
```

Verify it's healthy:
```bash
docker compose ps
```
Expected: `postgres` service shows `running`.

- [ ] **Step 6: Commit**

```bash
git add requirements.txt docker-compose.yml .env
git commit -m "feat: add project scaffolding and Docker PostgreSQL"
```

---

## Task 2: Extract function

**Files:**
- Create: `pipeline.py` (extract function only)
- Create: `tests/test_pipeline.py` (extract tests)

- [ ] **Step 1: Create `tests/test_pipeline.py` with a failing test for extract**

```python
import pandas as pd
from unittest.mock import MagicMock, patch
import pytest


def test_extract_returns_dataframe_with_expected_columns():
    """extract() should return a DataFrame with exactly these columns."""
    mock_conn = MagicMock()

    expected_columns = [
        "order_item_id", "order_id", "product_id",
        "product_name", "created_at", "price_usd"
    ]
    fake_df = pd.DataFrame(columns=expected_columns)

    with patch("pandas.read_sql", return_value=fake_df) as mock_read_sql:
        from pipeline import extract
        result = extract(mock_conn)

    assert list(result.columns) == expected_columns
    mock_read_sql.assert_called_once()


def test_extract_query_joins_order_items_and_products():
    """extract() SQL must JOIN order_items and products."""
    mock_conn = MagicMock()
    fake_df = pd.DataFrame()

    with patch("pandas.read_sql", return_value=fake_df) as mock_read_sql:
        from pipeline import extract
        extract(mock_conn)

    sql_called = mock_read_sql.call_args[0][0]
    assert "order_items" in sql_called
    assert "products" in sql_called
    assert "JOIN" in sql_called.upper()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_pipeline.py -v
```

Expected: `ImportError: No module named 'pipeline'` or `ModuleNotFoundError`.

- [ ] **Step 3: Create `pipeline.py` with the extract function**

```python
import os
import pymysql
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def extract(mysql_conn) -> pd.DataFrame:
    query = """
        SELECT
            oi.order_item_id,
            oi.order_id,
            oi.product_id,
            p.product_name,
            oi.created_at,
            oi.price_usd
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
    """
    return pd.read_sql(query, mysql_conn)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_pipeline.py -v
```

Expected:
```
PASSED tests/test_pipeline.py::test_extract_returns_dataframe_with_expected_columns
PASSED tests/test_pipeline.py::test_extract_query_joins_order_items_and_products
```

- [ ] **Step 5: Commit**

```bash
git add pipeline.py tests/test_pipeline.py
git commit -m "feat: add extract function with tests"
```

---

## Task 3: Load staging function

**Files:**
- Modify: `pipeline.py` (add load_staging function)
- Modify: `tests/test_pipeline.py` (add load_staging tests)

- [ ] **Step 1: Add failing tests for load_staging**

Append to `tests/test_pipeline.py`:

```python
import psycopg2


def get_test_pg_conn():
    """Returns a real connection to the Docker PostgreSQL for integration tests."""
    return psycopg2.connect(
        host="localhost",
        port=5432,
        user="pipeline",
        password="pipeline",
        dbname="basket_craft"
    )


def test_load_staging_creates_table_and_inserts_rows():
    """load_staging() must create stg_order_items and insert all DataFrame rows."""
    import datetime
    from pipeline import load_staging

    conn = get_test_pg_conn()

    # Drop table if it exists from a prior test run
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg_order_items")
    conn.commit()

    sample_df = pd.DataFrame([
        {
            "order_item_id": 1,
            "order_id": 100,
            "product_id": 1,
            "product_name": "The Original Gift Basket",
            "created_at": datetime.datetime(2024, 1, 15, 10, 0, 0),
            "price_usd": 49.99,
        },
        {
            "order_item_id": 2,
            "order_id": 101,
            "product_id": 2,
            "product_name": "The Valentine's Gift Basket",
            "created_at": datetime.datetime(2024, 2, 10, 9, 0, 0),
            "price_usd": 59.99,
        },
    ])

    load_staging(sample_df, conn)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stg_order_items")
        count = cur.fetchone()[0]

    conn.close()
    assert count == 2


def test_load_staging_truncates_before_insert():
    """Calling load_staging() twice should not double the rows."""
    import datetime
    from pipeline import load_staging

    conn = get_test_pg_conn()

    sample_df = pd.DataFrame([
        {
            "order_item_id": 1,
            "order_id": 100,
            "product_id": 1,
            "product_name": "The Original Gift Basket",
            "created_at": datetime.datetime(2024, 1, 15, 10, 0, 0),
            "price_usd": 49.99,
        }
    ])

    load_staging(sample_df, conn)
    load_staging(sample_df, conn)  # second call should truncate first

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stg_order_items")
        count = cur.fetchone()[0]

    conn.close()
    assert count == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_pipeline.py::test_load_staging_creates_table_and_inserts_rows -v
```

Expected: `ImportError` or `cannot import name 'load_staging'`.

- [ ] **Step 3: Add load_staging to `pipeline.py`**

Append after the `extract` function:

```python
def load_staging(df: pd.DataFrame, pg_conn) -> None:
    with pg_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stg_order_items (
                order_item_id INTEGER,
                order_id      INTEGER,
                product_id    INTEGER,
                product_name  VARCHAR(50),
                created_at    TIMESTAMP,
                price_usd     NUMERIC(6,2)
            )
        """)
        cur.execute("TRUNCATE TABLE stg_order_items")
        rows = list(df.itertuples(index=False, name=None))
        cur.executemany(
            "INSERT INTO stg_order_items VALUES (%s, %s, %s, %s, %s, %s)",
            rows
        )
    pg_conn.commit()
    print(f"[load]    Loaded {len(df)} rows into stg_order_items")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_pipeline.py -v
```

Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add pipeline.py tests/test_pipeline.py
git commit -m "feat: add load_staging function with tests"
```

---

## Task 4: Transform function

**Files:**
- Modify: `pipeline.py` (add transform function)
- Modify: `tests/test_pipeline.py` (add transform tests)

- [ ] **Step 1: Add failing tests for transform**

Append to `tests/test_pipeline.py`:

```python
def test_transform_creates_mart_with_correct_aggregations():
    """transform() must produce correct revenue, order_count, and avg_order_value."""
    import datetime
    from pipeline import load_staging, transform

    conn = get_test_pg_conn()

    # Seed staging with known data:
    # Jan 2024: 2 order_items from 2 separate orders, $50 + $50 = $100 revenue, 2 orders, AOV $50
    # Feb 2024: 1 order_item, $60, 1 order, AOV $60
    seed_df = pd.DataFrame([
        {
            "order_item_id": 1,
            "order_id": 1,
            "product_id": 1,
            "product_name": "The Original Gift Basket",
            "created_at": datetime.datetime(2024, 1, 5),
            "price_usd": 50.00,
        },
        {
            "order_item_id": 2,
            "order_id": 2,
            "product_id": 1,
            "product_name": "The Original Gift Basket",
            "created_at": datetime.datetime(2024, 1, 20),
            "price_usd": 50.00,
        },
        {
            "order_item_id": 3,
            "order_id": 3,
            "product_id": 2,
            "product_name": "The Valentine's Gift Basket",
            "created_at": datetime.datetime(2024, 2, 10),
            "price_usd": 60.00,
        },
    ])
    load_staging(seed_df, conn)
    transform(conn)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT month, product_name, total_revenue, order_count, avg_order_value
            FROM mart_monthly_sales
            ORDER BY month, product_name
        """)
        rows = cur.fetchall()

    conn.close()

    assert len(rows) == 2

    jan_row = rows[0]
    assert jan_row[0] == "2024-01"
    assert jan_row[1] == "The Original Gift Basket"
    assert float(jan_row[2]) == 100.00
    assert jan_row[3] == 2
    assert float(jan_row[4]) == 50.00

    feb_row = rows[1]
    assert feb_row[0] == "2024-02"
    assert feb_row[1] == "The Valentine's Gift Basket"
    assert float(feb_row[2]) == 60.00
    assert feb_row[3] == 1
    assert float(feb_row[4]) == 60.00


def test_transform_truncates_before_rebuild():
    """Calling transform() twice should not duplicate mart rows."""
    import datetime
    from pipeline import load_staging, transform

    conn = get_test_pg_conn()

    seed_df = pd.DataFrame([
        {
            "order_item_id": 1,
            "order_id": 1,
            "product_id": 1,
            "product_name": "The Original Gift Basket",
            "created_at": datetime.datetime(2024, 3, 1),
            "price_usd": 49.99,
        }
    ])
    load_staging(seed_df, conn)
    transform(conn)
    transform(conn)  # second call should truncate first

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM mart_monthly_sales")
        count = cur.fetchone()[0]

    conn.close()
    assert count == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_pipeline.py::test_transform_creates_mart_with_correct_aggregations -v
```

Expected: `cannot import name 'transform'`.

- [ ] **Step 3: Add transform to `pipeline.py`**

Append after the `load_staging` function:

```python
def transform(pg_conn) -> None:
    with pg_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_monthly_sales (
                month           CHAR(7),
                product_name    VARCHAR(50),
                total_revenue   NUMERIC(10,2),
                order_count     INTEGER,
                avg_order_value NUMERIC(10,2),
                PRIMARY KEY (month, product_name)
            )
        """)
        cur.execute("TRUNCATE TABLE mart_monthly_sales")
        cur.execute("""
            INSERT INTO mart_monthly_sales
                (month, product_name, total_revenue, order_count, avg_order_value)
            SELECT
                TO_CHAR(created_at, 'YYYY-MM')                                   AS month,
                product_name,
                SUM(price_usd)                                                   AS total_revenue,
                COUNT(DISTINCT order_id)                                         AS order_count,
                ROUND(SUM(price_usd) / NULLIF(COUNT(DISTINCT order_id), 0), 2)  AS avg_order_value
            FROM stg_order_items
            GROUP BY TO_CHAR(created_at, 'YYYY-MM'), product_name
            ORDER BY TO_CHAR(created_at, 'YYYY-MM'), product_name
        """)
        cur.execute("SELECT COUNT(*) FROM mart_monthly_sales")
        count = cur.fetchone()[0]
    pg_conn.commit()
    print(f"[transform] Built mart_monthly_sales ({count} rows)")
```

- [ ] **Step 4: Run all tests to verify they pass**

```bash
pytest tests/test_pipeline.py -v
```

Expected: all 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add pipeline.py tests/test_pipeline.py
git commit -m "feat: add transform function with tests"
```

---

## Task 5: Wire up `__main__` and connection helpers

**Files:**
- Modify: `pipeline.py` (add get_mysql_conn, get_pg_conn, __main__ block)

- [ ] **Step 1: Append connection helpers and `__main__` to `pipeline.py`**

```python
def get_mysql_conn():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DATABASE"),
    )


if __name__ == "__main__":
    mysql_conn = get_mysql_conn()
    pg_conn = get_pg_conn()
    try:
        df = extract(mysql_conn)
        print(f"[extract] Fetched {len(df)} rows from MySQL")
        load_staging(df, pg_conn)
        transform(pg_conn)
        print("Done.")
    finally:
        mysql_conn.close()
        pg_conn.close()
```

- [ ] **Step 2: Run the full pipeline end-to-end**

```bash
python pipeline.py
```

Expected output:
```
[extract] Fetched 40025 rows from MySQL
[load]    Loaded 40025 rows into stg_order_items
[transform] Built mart_monthly_sales (X rows)
Done.
```

- [ ] **Step 3: Verify the mart table in PostgreSQL**

```bash
docker compose exec postgres psql -U pipeline -d basket_craft -c \
  "SELECT * FROM mart_monthly_sales ORDER BY month, product_name LIMIT 10;"
```

Expected: rows with `month`, `product_name`, `total_revenue`, `order_count`, `avg_order_value` columns populated.

- [ ] **Step 4: Run full test suite one final time**

```bash
pytest tests/test_pipeline.py -v
```

Expected: all 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add pipeline.py
git commit -m "feat: wire up __main__ with connection helpers — pipeline complete"
```
