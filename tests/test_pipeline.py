import pandas as pd
import psycopg2
from unittest.mock import MagicMock, patch


def test_extract_returns_dataframe_with_expected_columns():
    """extract() should return a DataFrame with exactly these columns."""
    mock_conn = MagicMock()

    expected_columns = [
        "order_item_id", "order_id", "product_id",
        "product_name", "created_at", "price_usd"
    ]
    fake_df = pd.DataFrame(columns=expected_columns)

    with patch("pipeline.pd.read_sql", return_value=fake_df) as mock_read_sql:
        from pipeline import extract
        result = extract(mock_conn)

    assert list(result.columns) == expected_columns
    mock_read_sql.assert_called_once()


def test_extract_query_joins_order_items_and_products():
    """extract() SQL must JOIN order_items and products."""
    mock_conn = MagicMock()
    fake_df = pd.DataFrame()

    with patch("pipeline.pd.read_sql", return_value=fake_df) as mock_read_sql:
        from pipeline import extract
        extract(mock_conn)

    sql_called = mock_read_sql.call_args[0][0]
    assert "order_items" in sql_called
    assert "products" in sql_called
    assert "JOIN" in sql_called.upper()


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
        }
    ])

    load_staging(sample_df, conn)
    load_staging(sample_df, conn)  # second call should truncate first

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stg_order_items")
        count = cur.fetchone()[0]

    conn.close()
    assert count == 1


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
