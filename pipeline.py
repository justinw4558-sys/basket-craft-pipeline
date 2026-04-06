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
