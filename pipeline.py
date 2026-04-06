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
        """)
        cur.execute("SELECT COUNT(*) FROM mart_monthly_sales")
        count = cur.fetchone()[0]
    pg_conn.commit()
    print(f"[transform] Built mart_monthly_sales ({count} rows)")


def get_mysql_conn():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
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
