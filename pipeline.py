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
