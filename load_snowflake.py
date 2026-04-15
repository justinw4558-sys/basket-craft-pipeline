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
        quote_identifiers=False,
    )
    if not success:
        raise RuntimeError(f"[{table}] write_pandas reported failure")
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
