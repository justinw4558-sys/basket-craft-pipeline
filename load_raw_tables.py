import os
import pandas as pd
from sqlalchemy import create_engine, text
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


def get_mysql_engine():
    host = os.environ["MYSQL_HOST"]
    port = os.getenv("MYSQL_PORT", 3306)
    user = os.environ["MYSQL_USER"]
    password = os.environ["MYSQL_PASSWORD"]
    database = os.environ["MYSQL_DATABASE"]
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")


def get_rds_engine():
    host = os.environ["RDS_HOST"]
    port = os.getenv("RDS_PORT", 5432)
    user = os.environ["RDS_USER"]
    password = os.environ["RDS_PASSWORD"]
    database = os.environ["RDS_DATABASE"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")


def load_table(table, mysql_engine, rds_engine):
    print(f"[{table}] Extracting from MySQL...", flush=True)
    df = pd.read_sql_table(table, mysql_engine)
    print(f"[{table}] {len(df)} rows — loading into RDS...", flush=True)
    df.to_sql(table, rds_engine, if_exists="replace", index=False)
    print(f"[{table}] Done.", flush=True)


if __name__ == "__main__":
    mysql_engine = get_mysql_engine()
    rds_engine = get_rds_engine()
    for table in TABLES:
        load_table(table, mysql_engine, rds_engine)
    print("\nAll tables loaded.")
