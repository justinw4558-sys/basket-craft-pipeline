import pandas as pd
from unittest.mock import MagicMock, patch
import load_snowflake


def test_load_table_lowercases_columns():
    """load_table() must lowercase all column names before writing to Snowflake."""
    df_mixed = pd.DataFrame(columns=["OrderID", "ProductName", "Price_USD"])

    mock_engine = MagicMock()
    mock_sf_conn = MagicMock()

    with patch("load_snowflake.pd.read_sql_table", return_value=df_mixed), \
         patch("load_snowflake.write_pandas", return_value=(True, 1, 0, None)) as mock_write:
        load_snowflake.load_table("orders", mock_engine, mock_sf_conn)

    called_df = mock_write.call_args[0][1]
    assert list(called_df.columns) == ["orderid", "productname", "price_usd"]
