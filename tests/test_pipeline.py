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
