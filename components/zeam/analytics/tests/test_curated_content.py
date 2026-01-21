import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Ensure import path if needed, though Polylith usually handles this via creating a venv or similar. 
# For unit tests, we might need to be careful with paths if running standalone.
# Assuming standard pytest discovery.

from zeam.analytics.curated_content import get_curated_content_sql, get_results

@patch('zeam.analytics.curated_content.RedshiftConnection')
def test_get_results_sql_formatting(mock_conn_cls):
    """Test get_results formats query correctly."""
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn_cls.return_value = mock_conn
    mock_conn.execute_query.return_value = []

    get_results(
        start_date="2024-01-01 00:00:00",
        end_date="2024-01-07 23:59:59",
        dma_id=123,
        limit=5
    )
    
    executed_query = mock_conn.execute_query.call_args[0][0]
    assert "LIMIT 5" in executed_query
    assert "log.dmaid = 123" in executed_query
    assert "'2024-01-01 00:00:00'" in executed_query

@patch('zeam.analytics.curated_content.RedshiftConnection')
def test_get_results_without_dma(mock_conn_cls):
    """Test get_results without DMA."""
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn_cls.return_value = mock_conn
    mock_conn.execute_query.return_value = []

    get_results(
        start_date="2024-01-01 00:00:00",
        end_date="2024-01-07 23:59:59"
    )
    
    executed_query = mock_conn.execute_query.call_args[0][0]
    assert "media_channel.dma_id =" not in executed_query
    assert "log.dmaid =" not in executed_query

def test_get_curated_content_sql_basic():
    """Test the SQL generation helper directly."""
    sql = get_curated_content_sql("2024-01-01", "2024-01-02", 999, 20)
    assert "LIMIT 20" in sql
    assert "log.dmaid = 999" in sql
