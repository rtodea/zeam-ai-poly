"""
Tests for channels_by_dma worker.

Strategy:
- Use pytest fixtures to create minimal synthetic data
- Mock database connection to return controlled test data
- Verify Redis storage and data structure
- Test edge cases with minimal data rows
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Add scheduler to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from zeam.scheduler.workers.channels_by_dma_worker import ChannelsByDMAWorker, channels_by_dma_ingestion


@pytest.fixture
def mock_redis():
    """Mock Redis client with tracking."""
    redis_mock = Mock()
    redis_mock.stored_data = {}

    def mock_set(key, value):
        redis_mock.stored_data[key] = value

    def mock_setex(key, ttl, value):
        redis_mock.stored_data[key] = (value, ttl)

    redis_mock.set = mock_set
    redis_mock.setex = mock_setex
    return redis_mock


@pytest.fixture
def minimal_db_results():
    """
    Minimal synthetic data matching the SQL structure.

    Schema: dma_name, dma_id, channelCount
    Edge cases covered:
    - Single DMA with channels
    - Multiple DMAs
    - Different channel counts
    """
    return [
        {"dma_name": "New York", "dma_id": 501, "channelcount": 10},
        {"dma_name": "Los Angeles", "dma_id": 803, "channelcount": 8},
        {"dma_name": "Chicago", "dma_id": 602, "channelcount": 5},
    ]


@pytest.fixture
def edge_case_db_results():
    """Edge cases: single result, zero channels."""
    return [
        {"dma_name": "Test DMA", "dma_id": 999, "channelcount": 0},
    ]


class TestChannelsByDMAWorker:
    """Test suite for ChannelsByDMAWorker."""

    def test_worker_initialization(self):
        """Test worker initializes correctly."""
        worker = ChannelsByDMAWorker()
        assert worker.query_path.exists()
        assert worker.query_path.name == "channels_by_dma.sql"
        assert worker.redis_client is not None

    def test_process_with_minimal_data(self, mock_redis, minimal_db_results):
        """Test processing with minimal synthetic data."""
        worker = ChannelsByDMAWorker()
        worker.redis_client = mock_redis

        # Mock database connection
        with patch('zeam.scheduler.workers.channels_by_dma_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=minimal_db_results)
            mock_conn.return_value = mock_conn_instance

            result = worker.process()

        # Verify results
        assert result["dma_count"] == 3
        assert result["total_rows"] == 3

        # Verify Redis storage
        assert len(mock_redis.stored_data) == 3
        assert "channels:dma:501" in mock_redis.stored_data
        assert "channels:dma:803" in mock_redis.stored_data
        assert "channels:dma:602" in mock_redis.stored_data

    def test_redis_data_structure(self, mock_redis, minimal_db_results):
        """Test that Redis data has correct structure."""
        worker = ChannelsByDMAWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.channels_by_dma_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=minimal_db_results)
            mock_conn.return_value = mock_conn_instance

            worker.process()

        # Check first DMA data structure
        stored_value, ttl = mock_redis.stored_data["channels:dma:501"]
        assert ttl == 60 * 60 * 25  # 25 hours

        import json
        data = json.loads(stored_value)
        assert data["dma_name"] == "New York"
        assert data["dma_id"] == 501
        assert data["channelcount"] == 10

    def test_edge_case_single_result(self, mock_redis, edge_case_db_results):
        """Test with single result (edge case)."""
        worker = ChannelsByDMAWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.channels_by_dma_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=edge_case_db_results)
            mock_conn.return_value = mock_conn_instance

            result = worker.process()

        assert result["dma_count"] == 1
        assert result["total_rows"] == 1
        assert "channels:dma:999" in mock_redis.stored_data

    def test_edge_case_empty_results(self, mock_redis):
        """Test with no results from database."""
        worker = ChannelsByDMAWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.channels_by_dma_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=[])
            mock_conn.return_value = mock_conn_instance

            result = worker.process()

        assert result["dma_count"] == 0
        assert result["total_rows"] == 0
        assert len(mock_redis.stored_data) == 0

    def test_database_error_handling(self, mock_redis):
        """Test that database errors are properly raised."""
        worker = ChannelsByDMAWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.channels_by_dma_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(side_effect=Exception("DB Connection Failed"))
            mock_conn.return_value = mock_conn_instance

            with pytest.raises(Exception, match="DB Connection Failed"):
                worker.process()

    def test_redis_error_handling(self, mock_redis, minimal_db_results):
        """Test that Redis errors are properly raised."""
        worker = ChannelsByDMAWorker()
        worker.redis_client = mock_redis

        # Make Redis raise an error
        def mock_store_error(*args, **kwargs):
            raise Exception("Redis Connection Failed")

        worker.store_in_redis = mock_store_error

        with patch('zeam.scheduler.workers.channels_by_dma_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=minimal_db_results)
            mock_conn.return_value = mock_conn_instance

            with pytest.raises(Exception, match="Redis Connection Failed"):
                worker.process()

    def test_sql_query_loads(self):
        """Test that SQL query file loads correctly."""
        worker = ChannelsByDMAWorker()
        sql_content = worker.query_path.read_text()

        # Verify SQL has expected structure
        assert "dma.dma_name" in sql_content
        assert "dma.dma_id" in sql_content
        assert "count(*)" in sql_content
        assert "group by" in sql_content.lower()
        assert "media_channel" in sql_content
