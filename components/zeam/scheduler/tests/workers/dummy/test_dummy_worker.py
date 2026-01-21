"""
Tests for dummy worker.

Strategy:
- Use minimal synthetic data matching SQL structure
- Test content type grouping (channel, show, vod)
- Test DMA grouping vs global
- Mock database and Redis
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import json

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src"))

from zeam.scheduler.workers.dummy_worker import DummyWorker, dummy_popularity_ingestion


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
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
    Minimal synthetic data matching SQL structure.

    Schema: content_type, content_id, title, dma_id
    Edge cases:
    - Multiple content types (channel, show, vod)
    - With DMA (should group by DMA)
    - Without DMA (should go to global)
    """
    return [
        {"content_type": "channel", "content_id": 123, "title": "Channel 1", "dma_id": 501},
        {"content_type": "show", "content_id": 456, "title": "Show 1", "dma_id": 501},
        {"content_type": "vod", "content_id": 789, "title": "Movie 1", "dma_id": None},
    ]


@pytest.fixture
def edge_case_all_global():
    """Edge case: all items without DMA (global)."""
    return [
        {"content_type": "channel", "content_id": 111, "title": "Global Channel", "dma_id": None},
        {"content_type": "show", "content_id": 222, "title": "Global Show", "dma_id": None},
    ]


@pytest.fixture
def edge_case_single_dma():
    """Edge case: single DMA with multiple items."""
    return [
        {"content_type": "channel", "content_id": 100, "title": "NYC Channel", "dma_id": 501},
        {"content_type": "channel", "content_id": 101, "title": "NYC Channel 2", "dma_id": 501},
    ]


class TestDummyWorker:
    """Test suite for DummyWorker."""

    def test_worker_initialization(self):
        """Test worker initializes correctly."""
        worker = DummyWorker()
        assert worker.query_path.exists()
        assert worker.query_path.name == "dummy.sql"
        assert worker.redis_client is not None

    def test_process_with_minimal_data(self, mock_redis, minimal_db_results):
        """Test processing with minimal synthetic data."""
        worker = DummyWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.dummy_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=minimal_db_results)
            mock_conn.return_value = mock_conn_instance

            result = worker.process()

        # Verify results
        assert result["keys_updated"] == 2  # 1 DMA key + 1 global key
        assert result["total_rows"] == 3

        # Verify Redis keys
        assert "zeam-recommender:dummy:popularity:dma:501" in mock_redis.stored_data
        assert "zeam-recommender:dummy:popularity:global" in mock_redis.stored_data

    def test_dma_grouping(self, mock_redis, minimal_db_results):
        """Test that items are correctly grouped by DMA."""
        worker = DummyWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.dummy_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=minimal_db_results)
            mock_conn.return_value = mock_conn_instance

            worker.process()

        # Check DMA key
        stored_value, ttl = mock_redis.stored_data["zeam-recommender:dummy:popularity:dma:501"]
        assert ttl == 60 * 60 * 25  # 25 hours

        data = json.loads(stored_value)
        assert len(data) == 2  # 2 items with dma_id=501
        assert data[0]["type"] == "channel"
        assert data[1]["type"] == "show"

    def test_global_grouping(self, mock_redis, minimal_db_results):
        """Test that items without DMA go to global key."""
        worker = DummyWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.dummy_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=minimal_db_results)
            mock_conn.return_value = mock_conn_instance

            worker.process()

        # Check global key
        stored_value, ttl = mock_redis.stored_data["zeam-recommender:dummy:popularity:global"]
        data = json.loads(stored_value)
        assert len(data) == 1  # 1 item with dma_id=None
        assert data[0]["type"] == "vod"
        assert data[0]["id"] == "789"

    def test_edge_case_all_global(self, mock_redis, edge_case_all_global):
        """Test with all items being global (no DMA)."""
        worker = DummyWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.dummy_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=edge_case_all_global)
            mock_conn.return_value = mock_conn_instance

            result = worker.process()

        assert result["keys_updated"] == 1  # Only global key
        assert result["keys_updated"] == 1  # Only global key
        assert "zeam-recommender:dummy:popularity:global" in mock_redis.stored_data
        assert "zeam-recommender:dummy:popularity:dma:501" not in mock_redis.stored_data

    def test_edge_case_single_dma(self, mock_redis, edge_case_single_dma):
        """Test with multiple items for single DMA."""
        worker = DummyWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.dummy_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=edge_case_single_dma)
            mock_conn.return_value = mock_conn_instance

            result = worker.process()

        assert result["keys_updated"] == 1
        assert result["keys_updated"] == 1
        stored_value, _ = mock_redis.stored_data["zeam-recommender:dummy:popularity:dma:501"]
        data = json.loads(stored_value)
        assert len(data) == 2  # Both items in same DMA

    def test_edge_case_empty_results(self, mock_redis):
        """Test with no results from database."""
        worker = DummyWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.dummy_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=[])
            mock_conn.return_value = mock_conn_instance

            result = worker.process()

        assert result["keys_updated"] == 0
        assert result["total_rows"] == 0
        assert len(mock_redis.stored_data) == 0

    def test_content_item_structure(self, mock_redis, minimal_db_results):
        """Test that ContentItem structure is correct."""
        worker = DummyWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.dummy_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=minimal_db_results)
            mock_conn.return_value = mock_conn_instance

            worker.process()

            worker.process()
        
        stored_value, _ = mock_redis.stored_data["zeam-recommender:dummy:popularity:dma:501"]
        data = json.loads(stored_value)

        # Verify ContentItem structure
        item = data[0]
        assert "id" in item
        assert "title" in item
        assert "type" in item
        assert item["id"] == "123"
        assert item["title"] == "Channel 1"
        assert item["type"] == "channel"

    def test_database_error_handling(self, mock_redis):
        """Test database error handling."""
        worker = DummyWorker()
        worker.redis_client = mock_redis

        with patch('zeam.scheduler.workers.dummy_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(side_effect=Exception("DB Error"))
            mock_conn.return_value = mock_conn_instance

            with pytest.raises(Exception, match="DB Error"):
                worker.process()

    def test_redis_error_handling(self, mock_redis, minimal_db_results):
        """Test Redis error handling."""
        worker = DummyWorker()
        worker.redis_client = mock_redis

        def mock_store_error(*args, **kwargs):
            raise Exception("Redis Error")

        worker.store_in_redis = mock_store_error

        with patch('zeam.scheduler.workers.dummy_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=minimal_db_results)
            mock_conn.return_value = mock_conn_instance

            with pytest.raises(Exception, match="Redis Error"):
                worker.process()
