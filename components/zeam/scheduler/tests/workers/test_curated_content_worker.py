import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import json
import sys

# Ensure import path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from zeam.scheduler.workers.curated_content_worker import CuratedContentWorker

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

class TestCuratedContentWorker:
    """Test suite for CuratedContentWorker."""

    def test_worker_initialization(self):
        """Test worker initializes correctly."""
        worker = CuratedContentWorker()
        assert worker.query_path.exists()
    
    def test_process_parameters_formatting(self, mock_redis):
        """Test process method formats query correctly with DMA and Limit."""
        worker = CuratedContentWorker()
        worker.redis_client = mock_redis
        
        with patch('zeam.scheduler.workers.curated_content_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            
            # Capture the query executed
            mock_conn_instance.execute_query = Mock(return_value=[{"title": "Show 1", "viewers": 10}])
            mock_conn.return_value = mock_conn_instance
            
            result = worker.process(
                start_date="2024-01-01 00:00:00",
                end_date="2024-01-07 23:59:59",
                dma_id=123,
                item_count=5
            )
            
            # Verify call args
            executed_query = mock_conn_instance.execute_query.call_args[0][0]
            assert "LIMIT 5" in executed_query
            assert "media_channel.dma_id = 123" in executed_query
            assert "INNER JOIN prod.media_channel" in executed_query
            assert "'2024-01-01 00:00:00'" in executed_query
            
            # Verify Redis key
            redis_key = result["redis_key"]
            assert redis_key == "popularity:curated:2024-01-01:2024-01-07:123"
            
            assert f"zeam-recommender:{redis_key}" in mock_redis.stored_data
            
    def test_process_without_dma(self, mock_redis):
        """Test process method without DMA parameter."""
        worker = CuratedContentWorker()
        worker.redis_client = mock_redis
        
        with patch('zeam.scheduler.workers.curated_content_worker.RedshiftConnection') as mock_conn:
            mock_conn_instance = MagicMock()
            mock_conn_instance.__enter__ = Mock(return_value=mock_conn_instance)
            mock_conn_instance.__exit__ = Mock(return_value=None)
            mock_conn_instance.execute_query = Mock(return_value=[])
            mock_conn.return_value = mock_conn_instance
            
            result = worker.process(
                start_date="2024-01-01 00:00:00",
                end_date="2024-01-07 23:59:59"
            )
            
            executed_query = mock_conn_instance.execute_query.call_args[0][0]
            # Should NOT have the dma filter
            assert "media_channel.dma_id =" not in executed_query
            assert "INNER JOIN prod.media_channel" not in executed_query
            
            # Redis key should have 'global' suffix
            redis_key = result["redis_key"]
            assert redis_key == "popularity:curated:2024-01-01:2024-01-07:global"
            assert f"zeam-recommender:{redis_key}" not in mock_redis.stored_data # Should be empty because no rows returned
