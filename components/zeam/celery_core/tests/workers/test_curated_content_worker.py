import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import json
import sys

# Ensure import path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from zeam.celery_core.workers.curated_content_worker import curated_content_popularity

@patch('zeam.celery_core.workers.curated_content_worker.get_results')
@patch('zeam.celery_core.workers.curated_content_worker.get_redis_client')
def test_worker_process(mock_get_redis, mock_get_results):
    """Test worker process calls get_results and saves to redis."""
    
    # Mock Redis
    redis_mock = Mock()
    mock_get_redis.return_value = redis_mock
    
    # Mock Results
    mock_get_results.return_value = [{"title": "Show 1", "viewers": 10}]
    
    # Mock task context (self.request.id)
    with patch('celery.app.task.Task.request') as mock_request:
        mock_request.id = "test-run-id"
        
        # Execute
        result = curated_content_popularity(
            start_date="2024-01-01 00:00:00",
            end_date="2024-01-07 23:59:59",
            dma_id=123,
            item_count=5
        )
        
        # Verify get_results called
        mock_get_results.assert_called_once_with(
            "2024-01-01 00:00:00",
            "2024-01-07 23:59:59",
            123,
            5
        )
        
        # Verify Redis setex called
        expected_key = "zeam-recommender:popularity:curated:2024-01-01:2024-01-07:123"
        redis_mock.setex.assert_called_once()
        args = redis_mock.setex.call_args[0]
        assert args[0] == expected_key
        assert args[1] == 60 * 60 * 24 * 8 # TTL
        assert json.loads(args[2]) == [{"title": "Show 1", "viewers": 10}]

@patch('zeam.celery_core.workers.curated_content_worker.get_results')
@patch('zeam.celery_core.workers.curated_content_worker.get_redis_client')
def test_worker_no_results(mock_get_redis, mock_get_results):
    """Test worker does not save to redis if no results."""
     # Mock Redis
    redis_mock = Mock()
    mock_get_redis.return_value = redis_mock
    
    # Mock Results
    mock_get_results.return_value = []
    
    with patch('celery.app.task.Task.request') as mock_request:
        mock_request.id = "test-run-id"
        
        result = curated_content_popularity(
            start_date="2024-01-01 00:00:00",
            end_date="2024-01-07 23:59:59"
        )
        
        mock_get_results.assert_called_once()
        redis_mock.setex.assert_not_called()
        redis_mock.set.assert_not_called()
