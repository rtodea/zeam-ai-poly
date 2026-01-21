import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timedelta

from zeam.api.main import app
from zeam.redis.client import get_redis_client

client = TestClient(app)

# Mock Redis client
mock_redis = AsyncMock()

async def get_mock_redis():
    return mock_redis

@pytest.fixture(autouse=True)
def override_redis_dependency():
    app.dependency_overrides[get_redis_client] = get_mock_redis
    yield
    app.dependency_overrides = {}

def test_curated_recommendation_explicit_dates():
    """Test curated recommendation with explicit dates provided."""
    mock_data = [
        {
            "id": "1",
            "title": "Curated Show",
            "type": "show"
        }
    ]
    
    async def mock_get(key):
        # Redis Key: popularity:curated:{start_date}:{end_date}:{dma_id_or_global}
        expected_key = "zeam-recommender:popularity:curated:2025-01-01:2025-01-07:123"
        if key == expected_key:
            import json
            return json.dumps(mock_data)
        return None
        
    mock_redis.get.side_effect = mock_get
    
    payload = {
        "start_date": "2025-01-01 00:00:00",
        "end_date": "2025-01-07 23:59:59",
        "dma_id": 123,
        "items": 5
    }
    
    response = client.post("/api/v1/recommend/curated", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["title"] == "Curated Show"

def test_curated_recommendation_defaults():
    """Test defaults for dates and global fallback."""
    mock_redis.get.side_effect = None
    mock_redis.get.return_value = None # Reset
    
    request_start = None
    request_end = None
    
    # We can't easily assert the exact key here because datetime.now() changes.
    # But we can capture the key called.
    
    payload = {
        # Empty payload to trigger defaults
    }
    
    response = client.post("/api/v1/recommend/curated", json=payload)
    assert response.status_code == 200
    
    calls = mock_redis.get.call_args_list
    assert len(calls) > 0
    
    # Check key format
    last_call_key = calls[-1][0][0]
    parts = last_call_key.split(":")
    
    assert parts[0] == "zeam-recommender"
    assert parts[1] == "popularity"
    assert parts[2] == "curated"

    assert last_call_key.startswith("zeam-recommender:popularity:curated:")
    assert last_call_key.endswith(":global")

def test_invalid_content_type():
    response = client.post("/api/v1/recommend/invalid_type", json={})
    assert response.status_code == 400
