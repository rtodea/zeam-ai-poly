import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock

from zeam.api.main import app
from zeam.redis_client.client import get_redis_client

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

def test_health_check():
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_connections():
    with patch("zeam.api.api.health.execute_query") as mock_query:
        mock_query.return_value = []
        response = client.get("/api/health/connections")
        assert response.status_code == 200
        # Redis is mocked to ok, Redshift is mocked to ok
        assert response.json() == {"redis": "ok", "redshift": "ok"}

def test_recommendation_global_fallback():
    # Setup mock
    # When checking specific DMA key, return None
    # When checking Global key, return data
    
    mock_data = [
        {
            "id": "123",
            "title": "Test Channel",
            "type": "channel"
        }
    ]
    
    async def mock_get(key):
        if key == "zeam-recommender:popularity:global":
            import json
            return json.dumps(mock_data)
        return None
        
    mock_redis.get.side_effect = mock_get
    
    # Ensure correct event loop handling if needed (FastAPI TestClient handles it)

    payload = {
        "deviceidentifier": "test_device",
        "islocalized": True,
        "dmaid": 4001
    }
    
    response = client.post("/api/v1/recommend", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert len(data["channels"]) == 1
    assert data["channels"][0]["id"] == "123"

def test_recommendation_empty():
    # Reset mock to return None for everything
    mock_redis.get.side_effect = None
    mock_redis.get.return_value = None
    
    payload = {
        "deviceidentifier": "test_device",
        "islocalized": True
    }
    
    response = client.post("/api/v1/recommend", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert len(data["channels"]) == 0

