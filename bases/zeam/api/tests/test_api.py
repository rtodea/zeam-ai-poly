import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient

from zeam.api.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_connections():
    with patch("zeam.api.api.health.execute_query") as mock_query, \
         patch("zeam.api.api.health.ping") as mock_ping:
        mock_query.return_value = []
        mock_ping.return_value = True
        
        response = client.get("/api/health/connections")
        assert response.status_code == 200
        # Redis is mocked to ok, Redshift is mocked to ok
        assert response.json() == {"redis": "ok", "redshift": "ok"}

@patch("zeam.api.api.v1.recommend.get_json")
def test_recommendation_global_fallback(mock_get_json):
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
            return mock_data
        return None
        
    mock_get_json.side_effect = mock_get
    
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

@patch("zeam.api.api.v1.recommend.get_json")
def test_recommendation_empty(mock_get_json):
    # Reset mock to return None for everything
    mock_get_json.return_value = None
    
    payload = {
        "deviceidentifier": "test_device",
        "islocalized": True
    }
    
    response = client.post("/api/v1/recommend", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert len(data["channels"]) == 0

