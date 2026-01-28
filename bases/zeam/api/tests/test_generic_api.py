from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient

from zeam.api.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@patch("zeam.redshift.health_check")
@patch("zeam.api.api.health.ping", new_callable=AsyncMock)
def test_health_connections(mock_ping, mock_health_check):
    mock_ping.return_value = True
    mock_health_check.return_value = True

    response = client.get("/api/health/connections")
    assert response.status_code == 200
    # Redis is mocked to ok, Redshift is mocked to ok
    assert response.json() == {"redis": "ok", "redshift": "ok"}


def test_recommendation_global_fallback():
    payload = {
        "deviceidentifier": "test_device",
        "islocalized": True,
        "dmaid": 4001
    }
    
    response = client.post("/api/v1/recommend", json=payload)
    
    assert response.status_code == 400
    data = response.json()
    assert data["detail"] == "Please specify a content type. Use routes like: /recommend/curated"
