from unittest.mock import patch, AsyncMock, MagicMock
from fastapi.testclient import TestClient
import pytest
from zeam.api.main import app

client = TestClient(app)

@pytest.fixture
def mock_redis():
    with patch("zeam.api.api.redis.async_client_context") as mock_context:
        mock_redis_client = AsyncMock()
        mock_context.return_value.__aenter__.return_value = mock_redis_client
        yield mock_redis_client

def test_redis_stats(mock_redis):
    mock_redis.dbsize.return_value = 10
    mock_redis.info.return_value = {"used_memory": 1024 * 1024 * 2.5}
    
    response = client.get("/api/redis/stats")
    
    assert response.status_code == 200
    assert response.json() == {"keys": 10, "sizeInMiB": 2.5}

def test_redis_stats_error(mock_redis):
    mock_redis.dbsize.side_effect = Exception("Redis down")
    mock_redis.info.side_effect = Exception("Redis down")
    
    response = client.get("/api/redis/stats")
    
    assert response.status_code == 200
    assert response.json() == {"keys": 0, "sizeInMiB": 0.0}

def test_list_redis_keys(mock_redis):
    keys = ["user:1", "user:2", "other:key"]
    
    # We use a regular MagicMock for scan_iter because it's NOT a coroutine,
    # it's a normal method that returns an async iterator.
    mock_redis.scan_iter = MagicMock()

    async def mock_scan_iter(match=None):
        for k in keys:
            if match is None or match.replace("*", "") in k:
                yield k
    
    mock_redis.scan_iter.side_effect = mock_scan_iter
    
    response = client.get("/api/redis/keys?pattern=user:*")
    
    assert response.status_code == 200
    assert response.json() == ["user:1", "user:2"]

def test_list_redis_keys_limit(mock_redis):
    keys = [f"key:{i}" for i in range(10)]
    
    mock_redis.scan_iter = MagicMock()

    async def mock_scan_iter(match=None):
        for k in keys:
            yield k

    mock_redis.scan_iter.side_effect = mock_scan_iter
    
    response = client.get("/api/redis/keys?limit=5")
    
    assert response.status_code == 200
    assert len(response.json()) == 5

def test_list_redis_keys_error(mock_redis):
    mock_redis.scan_iter.side_effect = Exception("Redis down")
    
    response = client.get("/api/redis/keys")
    
    assert response.status_code == 503
    assert response.json()["detail"] == "Redis unavailable"

@pytest.mark.parametrize("key_type, redis_value, expected_response", [
    ("string", "val", "val"),
    ("hash", {"field": "value"}, {"field": "value"}),
    ("list", ["item1", "item2"], ["item1", "item2"]),
    ("set", {"member1", "member2"}, ["member1", "member2"]),
    ("zset", [("m1", 1.0), ("m2", 2.0)], [["m1", 1.0], ["m2", 2.0]]),
    ("unknown", None, {"type": "unknown"}),
])
def test_get_redis_key_types(mock_redis, key_type, redis_value, expected_response):
    mock_redis.type.return_value = key_type
    
    if key_type == "string":
        mock_redis.get.return_value = redis_value
    elif key_type == "hash":
        mock_redis.hgetall.return_value = redis_value
    elif key_type == "list":
        mock_redis.lrange.return_value = redis_value
    elif key_type == "set":
        mock_redis.smembers.return_value = redis_value
    elif key_type == "zset":
        mock_redis.zrange.return_value = redis_value

    response = client.get("/api/redis/mykey")
    
    assert response.status_code == 200
    assert response.json() == expected_response

def test_get_redis_key_not_found(mock_redis):
    mock_redis.type.return_value = "none"
    
    response = client.get("/api/redis/nonexistent")
    
    assert response.status_code == 404
    assert response.json()["detail"] == "Key not found"

def test_get_redis_key_error(mock_redis):
    mock_redis.type.side_effect = Exception("Redis down")
    
    response = client.get("/api/redis/anykey")
    
    assert response.status_code == 503
    assert response.json()["detail"] == "Redis unavailable"

def test_delete_redis_key_success(mock_redis):
    mock_redis.delete.return_value = 1
    
    response = client.delete("/api/redis/mykey")
    
    assert response.status_code == 204

def test_delete_redis_key_not_found(mock_redis):
    mock_redis.delete.return_value = 0
    
    response = client.delete("/api/redis/nonexistent")
    
    assert response.status_code == 404
    assert response.json()["detail"] == "Key not found"

def test_delete_redis_key_error(mock_redis):
    mock_redis.delete.side_effect = Exception("Redis down")
    
    response = client.delete("/api/redis/anykey")
    
    assert response.status_code == 503
    assert response.json()["detail"] == "Redis unavailable"
