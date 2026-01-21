from zeam.redis_client import client


def test_client_module_exists():
    assert client is not None
