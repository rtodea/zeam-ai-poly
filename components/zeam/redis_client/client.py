import json
import logging
from typing import Any
import redis
import redis.asyncio as aredis
from zeam.redis_client.config import settings

logger = logging.getLogger(__name__)

from typing import Any, Optional
from contextlib import asynccontextmanager

async def _get_redis_client() -> aredis.Redis:
    return aredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD,
        decode_responses=True
    )

@asynccontextmanager
async def async_client_context():
    client = await _get_redis_client()
    try:
        yield client
    finally:
        await client.aclose()

def _get_sync_redis_client() -> redis.Redis:
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD,
        decode_responses=True
    )

async def get_value(key: str) -> Optional[str]:
    client = await _get_redis_client()
    try:
        return await client.get(key)
    finally:
        await client.aclose()

async def get_json(key: str) -> Any:
    val = await get_value(key)
    if val:
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from key: {key}")
            return None
    return None

async def ping() -> bool:
    client = await _get_redis_client()
    try:
        return await client.ping()
    finally:
        await client.aclose()

def set_json(key: str, data: Any) -> None:
    if not data:
        logger.info("No data provided, skipping Redis write.")
        return

    client = _get_sync_redis_client()
    try:
        client.set(key, json.dumps(data))
        logger.info(f"Stored data in Redis: {key}")
    finally:
        client.close()

# Alias for backward compatibility if needed, or just remove if I update consumers
store_json_data = set_json
