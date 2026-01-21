import json
import logging
from typing import Any
import redis
import redis.asyncio as aredis
from zeam.redis_client.config import settings

logger = logging.getLogger(__name__)

async def get_redis_client() -> aredis.Redis:
    return aredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD,
        decode_responses=True
    )

def get_sync_redis_client() -> redis.Redis:
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD,
        decode_responses=True
    )

def store_json_data(key: str, data: Any) -> None:
    if not data:
        logger.info("No data provided, skipping Redis write.")
        return

    client = get_sync_redis_client()
    try:
        client.set(key, json.dumps(data))
        logger.info(f"Stored data in Redis: {key}")
    finally:
        client.close()
