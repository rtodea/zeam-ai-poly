import asyncio
import logging

from fastapi import APIRouter, Depends
from redis.asyncio import Redis

from zeam.popularity.core.database import RedshiftConnection
from zeam.popularity.core.redis import get_redis_client

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/")
async def health_check():
    return {"status": "ok"}


@router.get("/connections")
async def health_connections(redis: Redis = Depends(get_redis_client)):
    redis_status = "error"
    redshift_status = "error"

    # Redis: ping with a short timeout
    try:
        await asyncio.wait_for(redis.ping(), timeout=1.0)
        redis_status = "ok"
    except Exception as e:
        logger.warning("Redis health check failed: %s", e)

    # Redshift/Postgres: connect + SELECT 1
    try:
        with RedshiftConnection() as conn:
            conn.execute_query("SELECT 1")
        redshift_status = "ok"
    except Exception as e:
        logger.warning("Database health check failed: %s", e)

    return {"redis": redis_status, "redshift": redshift_status}
