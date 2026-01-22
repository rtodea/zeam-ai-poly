import asyncio
import logging

from fastapi import APIRouter, Depends
from zeam.redshift import health_check as redshift_health_check
from zeam.redis_client import ping

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/")
async def health_check():
    return {"status": "ok"}


@router.get("/connections")
async def health_connections():
    redis_status = "error"
    redshift_status = "error"

    # Redis: ping with a short timeout
    try:
        await asyncio.wait_for(ping(), timeout=1.0)
        redis_status = "ok"
    except Exception as e:
        logger.warning("Redis health check failed: %s", e)

    # Redshift/Postgres: connect + SELECT 1
    try:
        redshift_health_check()
        redshift_status = "ok"
    except Exception as e:
        logger.warning("Database health check failed: %s", e)

    return {"redis": redis_status, "redshift": redshift_status}
