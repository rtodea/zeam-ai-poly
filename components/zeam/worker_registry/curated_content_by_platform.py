import logging
from typing import Dict, Any, Optional
from zeam.analytics.curated_content import get_results_by_platform
from zeam.redis_client import set_json

logger = logging.getLogger(__name__)


def get_curated_content_by_platform_redis_key(start_date: str, end_date: str, platformos_id: Optional[int] = None) -> str:
    """
    Generates the Redis key for curated content popularity by platform.
    """
    # Extract only YYYY-MM-DD
    start_date_key = start_date.split(' ')[0]
    end_date_key = end_date.split(' ')[0]

    platform_suffix = str(platformos_id) if platformos_id else "global"
    return f"zeam-recommender:popularity:curated:platform:{start_date_key}:{end_date_key}:{platform_suffix}"


def run_curated_content_by_platform_task(start_date: str, end_date: str, platformos_id: Optional[int] = None, item_count: int = 10, run_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Executes the curated content popularity by platform logic: queries analytics and saves to Redis.
    """
    logger.info(f"Running curated content by platform task for period {start_date} to {end_date}, Platform: {platformos_id}, Limit: {item_count}. Run ID: {run_id}")

    # Execute query
    rows = get_results_by_platform(start_date, end_date, platformos_id, item_count)
    logger.info(f"Query returned {len(rows)} rows")

    # Save to Redis
    redis_key = get_curated_content_by_platform_redis_key(start_date, end_date, platformos_id)
    set_json(redis_key, rows)

    return {
        "status": "success",
        "args": {
            "start_date": start_date,
            "end_date": end_date,
            "platformos_id": platformos_id,
            "item_count": item_count
        },
        "run_id": run_id,
        "rows_count": len(rows),
        "redis_key": redis_key,
    }
